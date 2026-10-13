//go:build integration && linux

package nbdclient_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/nbd"
	"github.com/hyperized/silo/internal/nbdclient"
	"github.com/hyperized/silo/internal/nbdnl"
)

// TestKernelAttachReconnectDetach exercises the whole restart-resilience
// stack against the real kernel: internal/nbd serves a volume from memory,
// nbdclient attaches it as /dev/nbdX over netlink, raw I/O flows through the
// kernel, the server dies and comes back (a silod restart), the dead-link
// watcher fires, the session reconnects, and I/O resumes without an error.
//
// Needs root, the nbd kernel module, and /dev/nbd*; run it in a privileged
// container:
//
//	docker run --rm --privileged -v "$PWD":/src -w /src golang:1.25-alpine \
//	  go test -tags integration -run TestKernel -v ./internal/nbdclient/
func TestKernelAttachReconnectDetach(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("needs root to configure NBD devices")
	}
	kernel, err := nbdnl.Dial()
	if err != nil {
		t.Skipf("kernel NBD netlink unavailable (%v); load the nbd module first", err)
	}
	defer func() { _ = kernel.Close() }()

	srv := nbd.NewServer(memBackend{size: 64 << 20}, discardLogger())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ctx1, cancelServe1 := context.WithCancel(context.Background())
	go func() { _ = srv.Serve(ctx1, ln) }()

	cfg := nbdclient.Config{
		Addr:            addr,
		Export:          "/integration/vol",
		Kernel:          kernel,
		ReconnectWindow: 2 * time.Minute,
		// The default socket watch runs: this is the real-fd environment.
	}
	session, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	t.Logf("attached %s", session.Device())
	defer func() { _ = session.Detach(context.Background()) }()

	// The dead-link watcher, wired the way the CSI attacher wires it.
	watcher, err := kernel.Watch()
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer func() { _ = watcher.Close() }()
	watcherFired := make(chan uint32, 8)
	go func() {
		for {
			idx, err := watcher.Next()
			if err != nil {
				return
			}
			watcherFired <- idx
			if idx == session.Index() {
				session.Kick()
			}
		}
	}()

	// Raw I/O through the kernel device; fsync forces it through to the server.
	dev, err := os.OpenFile(session.Device(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s: %v", session.Device(), err)
	}
	defer func() { _ = dev.Close() }()
	pattern := make([]byte, 1<<20)
	for i := range pattern {
		pattern[i] = byte(i)
	}
	if _, err := dev.WriteAt(pattern, 0); err != nil {
		t.Fatalf("write before restart: %v", err)
	}
	if err := dev.Sync(); err != nil {
		t.Fatalf("sync before restart: %v", err)
	}

	// The "silod restart": kill the server, let the link die, bring the
	// server back on the same address.
	cancelServe1()
	_ = ln.Close()

	select {
	case idx := <-watcherFired:
		if idx != session.Index() {
			t.Logf("watcher reported another device first (%d); still waiting", idx)
		}
		t.Logf("kernel reported the dead link for device %d", idx)
	case <-time.After(15 * time.Second):
		t.Fatal("the kernel never multicast a dead-link notification")
	}

	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("re-listen on %s: %v", addr, err)
	}
	ctx2, cancelServe2 := context.WithCancel(context.Background())
	defer cancelServe2()
	go func() { _ = srv.Serve(ctx2, ln2) }()

	waitForCondition(t, "reconnect", 30*time.Second, func() bool {
		return session.Reconnects() >= 1 && session.State() == nbdclient.StateHealthy
	})

	// I/O must flow again on the same device node, no remount, no error.
	if _, err := dev.WriteAt(pattern, 1<<20); err != nil {
		t.Fatalf("write after reconnect: %v", err)
	}
	if err := dev.Sync(); err != nil {
		t.Fatalf("sync after reconnect: %v", err)
	}
	got := make([]byte, 1<<20)
	if _, err := dev.ReadAt(got, 1<<20); err != nil {
		t.Fatalf("read after reconnect: %v", err)
	}

	// An open device pins the kernel configuration, so close before
	// detaching — the same order the kubelet guarantees (unmount, then
	// unpublish). The kernel then tears the configuration down
	// asynchronously after acknowledging the disconnect.
	if err := dev.Close(); err != nil {
		t.Fatalf("close device: %v", err)
	}
	if err := session.Detach(context.Background()); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	waitForCondition(t, "device deconfiguration", 10*time.Second, func() bool {
		_, err := os.Stat(fmt.Sprintf("/sys/block/nbd%d/pid", session.Index()))
		return errors.Is(err, os.ErrNotExist)
	})
}

func waitForCondition(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestKernelPluginExitThenWindowExpiry replays issue #26: the node plugin
// exits while its device is healthy, silod then dies, and nobody reconnects
// before the kernel's dead-connection window runs out. The kernel fails the
// queued I/O with EIO and keeps failing new I/O until a reconfigure; a
// restarted plugin adopting the device must be able to bring it back.
func TestKernelPluginExitThenWindowExpiry(t *testing.T) {
	kernel := dialKernel(t)
	store := newStoreBackend(64 << 20)
	srv := startServer(t, store, "127.0.0.1:0")

	cfg := nbdclient.Config{
		Addr:            srv.addr,
		Export:          "/integration/window-expiry",
		Kernel:          kernel,
		ReconnectWindow: 2 * time.Second,
	}
	session, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	current := session
	t.Cleanup(func() { _ = current.Detach(context.Background()) })
	deadLinks := watchDeadLinks(t, kernel)

	dev := openDevice(t, session.Device())
	first := filledPattern(1<<20, 1)
	writeAndSync(t, dev, first, 0, "before plugin exit")

	session.Stop()
	// The kernel owns its socket reference, so the plugin going away must not
	// interrupt I/O on a device something still has open.
	writeAndSync(t, dev, first, 0, "after plugin exit")

	srv.kill()
	waitForDeadLink(t, deadLinks, session.Index())

	type result struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		_, err := dev.WriteAt(filledPattern(1<<20, 2), 1<<20)
		if err == nil {
			err = dev.Sync()
		}
		done <- result{err: err, elapsed: time.Since(start)}
	}()
	var res result
	select {
	case res = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("I/O on a dead link was still blocked 30s after a 2s reconnect window")
	}
	t.Logf("I/O on the dead link failed after %s: %v", res.elapsed, res.err)
	if !isEIO(res.err) {
		t.Fatalf("I/O after the window expired returned %v, want EIO", res.err)
	}
	// The kernel rounds the window to whole seconds; anything faster than a
	// second means the request never waited for a reconnect at all.
	if res.elapsed < time.Second {
		t.Fatalf("I/O failed after %s, before the reconnect window could have expired", res.elapsed)
	}

	// The state a restarted plugin finds: the kernel has given up, so every
	// new request fails without waiting.
	start := time.Now()
	_, err = directRead(session.Device(), 0, 4096)
	elapsed := time.Since(start)
	t.Logf("direct read after the kernel gave up returned after %s: %v", elapsed, err)
	if err == nil {
		t.Fatal("direct read succeeded on a device whose reconnect window expired")
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("direct read took %s to fail; the kernel should fail it immediately", elapsed)
	}

	adopted, err := nbdclient.Adopt(context.Background(), cfg, session.Index(), uint64(store.Size()))
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	current = adopted
	adopted.Kick()
	reconnectStart := time.Now()
	srv.restart(t)
	waitForCondition(t, "reconnect after window expiry", 30*time.Second, func() bool {
		return adopted.Reconnects() >= 1 && adopted.State() == nbdclient.StateHealthy
	})
	t.Logf("adopted session reconnected %s after the server came back", time.Since(reconnectStart))

	second := filledPattern(1<<20, 3)
	writeAndSync(t, dev, second, 2<<20, "after adopt and reconnect")
	assertDeviceHolds(t, adopted.Device(), 2<<20, second)
	assertDeviceHolds(t, adopted.Device(), 0, first)

	detachAndWait(t, dev, adopted)
}

// TestKernelAdoptInsideWindowLosesNoIO is the outcome issue #26 needs: a
// plugin that adopts the device while the kernel is still holding I/O for a
// dead link reconnects in time, and the I/O that was queued meanwhile
// completes without an error and lands on the server.
func TestKernelAdoptInsideWindowLosesNoIO(t *testing.T) {
	kernel := dialKernel(t)
	store := newStoreBackend(64 << 20)
	srv := startServer(t, store, "127.0.0.1:0")

	cfg := nbdclient.Config{
		Addr:            srv.addr,
		Export:          "/integration/adopt-inside-window",
		Kernel:          kernel,
		ReconnectWindow: 30 * time.Second,
	}
	session, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	current := session
	t.Cleanup(func() { _ = current.Detach(context.Background()) })
	deadLinks := watchDeadLinks(t, kernel)

	dev := openDevice(t, session.Device())
	first := filledPattern(1<<20, 4)
	writeAndSync(t, dev, first, 0, "before plugin exit")

	session.Stop()
	srv.kill()
	waitForDeadLink(t, deadLinks, session.Index())

	queued := filledPattern(1<<20, 5)
	done := make(chan error, 1)
	queuedAt := time.Now()
	go func() {
		_, err := dev.WriteAt(queued, 1<<20)
		if err == nil {
			err = dev.Sync()
		}
		done <- err
	}()
	// Long enough for the writeback to reach the kernel's reconnect wait, so
	// the reconnect below has to rescue a request that is already queued.
	select {
	case err := <-done:
		t.Fatalf("I/O on a dead link returned before any reconnect: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	adopted, err := nbdclient.Adopt(context.Background(), cfg, session.Index(), uint64(store.Size()))
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	current = adopted
	adopted.Kick()
	reconnectStart := time.Now()
	srv.restart(t)
	waitForCondition(t, "reconnect inside the window", 20*time.Second, func() bool {
		return adopted.Reconnects() >= 1 && adopted.State() == nbdclient.StateHealthy
	})
	t.Logf("adopted session reconnected %s after the server came back", time.Since(reconnectStart))

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("I/O queued during the outage failed: %v", err)
		}
		t.Logf("I/O queued during the outage completed after %s", time.Since(queuedAt))
	case <-time.After(20 * time.Second):
		t.Fatal("I/O queued during the outage never completed after the reconnect")
	}
	assertDeviceHolds(t, adopted.Device(), 1<<20, queued)
	assertDeviceHolds(t, adopted.Device(), 0, first)

	detachAndWait(t, dev, adopted)
}

func dialKernel(t *testing.T) *nbdnl.Conn {
	t.Helper()
	if os.Getuid() != 0 {
		t.Skip("needs root to configure NBD devices")
	}
	kernel, err := nbdnl.Dial()
	if err != nil {
		t.Skipf("kernel NBD netlink unavailable (%v); load the nbd module first", err)
	}
	t.Cleanup(func() { _ = kernel.Close() })
	return kernel
}

// storeBackend keeps what the kernel writes, unlike memBackend, so a test can
// prove I/O queued across an outage reached the server.
type storeBackend struct {
	mu   sync.Mutex
	data []byte
}

func newStoreBackend(size int) *storeBackend { return &storeBackend{data: make([]byte, size)} }

func (b *storeBackend) Open(context.Context, string) (nbd.Device, func(), error) {
	return b, func() {}, nil
}

func (b *storeBackend) Size() int64 { return int64(len(b.data)) }

func (b *storeBackend) ReadAt(p []byte, off int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if off < 0 || off+int64(len(p)) > int64(len(b.data)) {
		return 0, io.ErrUnexpectedEOF
	}
	return copy(p, b.data[off:]), nil
}

func (b *storeBackend) WriteAt(p []byte, off int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if off < 0 || off+int64(len(p)) > int64(len(b.data)) {
		return 0, io.ErrShortWrite
	}
	return copy(b.data[off:], p), nil
}

// restartableServer is silod's NBD listener, killable and restartable on the
// same address the way a silod restart behaves.
type restartableServer struct {
	srv    *nbd.Server
	addr   string
	ln     net.Listener
	cancel context.CancelFunc
}

func startServer(t *testing.T, backend nbd.Backend, addr string) *restartableServer {
	t.Helper()
	s := &restartableServer{srv: nbd.NewServer(backend, discardLogger()), addr: addr}
	s.restart(t)
	t.Cleanup(s.kill)
	return s
}

func (s *restartableServer) restart(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		t.Fatalf("listen on %s: %v", s.addr, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ln, s.addr, s.cancel = ln, ln.Addr().String(), cancel
	go func() { _ = s.srv.Serve(ctx, ln) }()
}

func (s *restartableServer) kill() {
	if s.cancel != nil {
		s.cancel()
		_ = s.ln.Close()
	}
}

// watchDeadLinks streams the kernel's dead-link multicasts. It is opened
// while the link is still healthy so the notification cannot be missed.
func watchDeadLinks(t *testing.T, kernel *nbdnl.Conn) <-chan uint32 {
	t.Helper()
	watcher, err := kernel.Watch()
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	t.Cleanup(func() { _ = watcher.Close() })
	fired := make(chan uint32, 16)
	go func() {
		for {
			idx, err := watcher.Next()
			if err != nil {
				return
			}
			select {
			case fired <- idx:
			default:
			}
		}
	}()
	return fired
}

func waitForDeadLink(t *testing.T, fired <-chan uint32, index uint32) {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case idx := <-fired:
			if idx == index {
				return
			}
			t.Logf("dead link reported for another device (%d); still waiting for %d", idx, index)
		case <-deadline:
			t.Fatalf("the kernel never multicast a dead-link notification for device %d", index)
		}
	}
}

func openDevice(t *testing.T, path string) *os.File {
	t.Helper()
	dev, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	return dev
}

func filledPattern(n int, seed byte) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i) ^ seed
	}
	return p
}

func writeAndSync(t *testing.T, dev *os.File, p []byte, off int64, when string) {
	t.Helper()
	if _, err := dev.WriteAt(p, off); err != nil {
		t.Fatalf("write %s: %v", when, err)
	}
	if err := dev.Sync(); err != nil {
		t.Fatalf("sync %s: %v", when, err)
	}
}

// directRead bypasses the page cache, which would otherwise answer from
// memory and hide both a failed device and data that never reached the
// server. O_DIRECT needs a page-aligned buffer; an anonymous mapping is one.
func directRead(path string, off int64, n int) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECT, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	buf, err := syscall.Mmap(-1, 0, n, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	defer func() { _ = syscall.Munmap(buf) }()
	if _, err := f.ReadAt(buf, off); err != nil {
		return nil, err
	}
	return bytes.Clone(buf), nil
}

func assertDeviceHolds(t *testing.T, path string, off int64, want []byte) {
	t.Helper()
	got, err := directRead(path, off, len(want))
	if err != nil {
		t.Fatalf("direct read at %d: %v", off, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("device at offset %d does not hold the pattern written there", off)
	}
}

func isEIO(err error) bool {
	return errors.Is(err, syscall.EIO) || (err != nil && strings.Contains(err.Error(), "input/output error"))
}

// detachAndWait closes the device before detaching because an open device
// pins the kernel configuration, the same order the kubelet guarantees.
func detachAndWait(t *testing.T, dev *os.File, session *nbdclient.Session) {
	t.Helper()
	if err := dev.Close(); err != nil {
		t.Fatalf("close device: %v", err)
	}
	if err := session.Detach(context.Background()); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	waitForCondition(t, "device deconfiguration", 10*time.Second, func() bool {
		_, err := os.Stat(fmt.Sprintf("/sys/block/nbd%d/pid", session.Index()))
		return errors.Is(err, os.ErrNotExist)
	})
}
