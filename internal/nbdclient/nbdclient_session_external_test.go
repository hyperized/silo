package nbdclient_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/nbdclient"
)

func TestAttachSurfacesKernelConnectFailure(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()

	kernel := &fakeKernel{index: 1, connectErr: errors.New("netlink connect refused")}
	cfg := testConfig(t, kernel, memBackend{size: 1 << 20})
	if _, err := nbdclient.Attach(context.Background(), cfg); err == nil {
		t.Fatal("Attach should surface a kernel Connect failure")
	}
}

func TestAttachSurfacesNegotiationFailure(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()

	kernel := &fakeKernel{index: 1}
	// The server refuses every export, so the handshake fails before the fd is
	// ever handed to the kernel.
	cfg := testConfig(t, kernel, memBackend{unknown: true})
	if _, err := nbdclient.Attach(context.Background(), cfg); err == nil {
		t.Fatal("Attach should fail when negotiation is refused")
	}
}

func TestAttachSurfacesSocketFDFailure(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) {
		return -1, errors.New("no descriptor")
	})
	defer restoreFD()

	kernel := &fakeKernel{index: 1}
	cfg := testConfig(t, kernel, memBackend{size: 1 << 20})
	if _, err := nbdclient.Attach(context.Background(), cfg); err == nil {
		t.Fatal("Attach should fail when the socket fd cannot be extracted")
	}
}

func TestAttachSurvivesWatchSocketFailure(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()

	kernel := &fakeKernel{index: 2}
	cfg := testConfig(t, kernel, memBackend{size: 1 << 20})
	cfg.WatchSocket = func(int, func()) (func(), error) {
		return nil, errors.New("no fd to watch")
	}
	s, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("a watch failure must not fail the attach: %v", err)
	}
	defer func() { _ = s.Detach(context.Background()) }()
	if s.State() != nbdclient.StateHealthy {
		t.Fatalf("state = %s, want healthy despite the watch failure", s.State())
	}
}

func TestAttachAppliesDefaultsAndSurfacesDialFailure(t *testing.T) {
	// A minimal config: only the kernel, address and export are set, so
	// applyDefaults must fill the dialer, logger, watcher and reconnect window.
	// Port 1 refuses immediately, so the default dialer fails before the default
	// watcher runs — safe even on linux where the real watcher dups a live fd.
	kernel := &fakeKernel{}
	cfg := nbdclient.Config{Addr: "127.0.0.1:1", Export: "/vol/db", Kernel: kernel}
	_, err := nbdclient.Attach(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "SILO_NBD_ADDR") {
		t.Fatalf("a dial failure with defaults should mention SILO_NBD_ADDR, got %v", err)
	}
}

func TestAdoptValidatesConfig(t *testing.T) {
	if _, err := nbdclient.Adopt(context.Background(), nbdclient.Config{}, 0, 0); err == nil {
		t.Fatal("Adopt should reject a config without a kernel")
	}
}

func TestReconnectRetriesUntilReconfigureSucceeds(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()
	restoreBackoff := nbdclient.SetBackoffForTest(time.Millisecond, 5*time.Millisecond)
	defer restoreBackoff()

	kernel := &fakeKernel{index: 4}
	kernel.setReconfigErr(errors.New("reconfigure rejected"))
	cfg := testConfig(t, kernel, memBackend{size: 1 << 20})
	s, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer func() { _ = s.Detach(context.Background()) }()

	kernel.setConnected(false)
	s.Kick()
	// While Reconfigure keeps failing the supervisor stays reconnecting and
	// never counts a reconfigure — reconfigErr short-circuits before the count.
	waitFor(t, "reconnecting", func() bool { return s.State() == nbdclient.StateReconnecting })
	time.Sleep(30 * time.Millisecond)
	if got := kernel.snapshot().reconfigures; got != 0 {
		t.Fatalf("reconfigures = %d while erroring, want 0", got)
	}
	if s.Reconnects() != 0 {
		t.Fatalf("reconnects = %d while erroring, want 0", s.Reconnects())
	}

	// Once the kernel accepts reconfigures again, the retry loop's next attempt wins.
	kernel.setReconfigErr(nil)
	waitFor(t, "reconnect success", func() bool { return s.Reconnects() == 1 })
	if s.State() != nbdclient.StateHealthy {
		t.Fatalf("state after recovery = %s, want healthy", s.State())
	}
	if got := kernel.snapshot().reconfigures; got != 1 {
		t.Fatalf("reconfigures = %d, want 1", got)
	}
}

func TestReconnectRetriesAfterDialFailure(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()
	restoreBackoff := nbdclient.SetBackoffForTest(time.Millisecond, 5*time.Millisecond)
	defer restoreBackoff()

	kernel := &fakeKernel{index: 9}
	var mu sync.Mutex
	failNext := false
	dial := func(_ context.Context, _ string) (net.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		if failNext {
			failNext = false
			return nil, errors.New("connection refused")
		}
		client, server := net.Pipe()
		serveOnce(t, memBackend{size: 1 << 20}, server)
		return client, nil
	}
	cfg := nbdclient.Config{
		Addr: "127.0.0.1:10809", Export: "/vol/db",
		Kernel: kernel, Dial: dial, Logger: discardLogger(),
		WatchSocket: (&watchRecorder{}).watch,
	}
	s, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer func() { _ = s.Detach(context.Background()) }()

	mu.Lock()
	failNext = true
	mu.Unlock()
	kernel.setConnected(false)
	s.Kick()
	// The first reconnect dial fails; the backoff loop must recover on the next.
	waitFor(t, "reconnect after a dial failure", func() bool { return s.Reconnects() == 1 })
}

func TestReconnectLearnsSizeWhenUnknown(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()
	restoreBackoff := nbdclient.SetBackoffForTest(time.Millisecond, 5*time.Millisecond)
	defer restoreBackoff()

	kernel := &fakeKernel{index: 8}
	cfg := testConfig(t, kernel, memBackend{size: 1 << 20})
	// Adopt with an unknown size (0): the first reconnect must learn it from the
	// handshake rather than reject the export as changed.
	s, err := nbdclient.Adopt(context.Background(), cfg, 8, 0)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	defer func() { _ = s.Detach(context.Background()) }()

	s.Kick()
	waitFor(t, "size learned", func() bool { return s.Size() == 1<<20 })
	if s.Reconnects() != 1 {
		t.Fatalf("reconnects = %d, want 1", s.Reconnects())
	}
}

func TestDetachSurfacesDisconnectFailure(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()

	kernel := &fakeKernel{index: 6}
	cfg := testConfig(t, kernel, memBackend{size: 1 << 20})
	s, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	kernel.setDisconnectErr(errors.New("kernel busy"))
	if err := s.Detach(context.Background()); err == nil {
		t.Fatal("Detach should surface a kernel Disconnect failure")
	}

	// Detach marks the session detached before it calls Disconnect, so a second
	// Detach after the kernel recovers is a no-op — it never retries the
	// disconnect. The device is left configured; a caller wanting a guaranteed
	// teardown disconnects by index (the CSI attacher does exactly this).
	kernel.setDisconnectErr(nil)
	if err := s.Detach(context.Background()); err != nil {
		t.Fatalf("second Detach = %v, want nil (already marked detached)", err)
	}
	if got := kernel.snapshot().disconnectCalls; got != 1 {
		t.Fatalf("Disconnect calls = %d, want 1 (the second Detach short-circuits)", got)
	}
	if got := kernel.snapshot().disconnects; got != 0 {
		t.Fatalf("successful disconnects = %d, want 0 (the only call errored)", got)
	}
}

// closeTrackingDialer wraps pipe connections so a test can observe which of
// them were closed — the lease-guard behaviour hinges on exactly that.
type closeTrackingDialer struct {
	mu    sync.Mutex
	conns []*trackedConn
	base  func(context.Context, string) (net.Conn, error)
}

type trackedConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *trackedConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func (d *closeTrackingDialer) dial(ctx context.Context, addr string) (net.Conn, error) {
	conn, err := d.base(ctx, addr)
	if err != nil {
		return nil, err
	}
	tc := &trackedConn{Conn: conn}
	d.mu.Lock()
	d.conns = append(d.conns, tc)
	d.mu.Unlock()
	return tc, nil
}

func (d *closeTrackingDialer) unclosed() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, c := range d.conns {
		if !c.closed.Load() {
			n++
		}
	}
	return n
}

// TestReconfigureFailureKeepsLeaseGuardConnection: a connection whose kernel
// hand-off failed holds the volume's newest lease acquisition, so it must
// stay open (its close would vacate the live lease) until a later attempt
// re-acquires — at which point everything closes.
func TestReconfigureFailureKeepsLeaseGuardConnection(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()
	restoreBackoff := nbdclient.SetBackoffForTest(time.Millisecond, 5*time.Millisecond)
	defer restoreBackoff()

	kernel := &fakeKernel{index: 2}
	dialer := &closeTrackingDialer{base: pipeDialer(t, memBackend{size: 1 << 20})}
	cfg := testConfig(t, kernel, memBackend{size: 1 << 20})
	cfg.Dial = dialer.dial

	s, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer func() { _ = s.Detach(context.Background()) }()

	kernel.setReconfigErr(errors.New("no dead slot"))
	s.Kick()
	// While reconfigures keep failing, exactly one negotiated connection must
	// stay open as the lease guard (each new attempt supersedes the previous).
	waitFor(t, "a lease-guard connection", func() bool { return dialer.unclosed() == 1 })
	time.Sleep(20 * time.Millisecond)
	if got := dialer.unclosed(); got != 1 {
		t.Fatalf("unclosed connections during failed reconfigures = %d, want exactly 1", got)
	}

	kernel.setReconfigErr(nil)
	waitFor(t, "reconnect success", func() bool { return s.Reconnects() >= 1 })
	// The successful attempt re-acquired the lease and the kernel holds its
	// own socket reference: every user-space connection can close.
	waitFor(t, "all connections closed", func() bool { return dialer.unclosed() == 0 })
}
