package nbdclient_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/nbd"
	"github.com/hyperized/silo/internal/nbdclient"
	"github.com/hyperized/silo/internal/nbdnl"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// memDevice backs the real internal/nbd server the client negotiates with —
// the strongest possible handshake peer is our own server.
type memDevice struct{ size int64 }

func (d memDevice) Size() int64                            { return d.size }
func (d memDevice) ReadAt(p []byte, _ int64) (int, error)  { return len(p), nil }
func (d memDevice) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }

type memBackend struct {
	size    int64
	unknown bool
}

func (b memBackend) Open(_ context.Context, export string) (nbd.Device, func(), error) {
	if b.unknown || export == "/missing" {
		return nil, nil, errors.New("no such volume")
	}
	return memDevice{size: b.size}, func() {}, nil
}

// serveOnce runs the silo NBD server for a single connection.
func serveOnce(t *testing.T, backend nbd.Backend, conn net.Conn) {
	t.Helper()
	srv := nbd.NewServer(backend, discardLogger())
	go func() { _ = srv.ServeConn(context.Background(), conn) }()
}

func TestNegotiateAgainstSiloServer(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	serveOnce(t, memBackend{size: 1 << 20}, server)

	export, err := nbdclient.Negotiate(context.Background(), client, "/vol/db")
	if err != nil {
		t.Fatalf("Negotiate: %v", err)
	}
	if export.Size != 1<<20 {
		t.Fatalf("size = %d, want %d", export.Size, 1<<20)
	}
	if export.TransmissionFlags == 0 {
		t.Fatal("transmission flags should carry the server's feature bits")
	}
}

func TestNegotiateUnknownExport(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	serveOnce(t, memBackend{unknown: true}, server)

	_, err := nbdclient.Negotiate(context.Background(), client, "/missing")
	if err == nil {
		t.Fatal("Negotiate accepted an export the server refused")
	}
	if !strings.Contains(err.Error(), "/missing") {
		t.Fatalf("the error should name the export: %v", err)
	}
}

func TestNegotiateRejectsNonNBDServer(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	go func() {
		_, _ = server.Write(make([]byte, 64)) // zeros: not an NBD greeting
	}()
	_, err := nbdclient.Negotiate(context.Background(), client, "/vol")
	if err == nil {
		t.Fatal("Negotiate accepted a non-NBD greeting")
	}
}

func TestNegotiateHonoursContextDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	// The server never greets; the deadline must unblock the handshake.
	_, err := nbdclient.Negotiate(ctx, client, "/vol")
	if err == nil {
		t.Fatal("Negotiate should time out against a silent server")
	}
}

// fakeKernel records netlink operations and simulates device state.
type fakeKernel struct {
	mu           sync.Mutex
	index        uint32
	connected    bool
	connects     int
	reconfigures int
	disconnects  int
	connectCfg   nbdnl.ConnectConfig
	reconfigErr  error
}

func (k *fakeKernel) Connect(cfg nbdnl.ConnectConfig) (uint32, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.connects++
	k.connectCfg = cfg
	k.connected = true
	return k.index, nil
}

func (k *fakeKernel) Reconfigure(uint32, int, time.Duration, time.Duration) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.reconfigErr != nil {
		return k.reconfigErr
	}
	k.reconfigures++
	k.connected = true
	return nil
}

func (k *fakeKernel) Disconnect(uint32) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.disconnects++
	k.connected = false
	return nil
}

func (k *fakeKernel) Connected(uint32) (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.connected, nil
}

func (k *fakeKernel) snapshot() fakeKernel {
	k.mu.Lock()
	defer k.mu.Unlock()
	return fakeKernel{
		connects:     k.connects,
		reconfigures: k.reconfigures,
		disconnects:  k.disconnects,
		connectCfg:   k.connectCfg,
	}
}

func (k *fakeKernel) setConnected(v bool) {
	k.mu.Lock()
	k.connected = v
	k.mu.Unlock()
}

// pipeDialer hands out client ends of pipes served by a fresh silo NBD server,
// so every dial performs the full real handshake.
func pipeDialer(t *testing.T, backend nbd.Backend) func(context.Context, string) (net.Conn, error) {
	return func(_ context.Context, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		serveOnce(t, backend, server)
		return client, nil
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func testConfig(t *testing.T, kernel *fakeKernel, backend nbd.Backend) nbdclient.Config {
	return nbdclient.Config{
		Addr:           "127.0.0.1:10809",
		Export:         "/vol/db",
		Kernel:         kernel,
		Dial:           pipeDialer(t, backend),
		Logger:         discardLogger(),
		HealthInterval: time.Hour, // out of the way unless a test exercises the poll
	}
}

func TestAttachSuperviseReconnectDetach(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()
	restoreBackoff := nbdclient.SetBackoffForTest(time.Millisecond, 5*time.Millisecond)
	defer restoreBackoff()

	kernel := &fakeKernel{index: 7}
	cfg := testConfig(t, kernel, memBackend{size: 1 << 20})
	cfg.ReconnectWindow = 2 * time.Minute

	s, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if s.Device() != "/dev/nbd7" || s.Index() != 7 {
		t.Fatalf("device = %s index = %d, want /dev/nbd7 index 7", s.Device(), s.Index())
	}
	if s.State() != nbdclient.StateHealthy {
		t.Fatalf("state = %s, want healthy", s.State())
	}
	if s.Size() != 1<<20 {
		t.Fatalf("size = %d, want %d", s.Size(), 1<<20)
	}
	snap := kernel.snapshot()
	if snap.connects != 1 {
		t.Fatalf("kernel connects = %d, want 1", snap.connects)
	}
	if snap.connectCfg.SocketFD != 42 || snap.connectCfg.SizeBytes != 1<<20 || snap.connectCfg.DeadConnTimeout != 2*time.Minute {
		t.Fatalf("connect config mismatch: %+v", snap.connectCfg)
	}

	// The link dies: the watcher kicks the session, which must redo the full
	// handshake and hand the kernel a fresh socket.
	kernel.setConnected(false)
	s.Kick()
	waitFor(t, "reconnect", func() bool { return s.Reconnects() == 1 })
	if s.State() != nbdclient.StateHealthy {
		t.Fatalf("state after reconnect = %s, want healthy", s.State())
	}
	if kernel.snapshot().reconfigures != 1 {
		t.Fatalf("reconfigures = %d, want 1", kernel.snapshot().reconfigures)
	}

	if err := s.Detach(context.Background()); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	if err := s.Detach(context.Background()); err != nil {
		t.Fatalf("second Detach should be a no-op, got %v", err)
	}
	if got := kernel.snapshot().disconnects; got != 1 {
		t.Fatalf("disconnects = %d, want exactly 1", got)
	}
	if s.State() != nbdclient.StateDetached {
		t.Fatalf("state = %s, want detached", s.State())
	}
}

func TestReconnectRefusesChangedVolume(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()
	restoreBackoff := nbdclient.SetBackoffForTest(time.Millisecond, 5*time.Millisecond)
	defer restoreBackoff()

	kernel := &fakeKernel{index: 1}
	// Attach against a 1 MiB volume, then serve a differently-sized export on
	// every subsequent dial — as if the volume was deleted and recreated.
	small := memBackend{size: 1 << 20}
	big := memBackend{size: 2 << 20}
	var mu sync.Mutex
	backend := small
	dial := func(_ context.Context, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		mu.Lock()
		b := backend
		mu.Unlock()
		serveOnce(t, b, server)
		return client, nil
	}

	cfg := nbdclient.Config{
		Addr: "127.0.0.1:10809", Export: "/vol/db",
		Kernel: kernel, Dial: dial, Logger: discardLogger(),
		HealthInterval: time.Hour,
	}
	s, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer func() { _ = s.Detach(context.Background()) }()

	mu.Lock()
	backend = big
	mu.Unlock()
	kernel.setConnected(false)
	s.Kick()

	// The supervisor must keep refusing the mismatched export: no reconfigure,
	// state stays reconnecting.
	time.Sleep(50 * time.Millisecond)
	if got := kernel.snapshot().reconfigures; got != 0 {
		t.Fatalf("reconfigures = %d; a resized export must never be spliced in", got)
	}
	if s.State() != nbdclient.StateReconnecting {
		t.Fatalf("state = %s, want reconnecting", s.State())
	}
	if s.Reconnects() != 0 {
		t.Fatalf("reconnects = %d, want 0", s.Reconnects())
	}
}

func TestPollFallbackDetectsDeadLink(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()
	restoreBackoff := nbdclient.SetBackoffForTest(time.Millisecond, 5*time.Millisecond)
	defer restoreBackoff()

	kernel := &fakeKernel{index: 3}
	cfg := testConfig(t, kernel, memBackend{size: 1 << 20})
	cfg.HealthInterval = 10 * time.Millisecond

	s, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer func() { _ = s.Detach(context.Background()) }()

	// No Kick: only the poll can notice the dead link.
	kernel.setConnected(false)
	waitFor(t, "poll-driven reconnect", func() bool { return s.Reconnects() >= 1 })
}

func TestAdoptHealthyAndDead(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()
	restoreBackoff := nbdclient.SetBackoffForTest(time.Millisecond, 5*time.Millisecond)
	defer restoreBackoff()

	kernel := &fakeKernel{index: 5, connected: true}
	cfg := testConfig(t, kernel, memBackend{size: 1 << 20})

	s, err := nbdclient.Adopt(context.Background(), cfg, 5, 1<<20)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if s.State() != nbdclient.StateHealthy || s.Device() != "/dev/nbd5" {
		t.Fatalf("adopted session: state=%s device=%s", s.State(), s.Device())
	}
	if err := s.Detach(context.Background()); err != nil {
		t.Fatalf("Detach: %v", err)
	}

	// Adopting a device whose link already died must repair it immediately.
	kernel2 := &fakeKernel{index: 6, connected: false}
	cfg2 := testConfig(t, kernel2, memBackend{size: 1 << 20})
	s2, err := nbdclient.Adopt(context.Background(), cfg2, 6, 1<<20)
	if err != nil {
		t.Fatalf("Adopt (dead): %v", err)
	}
	defer func() { _ = s2.Detach(context.Background()) }()
	waitFor(t, "adoption repair", func() bool { return s2.Reconnects() == 1 })
}

func TestStopLeavesDeviceAttached(t *testing.T) {
	restoreFD := nbdclient.SetSocketFDForTest(func(net.Conn) (int, error) { return 42, nil })
	defer restoreFD()

	kernel := &fakeKernel{index: 2}
	cfg := testConfig(t, kernel, memBackend{size: 1 << 20})
	s, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	s.Stop()
	if got := kernel.snapshot().disconnects; got != 0 {
		t.Fatalf("Stop must not disconnect the device; disconnects = %d", got)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []nbdclient.Config{
		{},                                   // nothing set
		{Addr: "x:1", Export: "/v"},          // no kernel
		{Kernel: &fakeKernel{}},              // no addr/export
		{Kernel: &fakeKernel{}, Addr: "x:1"}, // no export
	}
	for i, cfg := range cases {
		if _, err := nbdclient.Attach(context.Background(), cfg); err == nil {
			t.Fatalf("case %d: Attach accepted an invalid config", i)
		}
	}
}

func TestAttachSurfacesDialFailure(t *testing.T) {
	kernel := &fakeKernel{}
	cfg := nbdclient.Config{
		Addr: "127.0.0.1:1", Export: "/vol",
		Kernel: kernel, Logger: discardLogger(),
		Dial: func(context.Context, string) (net.Conn, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	_, err := nbdclient.Attach(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "SILO_NBD_ADDR") {
		t.Fatalf("a dial failure should tell the operator what to check, got: %v", err)
	}
}
