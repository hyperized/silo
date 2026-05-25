package silod

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/config"
	"github.com/hyperized/silo/internal/crypto"
)

// testConfig returns a Config with the minimal fields Run needs.
func testConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		NodeID:        "test-node",
		GRPCAddr:      "127.0.0.1:0",
		HTTPAddr:      "127.0.0.1:0",
		DataDir:       t.TempDir(),
		ChunkSize:     4 * 1024 * 1024,
		Replication:   1,
		KeySource:     config.KeySourceStatic,
		EncryptionKey: make([]byte, crypto.ClusterKeyBytes),
		LogLevel:      "info",
		LogFormat:     "text",
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeSubsystem doubles a subsystem for lifecycle-edge-case tests. It is
// instrumented with hooks so tests can choose whether Start blocks,
// whether it returns an error, and whether Shutdown fails.
type fakeSubsystem struct {
	name        string
	startErr    error
	shutdownErr error
	blockStart  bool

	mu       sync.Mutex
	startCh  chan struct{}
	shutdown chan struct{}
}

func newFakeSubsystem(name string, startErr, shutdownErr error, blocking bool) *fakeSubsystem {
	return &fakeSubsystem{
		name:        name,
		startErr:    startErr,
		shutdownErr: shutdownErr,
		blockStart:  blocking,
		startCh:     make(chan struct{}),
		shutdown:    make(chan struct{}),
	}
}

func (f *fakeSubsystem) Name() string { return f.name }
func (f *fakeSubsystem) Start() error {
	close(f.startCh)
	if !f.blockStart {
		return f.startErr
	}
	<-f.shutdown
	return f.startErr
}
func (f *fakeSubsystem) Shutdown(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.shutdown:
	default:
		close(f.shutdown)
	}
	return f.shutdownErr
}

// installFakes swaps both subsystem factories for the duration of a
// test, restoring the originals on cleanup.
func installFakes(t *testing.T, httpSub, grpcSub *fakeSubsystem) {
	t.Helper()
	prevHTTP := newHTTPSubsystem
	prevGRPC := newGRPCSubsystem
	t.Cleanup(func() {
		newHTTPSubsystem = prevHTTP
		newGRPCSubsystem = prevGRPC
	})
	newHTTPSubsystem = func(_ *config.Config, _ string, _ *slog.Logger) subsystem { return httpSub }
	newGRPCSubsystem = func(_ *config.Config, _ chunkstore.Store, _ *slog.Logger) subsystem { return grpcSub }
}

func TestRun_NilConfig(t *testing.T) {
	err := Run(context.Background(), nil, discardLogger(), "v0")
	if err == nil || !strings.Contains(err.Error(), "cfg is nil") {
		t.Errorf("expected nil-cfg error, got %v", err)
	}
}

func TestRun_NilLogger(t *testing.T) {
	err := Run(context.Background(), testConfig(t), nil, "v0")
	if err == nil || !strings.Contains(err.Error(), "logger is nil") {
		t.Errorf("expected nil-logger error, got %v", err)
	}
}

func TestRun_BadEncryptionKeyLength(t *testing.T) {
	cfg := testConfig(t)
	cfg.EncryptionKey = []byte("too-short")
	err := Run(context.Background(), cfg, discardLogger(), "v0")
	if err == nil || !strings.Contains(err.Error(), "encryption key") {
		t.Errorf("expected encryption-key error, got %v", err)
	}
}

func TestRun_ChunkStoreInitFailure(t *testing.T) {
	prev := newChunkStore
	t.Cleanup(func() { newChunkStore = prev })
	newChunkStore = func(*config.Config, *crypto.Cipher) (chunkstore.Store, error) {
		return nil, errors.New("simulated mount failure")
	}
	err := Run(context.Background(), testConfig(t), discardLogger(), "v0")
	if err == nil || !strings.Contains(err.Error(), "chunk store") {
		t.Errorf("expected chunk-store error, got %v", err)
	}
}

func TestRun_GracefulShutdownOnContextCancel(t *testing.T) {
	httpSub := newFakeSubsystem("http", nil, nil, true)
	grpcSub := newFakeSubsystem("grpc", nil, nil, true)
	installFakes(t, httpSub, grpcSub)

	cfg := testConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, discardLogger(), "v0") }()

	// Wait until both subsystems are actually started.
	<-httpSub.startCh
	<-grpcSub.startCh

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil after cancel", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s of ctx cancel")
	}
}

func TestRun_HTTPSubsystemStartFails(t *testing.T) {
	httpSub := newFakeSubsystem("http", errors.New("simulated bind failure"), nil, false)
	grpcSub := newFakeSubsystem("grpc", nil, nil, true)
	installFakes(t, httpSub, grpcSub)

	err := Run(context.Background(), testConfig(t), discardLogger(), "v0")
	if err == nil || !strings.Contains(err.Error(), "http") || !strings.Contains(err.Error(), "before silod was fully running") {
		t.Errorf("expected subsystem-failure error naming http, got %v", err)
	}
}

func TestRun_SubsystemExitsCleanlyWithoutSignal(t *testing.T) {
	// blockStart=false + startErr=nil simulates Start returning early
	// without a Shutdown ever being called. Run should treat this as
	// unexpected and surface a "please file a bug" error.
	httpSub := newFakeSubsystem("http", nil, nil, false)
	grpcSub := newFakeSubsystem("grpc", nil, nil, true)
	installFakes(t, httpSub, grpcSub)

	err := Run(context.Background(), testConfig(t), discardLogger(), "v0")
	if err == nil || !strings.Contains(err.Error(), "without a shutdown signal") {
		t.Errorf("expected unexpected-exit error, got %v", err)
	}
}

func TestRun_ShutdownErrorIsLoggedAndReturned(t *testing.T) {
	httpSub := newFakeSubsystem("http", nil, errors.New("simulated http shutdown failure"), true)
	grpcSub := newFakeSubsystem("grpc", nil, nil, true)
	installFakes(t, httpSub, grpcSub)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, testConfig(t), discardLogger(), "v0") }()

	<-httpSub.startCh
	<-grpcSub.startCh
	cancel()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "simulated http shutdown failure") {
			t.Errorf("expected shutdown error, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s")
	}
}

func TestRun_ServesHealthAndMetricsWhileRunning(t *testing.T) {
	// End-to-end smoke through Run: real HTTP listener on an ephemeral
	// port, real (in-memory chunk store underneath), then cancel.
	cfg := testConfig(t)
	port := freeTCPPort(t)
	cfg.HTTPAddr = "127.0.0.1:" + port
	cfg.GRPCAddr = "127.0.0.1:" + freeTCPPort(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, discardLogger(), "v0-int") }()

	url := "http://" + cfg.HTTPAddr
	if !waitForServer(url+"/healthz", 2*time.Second) {
		cancel()
		<-done
		t.Fatal("server did not become reachable within 2s")
	}

	resp, err := http.Get(url + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), `"node":"test-node"`) {
		t.Errorf("/healthz response: missing node id, got %q", body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v after cancel, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not shut down within 3s")
	}
}

// freeTCPPort returns a port that was free at call time. Inherently racy
// but adequate for a single-process integration test.
func freeTCPPort(t *testing.T) string {
	t.Helper()
	ln, err := listenTCP()
	if err != nil {
		t.Fatalf("could not find a free TCP port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	i := strings.LastIndex(addr, ":")
	return addr[i+1:]
}

func waitForServer(url string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 300 {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
