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

	"github.com/hyperized/silo/internal/config"
)

// fakeHTTPService is an httpService double for lifecycle edge-case tests.
type fakeHTTPService struct {
	startErr     error // returned from Start (immediately if !blocking)
	blockStart   bool  // if true, Start blocks until Shutdown is called
	shutdownErr  error // returned from Shutdown

	mu       sync.Mutex
	startCh  chan struct{}
	shutdown chan struct{}
}

func newFakeHTTP(startErr, shutdownErr error, blocking bool) *fakeHTTPService {
	return &fakeHTTPService{
		startErr:    startErr,
		blockStart:  blocking,
		shutdownErr: shutdownErr,
		startCh:     make(chan struct{}),
		shutdown:    make(chan struct{}),
	}
}

func (f *fakeHTTPService) Start() error {
	close(f.startCh)
	if !f.blockStart {
		return f.startErr
	}
	<-f.shutdown
	return f.startErr
}

func (f *fakeHTTPService) Shutdown(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.shutdown:
	default:
		close(f.shutdown)
	}
	return f.shutdownErr
}

// installFakeHTTP swaps the HTTP factory for the duration of the test.
func installFakeHTTP(t *testing.T, f *fakeHTTPService) {
	t.Helper()
	prev := newHTTPService
	t.Cleanup(func() { newHTTPService = prev })
	newHTTPService = func(string, string, string, *slog.Logger) httpService { return f }
}

// testConfig returns a minimal valid Config bound to an ephemeral port.
func testConfig() *config.Config {
	return &config.Config{
		NodeID:        "test-node",
		GRPCAddr:      "127.0.0.1:0",
		HTTPAddr:      "127.0.0.1:0",
		DataDir:       "/tmp/silo-test",
		ChunkSize:     4 * 1024 * 1024,
		Replication:   1,
		KeySource:     config.KeySourceStatic,
		EncryptionKey: make([]byte, 32),
		LogLevel:      "info",
		LogFormat:     "text",
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRun_NilConfig(t *testing.T) {
	err := Run(context.Background(), nil, discardLogger(), "v0")
	if err == nil || !strings.Contains(err.Error(), "cfg is nil") {
		t.Errorf("expected nil-cfg error, got %v", err)
	}
}

func TestRun_NilLogger(t *testing.T) {
	err := Run(context.Background(), testConfig(), nil, "v0")
	if err == nil || !strings.Contains(err.Error(), "logger is nil") {
		t.Errorf("expected nil-logger error, got %v", err)
	}
}

func TestRun_GracefulShutdownOnContextCancel(t *testing.T) {
	cfg := testConfig()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, discardLogger(), "v0-test")
	}()

	// Give Run a moment to bind the listener.
	time.Sleep(50 * time.Millisecond)

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil after graceful cancel", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s of ctx cancel")
	}
}

func TestRun_HTTPBindFailure(t *testing.T) {
	cfg := testConfig()
	cfg.HTTPAddr = "not-an-address"

	err := Run(context.Background(), cfg, discardLogger(), "v0-test")
	if err == nil {
		t.Fatal("expected a bind error, got nil")
	}
	if !strings.Contains(err.Error(), "SILO_HTTP_ADDR") {
		t.Errorf("error should mention SILO_HTTP_ADDR for the operator, got %v", err)
	}
}

func TestRun_HTTPExitsBeforeShutdownSignal(t *testing.T) {
	// Start returns nil immediately, without Shutdown ever being called.
	// Run should detect this and return an actionable error.
	installFakeHTTP(t, newFakeHTTP(nil, nil, false))

	err := Run(context.Background(), testConfig(), discardLogger(), "v0")
	if err == nil || !strings.Contains(err.Error(), "without an error and without a shutdown signal") {
		t.Errorf("expected unexpected-exit error, got %v", err)
	}
}

func TestRun_ShutdownError(t *testing.T) {
	installFakeHTTP(t, newFakeHTTP(nil, errors.New("simulated shutdown failure"), true))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, testConfig(), discardLogger(), "v0") }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "shut down cleanly") {
			t.Errorf("expected actionable shutdown error, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s")
	}
}

func TestRun_ServesHealthAndMetricsWhileRunning(t *testing.T) {
	cfg := testConfig()
	cfg.HTTPAddr = "127.0.0.1:0"
	// We need the bound address before Run returns. Easiest approach:
	// build the server ourselves, scrape the addr, then teardown.
	// Instead we use a fixed port for this test.
	port := freeTCPPort(t)
	cfg.HTTPAddr = "127.0.0.1:" + port

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, discardLogger(), "v0-int") }()

	// Wait for the listener to come up.
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

	resp2, err := http.Get(url + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()
	if !strings.Contains(string(body2), `silo_build_info`) {
		t.Errorf("/metrics response: missing silo_build_info, got %q", body2)
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

// freeTCPPort returns a port that was free at call time. Inherently racy,
// but adequate for a single-process integration test.
func freeTCPPort(t *testing.T) string {
	t.Helper()
	ln, err := listenTCP()
	if err != nil {
		t.Fatalf("could not find a free TCP port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	// addr is of the form 127.0.0.1:NNNNN
	i := strings.LastIndex(addr, ":")
	return addr[i+1:]
}

// waitForServer polls url until it returns 2xx or the deadline passes.
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
