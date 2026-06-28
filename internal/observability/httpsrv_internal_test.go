package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

// newTestServer starts a Server bound to an ephemeral port and returns it
// together with a cleanup function. Tests can hit s.Addr() over HTTP.
func newTestServer(t *testing.T, opts ...Option) *Server {
	t.Helper()
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := NewServer("127.0.0.1:0", "node-test", "v0.0.0-test", lg, opts...)

	started := make(chan struct{})
	go func() {
		// Start blocks; signal as soon as Start enters Serve.
		// We can't observe Serve directly, so we let the listener
		// be created and rely on Addr() to surface the bound port.
		close(started)
		_ = s.Start()
	}()

	// Wait for the listener to be bound. Start binds synchronously
	// before Serve, but the goroutine boundary means we must poll briefly.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.Addr() != "" {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if s.Addr() == "" {
		t.Fatal("server did not bind within 2s")
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	_ = started
	return s
}

func TestServer_HealthzReturnsOK(t *testing.T) {
	s := newTestServer(t)
	resp, err := http.Get("http://" + s.Addr() + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Errorf("body: got %q, want status:ok", body)
	}
	if !strings.Contains(string(body), `"node":"node-test"`) {
		t.Errorf("body: missing node id, got %q", body)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("content-type: got %q, want application/json", got)
	}
}

func TestServer_MetricsDelegatesToHandler(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "silo_test_metric 1\n")
	})
	s := newTestServer(t, WithMetricsHandler(handler))
	resp, err := http.Get("http://" + s.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "silo_test_metric 1") {
		t.Errorf("metrics body did not come from the mounted handler: %q", body)
	}
}

func TestServer_MetricsAbsentWithoutHandler(t *testing.T) {
	// With no metrics handler the route is not mounted at all.
	s := newTestServer(t)
	resp, err := http.Get("http://" + s.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404 when no metrics handler is set", resp.StatusCode)
	}
}

func TestServer_DebugProfilesMountedWithOption(t *testing.T) {
	s := newTestServer(t, WithDebugProfiles())
	// The index lists the available profiles...
	resp, err := http.Get("http://" + s.Addr() + "/debug/pprof/")
	if err != nil {
		t.Fatalf("GET /debug/pprof/: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200 with WithDebugProfiles", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Types of profiles available") {
		t.Errorf("index body did not look like pprof: %q", body)
	}
	// ...and a named profile (heap) is dispatched by the subtree route too.
	hp, err := http.Get("http://" + s.Addr() + "/debug/pprof/heap?debug=1")
	if err != nil {
		t.Fatalf("GET /debug/pprof/heap: %v", err)
	}
	defer hp.Body.Close()
	if hp.StatusCode != http.StatusOK {
		t.Errorf("heap profile status: got %d, want 200", hp.StatusCode)
	}
}

func TestServer_DebugProfilesAbsentByDefault(t *testing.T) {
	// Opt-in: without WithDebugProfiles the pprof routes are not mounted.
	s := newTestServer(t)
	resp, err := http.Get("http://" + s.Addr() + "/debug/pprof/")
	if err != nil {
		t.Fatalf("GET /debug/pprof/: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404 when debug profiles are not enabled", resp.StatusCode)
	}
}

func TestServer_RejectsWrongMethod(t *testing.T) {
	s := newTestServer(t)
	resp, err := http.Post("http://"+s.Addr()+"/healthz", "text/plain", strings.NewReader(""))
	if err != nil {
		t.Fatalf("POST /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST status: got %d, want 405", resp.StatusCode)
	}
}

func TestServer_AddrEmptyBeforeStart(t *testing.T) {
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := NewServer("127.0.0.1:0", "n", "v", lg)
	if s.Addr() != "" {
		t.Errorf("Addr before Start: got %q, want empty", s.Addr())
	}
	if err := s.closeListener(); err != nil {
		t.Errorf("closeListener before Start: got %v, want nil", err)
	}
}

func TestServer_StartFailsOnBadAddress(t *testing.T) {
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := NewServer("not-an-address", "n", "v", lg)
	if err := s.Start(); err == nil {
		t.Error("Start should fail on a malformed address")
	}
}

func TestServer_StartReturnsErrorWhenListenerClosedAbruptly(t *testing.T) {
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := NewServer("127.0.0.1:0", "n", "v", lg)

	errCh := make(chan error, 1)
	go func() { errCh <- s.Start() }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.Addr() != "" {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if s.Addr() == "" {
		t.Fatal("server did not bind within 2s")
	}

	// Close the listener directly, bypassing Shutdown, so Serve returns
	// net.ErrClosed (not http.ErrServerClosed) and Start surfaces an error.
	_ = s.closeListener()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected non-nil error after listener close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return within 2s after listener close")
	}
}
