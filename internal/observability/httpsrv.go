package observability

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// Server is the silod HTTP listener. It serves health and metrics
// endpoints for liveness probes and observability tooling.
//
// In M0, /healthz reports "ok" if the process is alive. Future milestones
// will add deeper readiness checks (gossip joined, chunk store mounted,
// quorum healthy, etc.).
type Server struct {
	nodeID  string
	version string
	logger  *slog.Logger
	srv     *http.Server

	mu sync.Mutex  // guards ln
	ln net.Listener
}

// NewServer wires the HTTP routes. It does not start listening; call Start.
func NewServer(addr, nodeID, version string, logger *slog.Logger) *Server {
	s := &Server{
		nodeID:  nodeID,
		version: version,
		logger:  logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Start blocks until the server stops. The listener is bound synchronously
// before Serve is called, so callers can wire signal-based shutdown without
// races. The s.ln field is guarded by s.mu so concurrent Addr/closeListener
// calls are safe.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("could not bind HTTP listener at %q (%v); set SILO_HTTP_ADDR to a host:port that is free and reachable, e.g. 0.0.0.0:7080", s.srv.Addr, err)
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	s.logger.Info("http listener started", "addr", ln.Addr().String())
	if err := s.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Addr returns the bound address. Useful in tests that start with ":0".
// Returns an empty string before Start has bound the socket.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// closeListener forcibly closes the bound listener if any. Intended only
// for tests that need to exercise the unexpected-listener-close branch.
func (s *Server) closeListener() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Close()
}

// Shutdown stops accepting new requests and waits for in-flight ones,
// bounded by the context deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","node":%q,"version":%q}`+"\n", s.nodeID, s.version)
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP silo_build_info Build information for the running silod.\n")
	fmt.Fprintf(w, "# TYPE silo_build_info gauge\n")
	fmt.Fprintf(w, "silo_build_info{node=%q,version=%q} 1\n", s.nodeID, s.version)
}
