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

// Server runs the /healthz and /metrics HTTP endpoints. /healthz reports
// only "process is alive" today; deeper readiness checks (gossip joined,
// chunk store mounted, quorum healthy) land as those sub-components do.
// Health and metrics share one server because they share a lifecycle and
// listener; splitting them would just add a port for operators to remember.
type Server struct {
	nodeID  string
	version string
	logger  *slog.Logger
	srv     *http.Server
	metrics http.Handler // optional; the /metrics route is mounted only when set

	mu sync.Mutex // guards ln; race detector caught a Start/Addr race
	ln net.Listener
}

// Option configures a Server.
type Option func(*Server)

// WithMetricsHandler mounts handler at GET /metrics. The metrics text is owned
// by the exporter package; this server only hosts the route on its shared
// listener alongside /healthz.
func WithMetricsHandler(handler http.Handler) Option {
	return func(s *Server) { s.metrics = handler }
}

// NewServer wires the routes but does not bind the socket; call Start
// for that. The split matters because bind failures should surface
// during silod startup, not during ServeMux construction in tests.
func NewServer(addr, nodeID, version string, logger *slog.Logger, opts ...Option) *Server {
	s := &Server{
		nodeID:  nodeID,
		version: version,
		logger:  logger,
	}
	for _, opt := range opts {
		opt(s)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	if s.metrics != nil {
		mux.Handle("GET /metrics", s.metrics)
	}
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Start binds the listener and blocks on Serve. Returns nil on a clean
// Shutdown and an error otherwise — distinguishing the two is what lets
// silod's Run know whether it can exit normally.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("could not bind HTTP listener at %q (%w); set SILO_HTTP_ADDR to a host:port that is free and reachable, e.g. 0.0.0.0:7080", s.srv.Addr, err)
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

// Addr returns "" until Start has bound the socket.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// closeListener is a test-only escape hatch that exercises the
// unexpected-listener-close branch of Start.
func (s *Server) closeListener() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Close()
}

// Shutdown stops accepting new requests and waits for in-flight ones to
// finish, bounded by the context deadline. Anything still running when
// the context expires is force-terminated, which is acceptable for the
// stateless health/metrics handlers but worth revisiting once we serve
// long-lived streams.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","node":%q,"version":%q}`+"\n", s.nodeID, s.version)
}
