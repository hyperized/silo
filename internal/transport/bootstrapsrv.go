package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	bootstrapv1 "github.com/hyperized/silo/api/proto/silo/bootstrap/v1"
)

// BootstrapServer hosts the Bootstrap.Join RPC on a dedicated listener.
// The split from the main GRPCServer is intentional: this surface uses
// server-only TLS (the operator hasn't been issued a client cert yet),
// while every other silod RPC requires mTLS. Running them on the same
// port would conflate two trust models on one wire — the dedicated port
// makes the boundary visible from firewalls and netstat.
type BootstrapServer struct {
	addr   string
	logger *slog.Logger
	server *grpc.Server

	mu sync.Mutex // guards ln; same shape as the HTTP/gRPC listener race fix
	ln net.Listener
}

// NewBootstrapServer wires the bootstrap service onto a fresh
// grpc.Server. The TLS config is server-only (no client-cert request) so
// operators on a brand-new laptop can reach the endpoint before they
// have any cluster-issued material.
func NewBootstrapServer(addr string, tlsCfg *tls.Config, svc *BootstrapService, logger *slog.Logger) *BootstrapServer {
	creds := credentials.NewTLS(tlsCfg)
	s := grpc.NewServer(grpc.Creds(creds))
	bootstrapv1.RegisterBootstrapServer(s, svc)
	return &BootstrapServer{
		addr:   addr,
		logger: logger,
		server: s,
	}
}

// Start binds and serves. Returns nil on graceful Shutdown.
func (s *BootstrapServer) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("could not bind bootstrap listener at %q (%w); set SILO_BOOTSTRAP_ADDR to a free, reachable host:port, e.g. 0.0.0.0:7001", s.addr, err)
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	s.logger.Info("bootstrap listener started", "addr", ln.Addr().String())
	if err := s.server.Serve(ln); err != nil {
		return err
	}
	return nil
}

// Addr returns "" until Start has bound the socket.
func (s *BootstrapServer) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Shutdown waits for in-flight RPCs up to the context deadline. Mirrors
// GRPCServer.Shutdown so silod's lifecycle code can treat the two
// listeners the same way.
func (s *BootstrapServer) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.server.Stop()
		return fmt.Errorf("bootstrap shutdown deadline expired (%w); in-flight RPCs were terminated forcefully — increase the shutdown budget or investigate slow handlers", ctx.Err())
	}
}
