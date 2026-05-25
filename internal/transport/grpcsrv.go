package transport

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"google.golang.org/grpc"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	"github.com/hyperized/silo/internal/chunkstore"
)

// GRPCServer is silod's gRPC listener. Split from the HTTP listener
// because gRPC's lifecycle (GracefulStop) and HTTP's (Shutdown) take
// different shapes; collapsing them under one wrapper would obscure
// both.
type GRPCServer struct {
	addr   string
	logger *slog.Logger
	server *grpc.Server

	mu sync.Mutex // guards ln; race-detector found a Start/Addr race in the HTTP side, same shape here
	ln net.Listener
}

// NewGRPCServer wires the chunk service onto a fresh grpc.Server.
// Additional services (replication, namespace) register here later.
func NewGRPCServer(addr string, store chunkstore.Store, logger *slog.Logger) *GRPCServer {
	s := grpc.NewServer()
	chunkv1.RegisterChunkStoreServer(s, NewChunkService(store, logger))
	return &GRPCServer{
		addr:   addr,
		logger: logger,
		server: s,
	}
}

// Start binds and serves. Returns nil on graceful Shutdown.
func (s *GRPCServer) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("could not bind gRPC listener at %q (%v); set SILO_GRPC_ADDR to a free, reachable host:port, e.g. 0.0.0.0:7000", s.addr, err)
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	s.logger.Info("grpc listener started", "addr", ln.Addr().String())
	if err := s.server.Serve(ln); err != nil {
		return err
	}
	return nil
}

// Addr returns "" until Start has bound the socket.
func (s *GRPCServer) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Shutdown waits for in-flight RPCs up to the context deadline, then
// force-stops. The race between GracefulStop returning and the ctx
// expiring is resolved in favour of bounded shutdown so silod always
// honours the orchestrator's deadline.
func (s *GRPCServer) Shutdown(ctx context.Context) error {
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
		return fmt.Errorf("gRPC shutdown deadline expired (%v); in-flight RPCs were terminated forcefully — increase the shutdown budget or investigate slow handlers", ctx.Err())
	}
}
