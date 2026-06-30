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

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	extentv1 "github.com/hyperized/silo/api/proto/silo/extent/v1"
	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
	"github.com/hyperized/silo/internal/chunkstore"
)

// grpcConfig accumulates what a GRPCOption contributes: extra service
// registrations (applied after the server is built) and grpc.ServerOptions
// (applied at construction, where interceptors must be set).
type grpcConfig struct {
	services   []func(*grpc.Server)
	serverOpts []grpc.ServerOption
	nsOpts     []NamespaceOption
}

// GRPCOption configures the gRPC server at construction — registering an extra
// service or contributing a server option such as an auth interceptor.
type GRPCOption func(*grpcConfig)

// WithStatusService registers the operator-facing ClusterStatus service.
func WithStatusService(svc statusv1.ClusterStatusServer) GRPCOption {
	return func(c *grpcConfig) {
		c.services = append(c.services, func(s *grpc.Server) { statusv1.RegisterClusterStatusServer(s, svc) })
	}
}

// WithExtentService registers the extent-map replica service so peers can
// replicate, fetch, and stat a volume's extent map on this node.
func WithExtentService(svc extentv1.ExtentMapServer) GRPCOption {
	return func(c *grpcConfig) {
		c.services = append(c.services, func(s *grpc.Server) { extentv1.RegisterExtentMapServer(s, svc) })
	}
}

// WithNamespaceExtentDeleter wires an extent deleter into the namespace service
// so removing a volume also deletes its extent map from the replica set.
func WithNamespaceExtentDeleter(d ExtentDeleter) GRPCOption {
	return func(c *grpcConfig) { c.nsOpts = append(c.nsOpts, WithExtentDeleter(d)) }
}

// WithNamespaceExtentSnapshotter wires an extent snapshotter into the namespace
// service so snapshotting a volume also clones its out-of-band extent map onto
// the snapshot's replica set. Wire it only under extent replication.
func WithNamespaceExtentSnapshotter(s ExtentSnapshotter) GRPCOption {
	return func(c *grpcConfig) { c.nsOpts = append(c.nsOpts, WithExtentSnapshotter(s)) }
}

// WithTokenAuth installs the capability-token interceptors (unary + stream) so
// client-cert callers must present a token scoped to each operation. Node-cert
// peers are exempt; see TokenAuthenticator.
func WithTokenAuth(a *TokenAuthenticator) GRPCOption {
	return func(c *grpcConfig) {
		c.serverOpts = append(c.serverOpts,
			grpc.UnaryInterceptor(a.UnaryInterceptor),
			grpc.StreamInterceptor(a.StreamInterceptor),
		)
	}
}

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

// NewGRPCServer wires the chunk service onto a fresh grpc.Server. The
// tlsCfg is required: silod runs every gRPC surface under mTLS so peer
// identity is part of the wire protocol, not a hope-for-the-best
// network ACL. Pass clustertls.ServerConfig(ca, nodeCert).
func NewGRPCServer(addr string, tlsCfg *tls.Config, store chunkstore.Store, coord Coordinator, ns NamespaceOps, logger *slog.Logger, opts ...GRPCOption) *GRPCServer {
	cfg := grpcConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	serverOpts := append([]grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsCfg))}, cfg.serverOpts...)
	s := grpc.NewServer(serverOpts...)
	chunkv1.RegisterChunkStoreServer(s, NewChunkService(store, coord, logger))
	namespacev1.RegisterNamespaceStoreServer(s, NewNamespaceService(ns, logger, cfg.nsOpts...))
	for _, register := range cfg.services {
		register(s)
	}
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
		return fmt.Errorf("could not bind gRPC listener at %q (%w); set SILO_GRPC_ADDR to a free, reachable host:port, e.g. 0.0.0.0:7000", s.addr, err)
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
		return fmt.Errorf("gRPC shutdown deadline expired (%w); in-flight RPCs were terminated forcefully — increase the shutdown budget or investigate slow handlers", ctx.Err())
	}
}
