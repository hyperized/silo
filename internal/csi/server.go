package csi

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
)

// Server hosts the CSI gRPC services on a single endpoint (a unix socket the
// kubelet and the CSI sidecars dial). Which services are registered is up to
// the caller, matching the process mode.
type Server struct {
	endpoint string
	grpc     *grpc.Server
	logger   *slog.Logger
}

// ServerOption registers a CSI service on the server.
type ServerOption func(*Server)

// WithIdentity registers the Identity service (always required).
func WithIdentity(svc *IdentityService) ServerOption {
	return func(s *Server) { csiv1.RegisterIdentityServer(s.grpc, svc) }
}

// WithController registers the Controller service.
func WithController(svc *ControllerService) ServerOption {
	return func(s *Server) { csiv1.RegisterControllerServer(s.grpc, svc) }
}

// WithNode registers the Node service.
func WithNode(svc *NodeService) ServerOption {
	return func(s *Server) { csiv1.RegisterNodeServer(s.grpc, svc) }
}

// NewServer builds a CSI server listening at endpoint (e.g.
// unix:///csi/csi.sock) with the given services registered.
func NewServer(endpoint string, logger *slog.Logger, opts ...ServerOption) *Server {
	s := &Server{endpoint: endpoint, grpc: grpc.NewServer(), logger: logger}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Serve listens on the endpoint and serves until ctx is cancelled, then drains
// in-flight RPCs gracefully. A stale unix socket left by a previous run is
// removed first so a restart binds cleanly.
func (s *Server) Serve(ctx context.Context) error {
	lis, err := s.listen()
	if err != nil {
		return err
	}
	return s.serve(ctx, lis)
}

// listen resolves the endpoint and binds it, clearing a stale unix socket first.
func (s *Server) listen() (net.Listener, error) {
	network, addr, err := parseEndpoint(s.endpoint)
	if err != nil {
		return nil, err
	}
	if network == "unix" {
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("could not remove the stale CSI socket %s (%w); delete it and retry", addr, err)
		}
	}
	lis, err := net.Listen(network, addr)
	if err != nil {
		return nil, fmt.Errorf("could not listen on CSI endpoint %s (%w)", s.endpoint, err)
	}
	return lis, nil
}

// serve runs the gRPC server on lis until ctx is cancelled (graceful drain) or
// serving fails on its own.
func (s *Server) serve(ctx context.Context, lis net.Listener) error {
	errCh := make(chan error, 1)
	go func() { errCh <- s.grpc.Serve(lis) }()
	if s.logger != nil {
		s.logger.Info("silo-csi serving", "endpoint", s.endpoint)
	}

	select {
	case <-ctx.Done():
		s.grpc.GracefulStop()
		return nil
	case err := <-errCh:
		return fmt.Errorf("CSI server stopped serving %s (%w)", s.endpoint, err)
	}
}

// parseEndpoint splits a CSI endpoint into a net.Listen network and address. It
// accepts unix:// and tcp:// schemes — the two the CSI spec uses.
func parseEndpoint(endpoint string) (network, addr string, err error) {
	switch {
	case strings.HasPrefix(endpoint, "unix://"):
		return "unix", strings.TrimPrefix(endpoint, "unix://"), nil
	case strings.HasPrefix(endpoint, "tcp://"):
		return "tcp", strings.TrimPrefix(endpoint, "tcp://"), nil
	default:
		return "", "", fmt.Errorf("CSI endpoint %q must start with unix:// or tcp://", endpoint)
	}
}
