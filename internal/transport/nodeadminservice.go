package transport

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"

	nodev1 "github.com/hyperized/silo/api/proto/silo/node/v1"
)

// Drainer drains the local node out of the cluster. The gossip subsystem
// satisfies it: it marks the node Left and broadcasts that over gossip.
type Drainer interface {
	Drain() bool
}

// NodeAdminService exposes node-lifecycle operations to operators. It is kept
// separate from the read-only ClusterStatus service because these RPCs mutate
// the node.
type NodeAdminService struct {
	drainer Drainer
	nodeID  string
	logger  *slog.Logger
}

// NewNodeAdminService wires the node-admin service onto the drainer.
func NewNodeAdminService(drainer Drainer, nodeID string, logger *slog.Logger) *NodeAdminService {
	return &NodeAdminService{drainer: drainer, nodeID: nodeID, logger: logger}
}

// Drain marks this node as having left the cluster and announces it over gossip,
// so peers re-replicate its chunks onto survivors. The node keeps serving until
// it is removed; the operator watches silo_replication_shortfall_chunks reach
// zero first. Idempotent: a second call reports announced=false.
func (s *NodeAdminService) Drain(_ context.Context, _ *nodev1.DrainRequest) (*nodev1.DrainResponse, error) {
	announced := s.drainer.Drain()
	if announced {
		s.logger.Info("node draining on operator request; keep it running until re-replication completes", "node", s.nodeID)
	}
	return &nodev1.DrainResponse{NodeId: s.nodeID, Announced: announced}, nil
}

// WithNodeAdminService registers the operator-facing NodeAdmin service.
func WithNodeAdminService(svc nodev1.NodeAdminServer) GRPCOption {
	return func(s *grpc.Server) { nodev1.RegisterNodeAdminServer(s, svc) }
}

// Compile-time check that NodeAdminService satisfies the generated server.
var _ nodev1.NodeAdminServer = (*NodeAdminService)(nil)
