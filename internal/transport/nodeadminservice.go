package transport

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/hyperized/silo/api/proto/silo/node/v1"
	"github.com/hyperized/silo/internal/backup"
)

// Drainer drains the local node out of the cluster. The gossip subsystem
// satisfies it: it marks the node Left and broadcasts that over gossip.
type Drainer interface {
	Drain() bool
}

// BackupRunner starts a backup run and waits for it. *backup.Subsystem
// satisfies it.
type BackupRunner interface {
	RunNow(ctx context.Context) (backup.Run, error)
}

// NodeAdminService exposes node-lifecycle operations to operators. It is kept
// separate from the read-only ClusterStatus service because these RPCs mutate
// the node.
type NodeAdminService struct {
	drainer Drainer
	backup  BackupRunner
	nodeID  string
	logger  *slog.Logger
}

// NodeAdminOption configures a NodeAdminService.
type NodeAdminOption func(*NodeAdminService)

// WithBackupRunner enables RunBackup. Without it the RPC reports that backups
// are not configured on this node.
func WithBackupRunner(r BackupRunner) NodeAdminOption {
	return func(s *NodeAdminService) { s.backup = r }
}

// NewNodeAdminService wires the node-admin service onto the drainer.
func NewNodeAdminService(drainer Drainer, nodeID string, logger *slog.Logger, opts ...NodeAdminOption) *NodeAdminService {
	s := &NodeAdminService{drainer: drainer, nodeID: nodeID, logger: logger}
	for _, opt := range opts {
		opt(s)
	}
	return s
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

// RunBackup runs a backup now and returns its outcome. A failed run is a
// successful RPC whose run has ok=false: the call did what was asked, and the
// run's error explains the failure. Refusals (not configured, already running,
// shutting down) are FAILED_PRECONDITION; a caller that gives up waiting gets
// DEADLINE_EXCEEDED or CANCELED while the run continues on the node.
func (s *NodeAdminService) RunBackup(ctx context.Context, _ *nodev1.RunBackupRequest) (*nodev1.RunBackupResponse, error) {
	if s.backup == nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"backups are not configured on node %s; set SILO_BACKUP_TARGET (for example s3://bucket/prefix) on that node and restart it", s.nodeID)
	}
	s.logger.Info("backup started on operator request", "node", s.nodeID)
	run, err := s.backup.RunNow(ctx)
	switch {
	case errors.Is(err, backup.ErrRunInProgress):
		return nil, status.Errorf(codes.FailedPrecondition,
			"a backup is already running on node %s; wait for it to finish ('siloctl status' shows progress) and try again", s.nodeID)
	case errors.Is(err, backup.ErrStopped):
		return nil, status.Errorf(codes.FailedPrecondition,
			"node %s is shutting down and cannot start a backup; retry once it is back up", s.nodeID)
	case err != nil:
		return nil, status.FromContextError(err).Err()
	}
	return &nodev1.RunBackupResponse{NodeId: s.nodeID, Run: backupRunProto(&run)}, nil
}

// WithNodeAdminService registers the operator-facing NodeAdmin service.
func WithNodeAdminService(svc nodev1.NodeAdminServer) GRPCOption {
	return func(c *grpcConfig) {
		c.services = append(c.services, func(s *grpc.Server) { nodev1.RegisterNodeAdminServer(s, svc) })
	}
}

// Compile-time check that NodeAdminService satisfies the generated server.
var _ nodev1.NodeAdminServer = (*NodeAdminService)(nil)
