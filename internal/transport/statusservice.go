package transport

import (
	"context"
	"log/slog"

	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
	"github.com/hyperized/silo/internal/diskusage"
	"github.com/hyperized/silo/internal/membership"
)

// StatusMembers is the slice of the membership layer the status service reads.
// *membership.Membership satisfies it.
type StatusMembers interface {
	Members() []membership.Node
}

// StatusStore is the slice of the chunk store the status service reads (the
// local chunk inventory). chunkstore.Store satisfies it.
type StatusStore interface {
	List(ctx context.Context) ([]string, error)
}

// StatusService answers the operator-facing ClusterStatus RPC. It reports the
// membership the receiving node currently sees (its gossip view) plus that
// node's local storage — there is no global aggregator, so this is one node's
// honest snapshot, which is exactly the decentralised model silo runs on.
type StatusService struct {
	members   StatusMembers
	store     StatusStore
	dataDir   string
	nodeID    string
	version   string
	diskUsage func(path string) (diskusage.Usage, error)
	logger    *slog.Logger
}

// StatusOption configures a StatusService.
type StatusOption func(*StatusService)

// WithDiskUsage overrides how the service measures the data directory's
// filesystem (tests inject a fake; production uses statfs).
func WithDiskUsage(fn func(path string) (diskusage.Usage, error)) StatusOption {
	return func(s *StatusService) { s.diskUsage = fn }
}

// NewStatusService wires the status service onto the membership layer and the
// local chunk store. dataDir is the chunk directory whose filesystem usage is
// reported; nodeID and version identify the responding node.
func NewStatusService(members StatusMembers, store StatusStore, dataDir, nodeID, version string, logger *slog.Logger, opts ...StatusOption) *StatusService {
	s := &StatusService{
		members:   members,
		store:     store,
		dataDir:   dataDir,
		nodeID:    nodeID,
		version:   version,
		diskUsage: diskusage.Measure,
		logger:    logger,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// GetStatus returns the responding node's view of cluster membership and its
// local storage. Storage figures that cannot be read (a transient statfs or
// list error) are logged and left zero rather than failing the whole call —
// "which nodes are up" is the operator's first question and must always answer.
func (s *StatusService) GetStatus(ctx context.Context, _ *statusv1.GetStatusRequest) (*statusv1.GetStatusResponse, error) {
	members := s.members.Members()
	nodes := make([]*statusv1.NodeStatus, 0, len(members))
	for _, n := range members {
		nodes = append(nodes, &statusv1.NodeStatus{
			Id:             n.ID,
			GossipAddress:  n.Address,
			DataAddress:    n.DataAddress,
			State:          protoNodeState(n.State),
			Incarnation:    n.Incarnation,
			LastChangeUnix: n.LastChange.Unix(),
			CapacityBytes:  n.CapacityBytes,
			UsedBytes:      n.UsedBytes,
		})
	}

	storage := &statusv1.StorageStatus{DataDir: s.dataDir}
	if du, err := s.diskUsage(s.dataDir); err != nil {
		s.logger.Warn("status: could not measure the data directory", "dir", s.dataDir, "err", err)
	} else {
		storage.CapacityBytes = du.CapacityBytes
		storage.UsedBytes = du.UsedBytes
		storage.AvailableBytes = du.AvailableBytes
	}
	if ids, err := s.store.List(ctx); err != nil {
		s.logger.Warn("status: could not list local chunks", "err", err)
	} else {
		storage.ChunkCount = int64(len(ids))
	}

	return &statusv1.GetStatusResponse{
		RespondingNodeId: s.nodeID,
		Version:          s.version,
		Nodes:            nodes,
		Storage:          storage,
	}, nil
}

// protoNodeState maps a SWIM membership state to its wire enum.
func protoNodeState(st membership.State) statusv1.NodeState {
	switch st {
	case membership.StateAlive:
		return statusv1.NodeState_NODE_STATE_ALIVE
	case membership.StateSuspect:
		return statusv1.NodeState_NODE_STATE_SUSPECT
	case membership.StateDead:
		return statusv1.NodeState_NODE_STATE_DEAD
	case membership.StateLeft:
		return statusv1.NodeState_NODE_STATE_LEFT
	default:
		return statusv1.NodeState_NODE_STATE_UNSPECIFIED
	}
}

// Compile-time check that StatusService satisfies the generated server.
var _ statusv1.ClusterStatusServer = (*StatusService)(nil)
