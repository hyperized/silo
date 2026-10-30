// Package clusterstatus assembles the operator status snapshot a node reports
// about itself and the cluster as it sees it. The gRPC ClusterStatus service and
// the JSON endpoint behind the web UI both render the same Builder output, so
// `siloctl status` and the browser can never disagree about what a node said.
package clusterstatus

import (
	"context"
	"log/slog"
	"time"

	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
	"github.com/hyperized/silo/internal/diskusage"
	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/namespace"
)

// Members is the slice of the membership layer the builder reads.
// *membership.Membership satisfies it.
type Members interface {
	Members() []membership.Node
}

// ChunkLister is the slice of the chunk store the builder reads.
// chunkstore.Store satisfies it.
type ChunkLister interface {
	List(ctx context.Context) ([]string, error)
}

// VolumeLister enumerates the volumes in the namespace. *namespace.Namespace
// satisfies it.
type VolumeLister interface {
	Volumes() []namespace.VolumeInfo
}

// ExtentCounter reports how many mapped extents this node's out-of-band extent
// map replica holds for a volume; extents a trim unmapped are not counted.
// *extentmap.Store satisfies it.
type ExtentCounter interface {
	Has(volumeID string) bool
	Len(volumeID string) int
}

// Builder produces GetStatusResponse snapshots. It holds no mutable state of
// its own, so it is safe for concurrent use as long as its sources are, which
// the production ones (membership, chunk store, namespace, extent store,
// backup, clock-skew monitor, gossip, chunk GC, router, NBD server) are.
type Builder struct {
	members   Members
	chunks    ChunkLister
	volumes   VolumeLister
	extents   ExtentCounter
	dataDir   string
	nodeID    string
	version   string
	diskUsage func(path string) (diskusage.Usage, error)
	backup    BackupReporter
	logger    *slog.Logger

	skew        SkewReporter
	gossip      GossipHealth
	gc          GCReporter
	damage      MapDamage
	replicas    ReplicaResolver
	attached    AttachedLister
	replication int
	started     time.Time
}

// Option configures a Builder.
type Option func(*Builder)

// WithDiskUsage overrides how the data directory's filesystem is measured.
// Production uses statfs; tests inject a fake.
func WithDiskUsage(fn func(path string) (diskusage.Usage, error)) Option {
	return func(b *Builder) { b.diskUsage = fn }
}

// WithVolumes adds the volume table to the snapshot. Without it the response
// carries no volumes.
func WithVolumes(v VolumeLister) Option {
	return func(b *Builder) { b.volumes = v }
}

// WithExtentCounts lets the volume table count extents held in the out-of-band
// extent map store, where they live under extent replication.
func WithExtentCounts(c ExtentCounter) Option {
	return func(b *Builder) { b.extents = c }
}

// New returns a Builder over the membership view and the local chunk store.
// dataDir is the chunk directory whose filesystem usage is reported; nodeID
// and version identify the responding node.
func New(members Members, chunks ChunkLister, dataDir, nodeID, version string, logger *slog.Logger, opts ...Option) *Builder {
	b := &Builder{
		members:   members,
		chunks:    chunks,
		dataDir:   dataDir,
		nodeID:    nodeID,
		version:   version,
		diskUsage: diskusage.Measure,
		logger:    logger,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Build returns the responding node's view of cluster membership, its local
// storage, the volumes in its namespace replica and its backup state. It never
// fails: storage figures that cannot be read are logged and left zero, because
// "which nodes are up" is the operator's first question and must always get an
// answer.
func (b *Builder) Build(ctx context.Context) *statusv1.GetStatusResponse {
	members := b.members.Members()
	skews := b.peerSkews()
	return &statusv1.GetStatusResponse{
		RespondingNodeId: b.nodeID,
		Version:          b.version,
		Nodes:            b.nodes(members, skews),
		Storage:          b.storage(ctx),
		Volumes:          b.volumeTable(),
		Backup:           backupStatus(b.backup),
		Health:           b.health(members, skews),
		Gc:               gcStatus(b.gc),
	}
}

func (b *Builder) nodes(members []membership.Node, skews map[string]time.Duration) []*statusv1.NodeStatus {
	out := make([]*statusv1.NodeStatus, 0, len(members))
	for _, n := range members {
		var skew time.Duration
		if n.ID != b.nodeID {
			skew = skews[n.ID]
		}
		out = append(out, &statusv1.NodeStatus{
			Id:             n.ID,
			GossipAddress:  n.Address,
			DataAddress:    n.DataAddress,
			State:          protoNodeState(n.State),
			Incarnation:    n.Incarnation,
			LastChangeUnix: n.LastChange.Unix(),
			CapacityBytes:  n.CapacityBytes,
			UsedBytes:      n.UsedBytes,
			Pressured:      n.Pressured,
			ClockSkewMs:    skew.Milliseconds(),
		})
	}
	return out
}

func (b *Builder) storage(ctx context.Context) *statusv1.StorageStatus {
	st := &statusv1.StorageStatus{DataDir: b.dataDir}
	if du, err := b.diskUsage(b.dataDir); err != nil {
		b.logger.Warn("status: could not measure the data directory", "dir", b.dataDir, "err", err)
	} else {
		st.CapacityBytes = du.CapacityBytes
		st.UsedBytes = du.UsedBytes
		st.AvailableBytes = du.AvailableBytes
	}
	if ids, err := b.chunks.List(ctx); err != nil {
		b.logger.Warn("status: could not list local chunks", "err", err)
	} else {
		st.ChunkCount = int64(len(ids))
	}
	return st
}

func (b *Builder) volumeTable() []*statusv1.VolumeStatus {
	if b.volumes == nil {
		return nil
	}
	vols := b.volumes.Volumes()
	attached := b.attachedSet()
	out := make([]*statusv1.VolumeStatus, 0, len(vols))
	for _, v := range vols {
		_, here := attached[v.Path]
		out = append(out, &statusv1.VolumeStatus{
			Path:            v.Path,
			Inode:           v.Inode,
			SizeBytes:       v.Size,
			ExtentSizeBytes: v.ExtentSize,
			LeaseHolder:     v.LeaseHolder,
			ExtentsWritten:  b.extentsWritten(v),
			Replicas:        b.replicaSet(v.Inode),
			MapDamaged:      b.mapDamaged(v.Inode),
			AttachedHere:    here,
		})
	}
	return out
}

// extentsWritten takes the larger of the in-namespace and out-of-band counts
// because a volume's bindings live in exactly one of them depending on whether
// extent replication is on. A node outside a volume's extent-map replica set
// holds neither, so under extent replication it reports zero for that volume;
// asking a replica would turn a cheap local snapshot into a cluster fan-out.
func (b *Builder) extentsWritten(v namespace.VolumeInfo) int64 {
	n := v.InlineExtents
	if b.extents != nil && b.extents.Has(v.Inode) {
		n = max(n, b.extents.Len(v.Inode))
	}
	return int64(n)
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
