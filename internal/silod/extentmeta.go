package silod

import (
	"context"
	"fmt"

	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

// extentVolumeMeta is the gossiped-namespace surface the extent-replication
// adapter needs: the (small, gossiped) per-volume extent size, the stable inode
// id used as the replica-set key, and the single-writer lease for fencing.
// *namespace.Namespace satisfies it.
type extentVolumeMeta interface {
	ExtentSize(path string) (int64, error)
	VolumeInodeID(path string) (string, error)
	Lease(path string) (namespace.Lease, error)
}

// extentCoord is the replica-set extent-map coordinator the adapter drives.
// *replication.ExtentCoordinator satisfies it.
type extentCoord interface {
	Lookup(volumeID string, index uint64) (chunkID string, mapped bool)
	ApplyDelta(ctx context.Context, volumeID string, indexes []uint64, chunkIDs []string, ts hlc.Timestamp) error
	Warm(ctx context.Context, volumeID string) error
}

// extentMetadata satisfies volume.Metadata by serving a volume's extent map from
// its replica set instead of the gossiped namespace. ExtentSize stays a gossiped
// lookup; per-extent reads hit the warmed local store; writes are fenced against
// the gossiped lease and replicated to the replica set with quorum. One instance
// is bound to a single open volume session (one export, one volume id, the
// session ctx).
type extentMetadata struct {
	ctx      context.Context //nolint:containedctx // bound to the mount session, like volume.Volume
	ns       extentVolumeMeta
	coord    extentCoord
	clock    *hlc.Clock
	export   string
	volumeID string
}

// ExtentSize is the per-volume copy-on-write unit, still a small gossiped value.
func (m *extentMetadata) ExtentSize(path string) (int64, error) { return m.ns.ExtentSize(path) }

// Extent looks up the chunk backing an extent from the warmed local replica of
// the map. The session warmed it on Open and keeps it current on every write.
func (m *extentMetadata) Extent(_ string, index uint64) (string, bool, error) {
	id, mapped := m.coord.Lookup(m.volumeID, index)
	return id, mapped, nil
}

// WriteExtent fences against the lease, then replicates the single rebind to the
// volume's replica set.
func (m *extentMetadata) WriteExtent(_ string, index uint64, chunkID, holder string) error {
	if err := m.fence(holder); err != nil {
		return err
	}
	return m.coord.ApplyDelta(m.ctx, m.volumeID, []uint64{index}, []string{chunkID}, m.clock.Now())
}

// WriteExtents fences once, then replicates the whole batch of rebinds.
func (m *extentMetadata) WriteExtents(_ string, indexes []uint64, chunkIDs []string, holder string) error {
	if err := m.fence(holder); err != nil {
		return err
	}
	return m.coord.ApplyDelta(m.ctx, m.volumeID, indexes, chunkIDs, m.clock.Now())
}

// fence rejects a write from anyone but the current lease holder, mirroring the
// namespace's own check so a stolen-from writer is refused. The HLC stamped on
// each replicated extent then makes the map last-writer-wins, so even a write
// that slips through a stale lease view loses to the true holder's.
func (m *extentMetadata) fence(holder string) error {
	lease, err := m.ns.Lease(m.export)
	if err != nil {
		return err
	}
	if lease.Holder != holder {
		return fmt.Errorf("namespace: %q is held by %q, not %q; only the lease holder may write: %w", m.export, lease.Holder, holder, namespace.ErrLeaseHeld)
	}
	return nil
}
