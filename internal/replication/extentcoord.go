package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

// MetaPlacement resolves a volume's extent-map replica set (the un-steered ring
// placement of the volume's inode id) and node dial addresses. *Router
// satisfies it via MetaReplicas/DataAddr/SelfID.
type MetaPlacement interface {
	MetaReplicas(volumeID string, n int) []string
	DataAddr(nodeID string) (addr string, ok bool)
	SelfID() string
}

// ExtentStore is the local extent-map store the coordinator reads and writes.
// internal/extentmap.Store satisfies it.
type ExtentStore interface {
	SetBatch(volumeID string, indexes []uint64, chunkIDs []string, ts hlc.Timestamp) error
	Get(volumeID string, index uint64) (chunkID string, mapped bool)
	Has(volumeID string) bool
	Merge(volumeID string, entries []crdt.MapEntry[uint64, string])
	Ensure(volumeID string)
}

// ExtentPeers replicates extent-map operations to other nodes over the data
// plane. *ExtentGRPCPeers satisfies it. Implementations MUST bound each call
// with their own timeout: the coordinator detaches the request context for
// background fan-out, so a hung peer call would otherwise leak a goroutine.
type ExtentPeers interface {
	Apply(ctx context.Context, addr, volumeID string, entries []crdt.MapEntry[uint64, string], ensure bool) error
	Fetch(ctx context.Context, addr, volumeID string) ([]crdt.MapEntry[uint64, string], error)
	Stat(ctx context.Context, addr, volumeID string) (has bool, count int64, err error)
}

// ExtentCoordinator replicates a volume's extent map to the volume's replica
// set and warms a serving node's map from a holder — the same fan-out/fall-back
// shape the chunk Coordinator uses, keyed by volume inode id instead of chunk
// id. It holds no mutable state, so one instance is safe to share.
type ExtentCoordinator struct {
	place  MetaPlacement
	local  ExtentStore
	peers  ExtentPeers
	rf     int
	logger *slog.Logger
}

// NewExtentCoordinator builds a coordinator targeting replication factor rf
// (clamped to >= 1 so a misconfiguration degrades to single-copy).
func NewExtentCoordinator(place MetaPlacement, local ExtentStore, peers ExtentPeers, rf int, logger *slog.Logger) *ExtentCoordinator {
	if rf < 1 {
		rf = 1
	}
	return &ExtentCoordinator{place: place, local: local, peers: peers, rf: rf, logger: logger}
}

// ApplyDelta records that the given extents of volume now bind to the given
// chunk ids as of ts, and replicates the change to the volume's replica set,
// returning once a majority are durable. The local store is always updated
// first so the writing (serving) node reads its own writes, whether or not it
// is itself a replica. indexes and chunkIDs are positionally paired; an empty
// batch is a no-op.
func (c *ExtentCoordinator) ApplyDelta(ctx context.Context, volumeID string, indexes []uint64, chunkIDs []string, ts hlc.Timestamp) error {
	if len(indexes) != len(chunkIDs) {
		return fmt.Errorf("replication: ApplyDelta needs paired slices, got %d indexes and %d chunk ids", len(indexes), len(chunkIDs))
	}
	if len(indexes) == 0 {
		return nil
	}
	replicas := c.place.MetaReplicas(volumeID, c.rf)
	if len(replicas) == 0 {
		return c.errNoReplicas(volumeID)
	}
	if err := c.local.SetBatch(volumeID, indexes, chunkIDs, ts); err != nil {
		return err
	}
	initial := 0
	if containsNode(replicas, c.place.SelfID()) {
		initial = 1
	}
	entries := mapEntries(indexes, chunkIDs, ts)
	return c.quorumFanOut(ctx, volumeID, replicas, initial, func(ctx context.Context, addr string) error {
		return c.peers.Apply(ctx, addr, volumeID, entries, false)
	})
}

// EnsureMap establishes an empty extent map for volume on its replica set, so a
// freshly-created volume can be warmed by a later serving node even before its
// first write. Idempotent.
func (c *ExtentCoordinator) EnsureMap(ctx context.Context, volumeID string) error {
	replicas := c.place.MetaReplicas(volumeID, c.rf)
	if len(replicas) == 0 {
		return c.errNoReplicas(volumeID)
	}
	initial := 0
	if containsNode(replicas, c.place.SelfID()) {
		c.local.Ensure(volumeID)
		initial = 1
	}
	return c.quorumFanOut(ctx, volumeID, replicas, initial, func(ctx context.Context, addr string) error {
		return c.peers.Apply(ctx, addr, volumeID, nil, true)
	})
}

// Lookup returns the chunk id backing the extent at index of volume from the
// local store and whether it is mapped. The serving node warms its local map on
// Open (Warm) and keeps it current via ApplyDelta, so this stays an on-box read.
func (c *ExtentCoordinator) Lookup(volumeID string, index uint64) (string, bool) {
	return c.local.Get(volumeID, index)
}

// Warm makes sure the local store holds volume's extent map, fetching it from a
// replica that has it when this node does not. It is the serve-path bootstrap: a
// node that did not create the volume (or just joined its replica set) calls it
// before serving so per-extent Lookups are correct. A volume whose map exists
// only as an empty map still warms (to an empty local map that reads as zeros).
func (c *ExtentCoordinator) Warm(ctx context.Context, volumeID string) error {
	if c.local.Has(volumeID) {
		return nil
	}
	replicas := c.place.MetaReplicas(volumeID, c.rf)
	if len(replicas) == 0 {
		return c.errNoReplicas(volumeID)
	}
	self := c.place.SelfID()
	var errs []error
	for _, target := range replicas {
		if target == self {
			continue // local.Has was already false; cannot fetch from ourselves
		}
		addr, ok := c.place.DataAddr(target)
		if !ok {
			errs = append(errs, fmt.Errorf("node %q has no advertised data address", target))
			continue
		}
		has, _, err := c.peers.Stat(ctx, addr, volumeID)
		if err != nil {
			errs = append(errs, fmt.Errorf("peer %s: %w", target, err))
			continue
		}
		if !has {
			continue
		}
		entries, err := c.peers.Fetch(ctx, addr, volumeID)
		if err != nil {
			errs = append(errs, fmt.Errorf("peer %s: %w", target, err))
			continue
		}
		c.local.Merge(volumeID, entries)
		return nil
	}
	return fmt.Errorf("replication: could not warm the extent map of volume %q from any of its %d replicas (it may be unprovisioned or those nodes are unreachable): %w", volumeID, len(replicas), errors.Join(errs...))
}

// quorumFanOut applies the per-peer operation to every replica that is not this
// node, concurrently, and returns once initial+peer acks reach a majority of
// the replica set. Stragglers drain in the background so the caller neither
// blocks nor leaks goroutines. It fails only if the quorum is not reached.
func (c *ExtentCoordinator) quorumFanOut(ctx context.Context, volumeID string, replicas []string, initial int, apply func(ctx context.Context, addr string) error) error {
	self := c.place.SelfID()
	quorum := len(replicas)/2 + 1

	var peerTargets []string
	for _, t := range replicas {
		if t != self {
			peerTargets = append(peerTargets, t)
		}
	}

	fanCtx := context.WithoutCancel(ctx)
	results := make(chan error, len(peerTargets))
	for _, t := range peerTargets {
		go func(target string) {
			addr, ok := c.place.DataAddr(target)
			if !ok {
				results <- fmt.Errorf("replication: node %q has not advertised a data address; cannot replicate the extent map of volume %q to it", target, volumeID)
				return
			}
			results <- apply(fanCtx, addr)
		}(t)
	}

	acks := initial
	collected := 0
	var errs []error
	for acks < quorum && collected < len(peerTargets) {
		if err := <-results; err != nil {
			errs = append(errs, err)
		} else {
			acks++
		}
		collected++
	}
	if acks >= quorum {
		go c.drain(results, len(peerTargets)-collected, volumeID)
		return nil
	}
	return fmt.Errorf("replication: extent map of volume %q reached only %d of %d replicas (quorum %d); check peer reachability: %w", volumeID, acks, len(replicas), quorum, errors.Join(errs...))
}

// drain consumes the remaining count fan-out results after quorum, logging any
// shortfall so the extent scrubber's later repair is explainable.
func (c *ExtentCoordinator) drain(results <-chan error, count int, volumeID string) {
	for i := 0; i < count; i++ {
		if err := <-results; err != nil {
			c.logger.Warn("extent-map replica update fell short of full replication; the scrubber will repair it", "volume", volumeID, "error", err)
		}
	}
}

func (c *ExtentCoordinator) errNoReplicas(volumeID string) error {
	return fmt.Errorf("replication: no live node can hold the extent map of volume %q; the cluster view is empty — check gossip connectivity", volumeID)
}

// containsNode reports whether id is in ids.
func containsNode(ids []string, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// mapEntries pairs indexes and chunkIDs into CRDT map entries stamped with ts.
func mapEntries(indexes []uint64, chunkIDs []string, ts hlc.Timestamp) []crdt.MapEntry[uint64, string] {
	out := make([]crdt.MapEntry[uint64, string], len(indexes))
	for i := range indexes {
		out[i] = crdt.MapEntry[uint64, string]{Key: indexes[i], Value: chunkIDs[i], TS: ts}
	}
	return out
}
