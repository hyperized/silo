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
// internal/extentmap.Store satisfies it. SetBatch, Merge and Ensure apply in
// memory even when they return an error; the error means the change did not
// reach disk, so this node's copy must not count as durable.
type ExtentStore interface {
	SetBatch(volumeID string, indexes []uint64, chunkIDs []string, ts hlc.Timestamp) error
	Get(volumeID string, index uint64) (chunkID string, mapped bool)
	Has(volumeID string) bool
	// Snapshot returns volume's bindings (each carrying its own HLC), the payload
	// SnapshotMap clones onto a new snapshot volume's replica set.
	Snapshot(volumeID string) []crdt.MapEntry[uint64, string]
	Merge(volumeID string, entries []crdt.MapEntry[uint64, string]) error
	Ensure(volumeID string) error
	Delete(volumeID string) error
	// Lost reports that this node's persisted map of volume was damaged and has
	// not been rebuilt from every replica yet; ClearLost records that it has.
	Lost(volumeID string) bool
	ClearLost(volumeID string)
}

// ExtentPeers replicates extent-map operations to other nodes over the data
// plane. *ExtentGRPCPeers satisfies it. Implementations MUST bound each call
// with their own timeout: the coordinator detaches the request context for
// background fan-out, so a hung peer call would otherwise leak a goroutine.
type ExtentPeers interface {
	Apply(ctx context.Context, addr, volumeID string, entries []crdt.MapEntry[uint64, string], ensure bool) error
	Fetch(ctx context.Context, addr, volumeID string) ([]crdt.MapEntry[uint64, string], error)
	Stat(ctx context.Context, addr, volumeID string) (has bool, count int64, digest []byte, err error)
	Delete(ctx context.Context, addr, volumeID string) error
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
// is itself a replica. A local copy that could not be saved to disk still
// serves reads but does not count toward the majority. indexes and chunkIDs
// are positionally paired; an empty batch is a no-op.
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
	initial, localErr := c.localAck(volumeID, replicas, c.local.SetBatch(volumeID, indexes, chunkIDs, ts))
	entries := mapEntries(indexes, chunkIDs, ts)
	return c.quorumFanOut(ctx, volumeID, replicas, initial, localErr, func(ctx context.Context, addr string) error {
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
	initial, localErr := 0, error(nil)
	if containsNode(replicas, c.place.SelfID()) {
		initial, localErr = c.localAck(volumeID, replicas, c.local.Ensure(volumeID))
	}
	return c.quorumFanOut(ctx, volumeID, replicas, initial, localErr, func(ctx context.Context, addr string) error {
		return c.peers.Apply(ctx, addr, volumeID, nil, true)
	})
}

// SnapshotMap clones the source volume's extent map onto the destination
// (snapshot) volume's replica set, returning once a majority are durable. It is
// the out-of-band counterpart to namespace.SnapshotVolume's in-namespace extents
// clone: under extent replication a volume's bindings live here, not in the
// gossiped namespace, so without this a snapshot would capture an empty map and
// read as zeros. The source map is warmed locally first, so a node that does not
// hold the source can still snapshot it. The clone is a faithful point-in-time
// copy — each binding keeps its source HLC — and because chunks are immutable the
// two volumes share them safely until a copy-on-write write diverges one. The
// destination is established even when the source map is empty (a never-written
// source snapshots to a valid empty volume), via ensure=true on the fan-out.
func (c *ExtentCoordinator) SnapshotMap(ctx context.Context, srcVolumeID, dstVolumeID string) error {
	if err := c.Warm(ctx, srcVolumeID); err != nil {
		return fmt.Errorf("replication: cannot snapshot volume %q: could not warm its source extent map (%w)", srcVolumeID, err)
	}
	entries := c.local.Snapshot(srcVolumeID)

	replicas := c.place.MetaReplicas(dstVolumeID, c.rf)
	if len(replicas) == 0 {
		return c.errNoReplicas(dstVolumeID)
	}
	initial, localErr := 0, error(nil)
	if containsNode(replicas, c.place.SelfID()) {
		// Ensure first so the dst map exists even when the source was empty.
		initial, localErr = c.localAck(dstVolumeID, replicas, errors.Join(c.local.Ensure(dstVolumeID), c.local.Merge(dstVolumeID, entries)))
	}
	return c.quorumFanOut(ctx, dstVolumeID, replicas, initial, localErr, func(ctx context.Context, addr string) error {
		return c.peers.Apply(ctx, addr, dstVolumeID, entries, true)
	})
}

// Lookup returns the chunk id backing the extent at index of volume from the
// local store and whether it is mapped. The serving node warms its local map on
// Open (Warm) and keeps it current via ApplyDelta, so this stays an on-box read.
func (c *ExtentCoordinator) Lookup(volumeID string, index uint64) (string, bool) {
	return c.local.Get(volumeID, index)
}

// Warm makes sure the local store holds a current copy of volume's extent map
// before this node serves it. It is the serve-path bootstrap: a node that did
// not create the volume (or just joined its replica set) calls it before
// serving so per-extent Lookups are correct.
//
// A map this node already holds is trusted as is. Otherwise every reachable
// replica's copy is merged in; the map is last-writer-wins per extent, so the
// newest binding of each extent survives whichever replica it came from. A
// volume no replica holds while every replica answered has never been written,
// and warms to an empty map that reads as zeros.
//
// A map whose local copy was damaged (Lost) is held to a stricter rule, since
// this node may have accepted writes no reachable peer has: Warm refuses unless
// every replica answered and at least one held a copy. Serving after a partial
// rebuild could hand back an older version of the volume.
func (c *ExtentCoordinator) Warm(ctx context.Context, volumeID string) error {
	// Lost is checked on its own because a peer may already have pushed its
	// copy into a damaged volume, which makes Has true without making it current.
	lost := c.local.Lost(volumeID)
	if c.local.Has(volumeID) && !lost {
		return nil
	}
	replicas := c.place.MetaReplicas(volumeID, c.rf)
	if len(replicas) == 0 {
		return c.errNoReplicas(volumeID)
	}
	res := c.mergeReplicas(ctx, volumeID, replicas)
	if lost {
		return c.finishLostWarm(volumeID, len(replicas), res)
	}
	if res.merged > 0 {
		return nil
	}
	if res.unreachable == 0 {
		// Every replica answered and none holds a map for this volume: it has
		// never been written, so an empty (all-zero) volume is the correct view.
		// Safe only because no replica was unreachable; otherwise a holder might
		// exist and serving zeros would mask real data, which the error below
		// prevents.
		if err := c.local.Ensure(volumeID); err != nil {
			c.logger.Warn("could not save the empty extent map of a never-written volume; it serves from memory and is rebuilt from the replicas after a restart. Check free space and permissions on the data directory", "volume", volumeID, "error", err)
		}
		return nil
	}
	return fmt.Errorf("replication: could not warm the extent map of volume %q; %d of its %d replicas were unreachable, so serving it now could mask real data: %w", volumeID, res.unreachable, len(replicas), errors.Join(res.errs...))
}

// warmResult tallies one pass over a volume's replicas.
type warmResult struct {
	merged      int // replicas whose copy was merged in
	unreachable int // replicas that could not be asked or could not answer
	errs        []error
}

// mergeReplicas folds every reachable replica's copy of volume into the local
// store. A copy that merged in memory but could not be saved still counts as
// merged: this node serves from memory, and the replicas still hold it.
func (c *ExtentCoordinator) mergeReplicas(ctx context.Context, volumeID string, replicas []string) warmResult {
	self := c.place.SelfID()
	var res warmResult
	for _, target := range replicas {
		if target == self {
			continue
		}
		entries, has, err := c.fetchReplica(ctx, target, volumeID)
		if err != nil {
			res.errs = append(res.errs, err)
			res.unreachable++
			continue
		}
		if !has {
			continue
		}
		if err := c.local.Merge(volumeID, entries); err != nil {
			c.logger.Warn("merged a replica's extent map in memory but could not save it; check free space and permissions on the data directory", "volume", volumeID, "peer", target, "error", err)
		}
		res.merged++
	}
	return res
}

// fetchReplica asks one replica whether it holds volume's map and fetches it
// when it does.
func (c *ExtentCoordinator) fetchReplica(ctx context.Context, target, volumeID string) ([]crdt.MapEntry[uint64, string], bool, error) {
	addr, ok := c.place.DataAddr(target)
	if !ok {
		return nil, false, fmt.Errorf("node %q has no advertised data address", target)
	}
	has, _, _, err := c.peers.Stat(ctx, addr, volumeID)
	if err != nil {
		return nil, false, fmt.Errorf("peer %s: %w", target, err)
	}
	if !has {
		return nil, false, nil
	}
	entries, err := c.peers.Fetch(ctx, addr, volumeID)
	if err != nil {
		return nil, false, fmt.Errorf("peer %s: %w", target, err)
	}
	return entries, true, nil
}

// finishLostWarm decides whether a volume whose local map was damaged may be
// served after the merge pass in res.
func (c *ExtentCoordinator) finishLostWarm(volumeID string, replicaCount int, res warmResult) error {
	if res.unreachable > 0 {
		return fmt.Errorf("replication: will not serve volume %q yet: this node's copy of its extent map was damaged at the last unclean shutdown, and %d of its %d replicas could not be reached, so the newest copy may be on one of them and serving now could roll the volume back to an older state. Bring the unreachable nodes back or check gossip connectivity between the nodes, then retry the attach. The damaged copy is kept in this node's extents data directory with a .corrupt suffix: %w",
			volumeID, res.unreachable, replicaCount, errors.Join(res.errs...))
	}
	if res.merged == 0 {
		return fmt.Errorf("replication: will not serve volume %q: this node's copy of its extent map was damaged at the last unclean shutdown and no other replica holds a copy, so there is nothing to rebuild it from. Restore the volume from a backup; if it never held data, delete its .corrupt file in this node's extents data directory and restart the node", volumeID)
	}
	c.local.ClearLost(volumeID)
	c.logger.Info("rebuilt a damaged extent map from its replicas; the volume serves again and its .corrupt file can be deleted", "volume", volumeID, "replicas", res.merged)
	return nil
}

// DeleteMap removes volume's extent map from its replica set — the synchronous
// half of the delete path. It deletes the local copy unconditionally (this node
// may hold a warmed serving copy even when it is not a placement replica) and
// fans the delete out to the replica set concurrently, joining any failures so
// the caller can log them. It is best-effort by design: the reaper reclaims any
// copy this misses (an unreachable replica, or a serving node that warmed the
// map but is not a replica). A single down replica can delay the call up to the
// peer client's per-call timeout.
func (c *ExtentCoordinator) DeleteMap(ctx context.Context, volumeID string) error {
	var errs []error
	if err := c.local.Delete(volumeID); err != nil {
		errs = append(errs, fmt.Errorf("replication: could not delete the local extent map of volume %q (%w)", volumeID, err))
	}

	self := c.place.SelfID()
	var peerTargets []string
	for _, t := range c.place.MetaReplicas(volumeID, c.rf) {
		if t != self {
			peerTargets = append(peerTargets, t)
		}
	}
	if len(peerTargets) == 0 {
		return errors.Join(errs...)
	}

	fanCtx := context.WithoutCancel(ctx)
	results := make(chan error, len(peerTargets))
	for _, t := range peerTargets {
		go func(target string) {
			addr, ok := c.place.DataAddr(target)
			if !ok {
				results <- fmt.Errorf("replication: node %q has not advertised a data address; cannot delete the extent map of volume %q from it", target, volumeID)
				return
			}
			results <- c.peers.Delete(fanCtx, addr, volumeID)
		}(t)
	}
	for range peerTargets {
		if err := <-results; err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// quorumFanOut applies the per-peer operation to every replica that is not this
// node, concurrently, and returns once initial+peer acks reach a majority of
// the replica set. Stragglers drain in the background so the caller neither
// blocks nor leaks goroutines. It fails only if the quorum is not reached;
// localErr, when set, explains why this node did not count itself and is
// included in that failure.
func (c *ExtentCoordinator) quorumFanOut(ctx context.Context, volumeID string, replicas []string, initial int, localErr error, apply func(ctx context.Context, addr string) error) error {
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
	if localErr != nil {
		errs = append(errs, fmt.Errorf("this node: %w", localErr))
	}
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

// localAck turns the outcome of this node's own store update into its
// contribution to the write quorum: one ack when this node is a replica and
// the update reached disk, none otherwise. A failed save is logged here
// because, when the peers alone reach quorum, nothing else would surface it.
func (c *ExtentCoordinator) localAck(volumeID string, replicas []string, persistErr error) (int, error) {
	if persistErr != nil {
		c.logger.Warn("this node could not save its copy of a volume's extent map, so it does not count toward the write quorum; check free space and permissions on its data directory",
			"volume", volumeID, "error", persistErr)
		return 0, persistErr
	}
	if containsNode(replicas, c.place.SelfID()) {
		return 1, nil
	}
	return 0, nil
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
