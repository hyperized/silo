package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/metrics"
)

// DefaultChunkGCInterval is how often the chunk GC sweeps when none is
// configured. Chunks are bulk data and orphans are not urgent (they waste space,
// they do not corrupt), so the default is unhurried — a sweep walks every local
// chunk and is pure overhead on a clean store.
const DefaultChunkGCInterval = 10 * time.Minute

// DefaultChunkGCGrace is how old a chunk must be before the GC will reclaim it
// when none is configured. It is the safety margin for the write-then-record
// gap: a client stores a chunk and only then records the reference (in a file
// manifest or an extent map), so a just-written chunk is briefly unreferenced on
// disk. The grace must comfortably exceed that window plus gossip/HLC skew, or
// the GC could reclaim a chunk whose reference has not yet landed. It mirrors the
// extent reaper's reap-after guard.
const DefaultChunkGCGrace = time.Hour

// DefaultChunkGCMaxDeletes caps a single sweep's reclamations when none is
// configured. It only binds on a backlog: steady-state overwrite churn produces
// orders of magnitude fewer orphans per interval than this, so a healthy store
// never reaches the cap, while a store that leaked for weeks drains over a
// series of sweeps rather than one long burst of unlinks.
const DefaultChunkGCMaxDeletes = 100_000

// ChunkLister is the node-local chunk store the GC enumerates and prunes.
// *chunkstore.FileStore satisfies it via List/Stat/Delete.
type ChunkLister interface {
	List(ctx context.Context) ([]string, error)
	Stat(ctx context.Context, id string) (chunkstore.Info, error)
	Delete(ctx context.Context, id string) error
}

// BatchDeleter is the optional bulk-reclamation path: unlink without a
// directory fsync each time, then flush once for the whole batch.
// *chunkstore.FileStore satisfies it. A store that does not is still correct —
// the GC falls back to Delete, it just pays a journal commit per chunk.
type BatchDeleter interface {
	DeleteNoSync(ctx context.Context, id string) error
	SyncDir() error
}

// NamespaceRefSource yields the global half of the live (keep) set: the chunk
// ids the namespace references (file manifests + in-namespace volume extents)
// and the set of live volume inode ids. *namespace.Namespace satisfies it via
// LiveChunkRefs. The namespace is fully replicated, so this half is complete on
// every node.
type NamespaceRefSource interface {
	LiveChunkRefs() (chunks map[string]struct{}, volumes map[string]struct{})
}

// ExtentRefSource yields the chunk ids in the out-of-band extent maps this node
// holds and the volume ids it holds maps for. *extentmap.Store satisfies it via
// Volumes/Snapshot. This half is sharded: a node holds only the maps of the
// volumes it is a metadata replica of (or has warmed), which is why the GC needs
// the namespace's live volume set to know when its view is complete.
type ExtentRefSource interface {
	Volumes() []string
	Snapshot(volumeID string) []crdt.MapEntry[uint64, string]
}

// ExtentAgreement reports whether this node's extent maps match the other
// replicas'. The GC needs it because holding a map is not the same as holding a
// current one: the write path acknowledges at a quorum, so a replica can miss
// deltas and keep serving a copy that looks perfectly healthy. A keep set built
// from a map like that omits chunks the cluster still references, and sweeping
// on it deletes live data.
//
// Divergence is a transient state the extent scrubber repairs, so the GC waits
// rather than treating it as an error. *ExtentScrubber satisfies this.
type ExtentAgreement interface {
	// MapsConverged reports whether every volume's map agrees across its
	// replicas, and names one that does not when the answer is no.
	MapsConverged(ctx context.Context, volumes map[string]struct{}) (converged bool, disagreeing string)
}

// ChunkGC reclaims orphaned chunks — chunks no live volume or file references
// anymore — by mark-and-sweep. On a paced loop it builds the cluster's live
// (keep) set from the two reference kinds (file manifests + in-namespace extents
// from the global namespace; out-of-band extent maps held locally), then deletes
// the local chunks that set excludes.
//
// Three guards keep a sweep from ever reclaiming a live chunk:
//
//   - Completeness. A chunk is referenced from extent maps that are sharded
//     across nodes; a node holds only some of them. So before sweeping, the GC
//     checks it holds the extent map of every live volume (the namespace lists
//     them). If any live volume's map is missing here, the node has references it
//     cannot see, so it abstains entirely rather than risk deleting a chunk some
//     unseen map still binds. (On a cluster where the replication factor covers
//     every node — the common small-cluster case — each node holds every map, so
//     the view is always complete. A future revision can fetch missing maps from
//     peers instead of abstaining.)
//
//   - Currency. Holding a map is not the same as holding the current one. The
//     extent write path acknowledges at a quorum, so a replica can miss deltas and
//     serve a copy that looks healthy while lacking bindings its peers have. Before
//     sweeping, the GC checks its maps agree with the other replicas' and waits for
//     the extent scrubber to reconcile them if they do not.
//
//   - Grace. A chunk newer than the grace window is never reclaimed, covering the
//     write-then-record gap where a freshly stored chunk's reference has not yet
//     been recorded.
//
// Reclamation is also gated on an explicit enable flag: until it is set the GC
// runs as a dry run, reporting how many chunks it would reclaim (the
// orphan_chunks gauge) so an operator can confirm the live-set computation
// against real data before any deletion. Each node sweeps only its own store;
// because every node derives the same global live set, a chunk live anywhere is
// kept everywhere, so replication is preserved.
type ChunkGC struct {
	lister     ChunkLister
	ns         NamespaceRefSource
	ext        ExtentRefSource
	nodeID     string
	grace      time.Duration
	interval   time.Duration
	enabled    bool
	maxDeletes int
	agreement  ExtentAgreement
	logger     *slog.Logger
	now        func() time.Time

	// All read by the metrics scrape. orphans is the reclaimable orphan count at
	// the last completed sweep; reclaimed is cumulative deletions; lastReclaim is
	// the last sweep's deletions; incomplete is 1 when the last cycle abstained
	// for an incomplete view; unaccounted is how many live volumes' maps were
	// missing at the last cycle; deferred is how many reclaimable orphans the
	// last sweep left for the next one after spending its delete budget;
	// diverged is 1 when the last cycle abstained because this node's extent
	// maps disagreed with its peers'.
	orphans     atomic.Int64
	reclaimed   atomic.Int64
	lastReclaim atomic.Int64
	incomplete  atomic.Int64
	unaccounted atomic.Int64
	deferred    atomic.Int64
	diverged    atomic.Int64

	stop chan struct{}
	done chan struct{}
}

// ChunkGCOption configures optional GC behaviour at construction.
type ChunkGCOption func(*ChunkGC)

// WithMaxDeletes caps how many chunks one sweep may reclaim. A backlog larger
// than the cap drains across consecutive sweeps instead of in a single
// unbounded burst of unlinks, which keeps a first sweep after a long leak from
// monopolising the disk the serving path is using. n <= 0 means no cap.
func WithMaxDeletes(n int) ChunkGCOption {
	return func(g *ChunkGC) { g.maxDeletes = n }
}

// WithExtentAgreement gives the GC a way to check that this node's extent maps
// match the other replicas' before it sweeps. silod always supplies one. A GC
// built without it keeps the older behaviour of trusting the local maps, which
// is safe only where those maps cannot lag — in tests, or on a single node.
func WithExtentAgreement(a ExtentAgreement) ChunkGCOption {
	return func(g *ChunkGC) { g.agreement = a }
}

// NewChunkGC builds a chunk GC. A non-positive grace or interval falls back to
// the package default, so a misconfiguration paces conservatively rather than
// sweeping too eagerly or spinning. enabled=false (the default) makes it a dry
// run that reports but never deletes.
func NewChunkGC(lister ChunkLister, ns NamespaceRefSource, ext ExtentRefSource, nodeID string, grace, interval time.Duration, enabled bool, logger *slog.Logger, opts ...ChunkGCOption) *ChunkGC {
	if grace <= 0 {
		grace = DefaultChunkGCGrace
	}
	if interval <= 0 {
		interval = DefaultChunkGCInterval
	}
	g := &ChunkGC{
		lister:     lister,
		ns:         ns,
		ext:        ext,
		nodeID:     nodeID,
		grace:      grace,
		interval:   interval,
		enabled:    enabled,
		maxDeletes: DefaultChunkGCMaxDeletes,
		logger:     logger,
		now:        time.Now,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Name identifies the subsystem in silod's lifecycle logs.
func (g *ChunkGC) Name() string { return "chunk-gc" }

// Start runs the sweep loop until Shutdown, returning nil on a clean stop so
// silod can treat it like every other subsystem.
func (g *ChunkGC) Start() error {
	defer close(g.done)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel the in-flight sweep's context the moment Shutdown is called so a slow
	// sweep does not hold up the daemon's exit.
	go func() {
		<-g.stop
		cancel()
	}()

	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		select {
		case <-g.stop:
			return nil
		case <-ticker.C:
			g.runOnce(ctx)
		}
	}
}

// Shutdown stops the loop, waiting for an in-flight sweep to unwind or the
// context to expire.
func (g *ChunkGC) Shutdown(ctx context.Context) error {
	close(g.stop)
	select {
	case <-g.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("chunk-gc did not stop within the shutdown deadline (%w); a sweep may be stuck on a slow data dir", ctx.Err())
	}
}

// runOnce builds the live set, verifies the view is complete, then sweeps the
// local store of unreferenced chunks older than the grace window.
func (g *ChunkGC) runOnce(ctx context.Context) {
	// Completeness: a live volume whose extent map this node does not hold is a
	// blind spot — its chunks would look unreferenced here. Abstain rather than
	// risk reclaiming a chunk an unseen map still binds.
	keep, missing := liveSet(g.ns, g.ext)
	g.unaccounted.Store(missing)
	if missing > 0 {
		g.incomplete.Store(1)
		g.orphans.Store(0)
		g.lastReclaim.Store(0)
		g.logger.Warn("chunk-gc is skipping this sweep: it does not hold the extent maps of every live volume, so some chunk references are not visible here", "unaccounted_volumes", missing)
		return
	}
	g.incomplete.Store(0)

	// Currency: holding a map is not the same as holding the current one. The
	// write path acknowledges at a quorum, so a replica can miss deltas and go on
	// serving a copy that looks healthy. Sweeping on a keep set built from a map
	// like that deletes chunks the other replicas still reference, so wait for
	// the extent scrubber to reconcile them instead.
	if g.agreement != nil {
		converged, disagreeing := g.agreement.MapsConverged(ctx, liveVolumes(g.ns))
		if !converged {
			g.diverged.Store(1)
			g.orphans.Store(0)
			g.lastReclaim.Store(0)
			g.logger.Warn("chunk-gc is skipping this sweep: this node's extent maps disagree with its replicas, so its view of what is still referenced may be short", "volume", disagreeing)
			return
		}
	}
	g.diverged.Store(0)

	ids, err := g.lister.List(ctx)
	if err != nil {
		g.logger.Warn("chunk-gc could not list the local chunk store; will retry next cycle", "error", err)
		return
	}
	// Prefer the bulk path when the store offers one: unlink without syncing and
	// commit the whole sweep with a single directory fsync at the end. A store
	// that does not implement it still works, one journal commit per chunk.
	batch, batched := g.lister.(BatchDeleter)
	remove := g.lister.Delete
	if batched {
		remove = batch.DeleteNoSync
	}

	cutoff := g.now().Add(-g.grace)
	var orphans, reclaimed, deferred int64
	flush := func() {
		if !batched || reclaimed == 0 {
			return
		}
		if err := batch.SyncDir(); err != nil {
			// The chunks are gone either way; only their durability across a crash
			// is in question, and a resurrected orphan is reclaimed again next sweep.
			g.logger.Warn("chunk-gc could not flush the data directory after reclaiming; the removals are not durable until the next sync", "error", err)
		}
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			flush()
			return // shutting down mid-sweep; leave the last full counts in place
		}
		if _, live := keep[id]; live {
			continue
		}
		info, err := g.lister.Stat(ctx, id)
		if err != nil {
			continue // raced with a delete, or unreadable: skip and revisit next cycle
		}
		if !info.CreatedAt.Before(cutoff) {
			continue // within the grace window: a just-written chunk's reference may not have landed
		}
		orphans++
		if !g.enabled {
			continue
		}
		if g.maxDeletes > 0 && reclaimed >= int64(g.maxDeletes) {
			deferred++ // budget spent; keep counting so the orphan gauge stays honest
			continue
		}
		if err := remove(ctx, id); err != nil {
			g.logger.Warn("chunk-gc could not delete an orphaned chunk; will retry next cycle", "chunk", id, "error", err)
			continue
		}
		reclaimed++
	}
	flush()
	g.orphans.Store(orphans)
	g.lastReclaim.Store(reclaimed)
	g.deferred.Store(deferred)
	switch {
	case reclaimed > 0:
		g.reclaimed.Add(reclaimed)
		if deferred > 0 {
			g.logger.Info("chunk-gc reclaimed orphaned chunks and hit its per-sweep budget; the rest drain next sweep", "count", reclaimed, "deferred", deferred)
		} else {
			g.logger.Info("chunk-gc reclaimed orphaned chunks", "count", reclaimed)
		}
	case orphans > 0 && !g.enabled:
		g.logger.Info("chunk-gc (dry run) found reclaimable orphaned chunks; set SILO_CHUNK_GC_ENABLE to reclaim them", "orphans", orphans)
	}
}

// MetricPrefix namespaces the chunk GC's readings.
func (g *ChunkGC) MetricPrefix() string { return "silo_chunkgc" }

// CollectMetrics reports the sweep's state, labelled by node. orphan_chunks is
// the reclaimable orphan count at the last complete sweep (what a dry run would
// delete); reclaimed_total and last_reclaimed are cumulative and last-sweep
// deletions; incomplete_view is 1 when the node abstained for an incomplete
// view; unaccounted_volumes is how many live volumes' maps it could not see.
func (g *ChunkGC) CollectMetrics() []metrics.Metric {
	labels := [][2]string{{"node", g.nodeID}}
	return []metrics.Metric{
		{Name: "orphan_chunks", Help: "Unreferenced chunks past the grace window at the last complete sweep (what reclamation would delete).", Kind: metrics.Gauge, Value: float64(g.orphans.Load()), Labels: labels},
		{Name: "reclaimed_total", Help: "Orphaned chunks this node has reclaimed.", Kind: metrics.Counter, Value: float64(g.reclaimed.Load()), Labels: labels},
		{Name: "last_reclaimed", Help: "Orphaned chunks reclaimed in the last sweep.", Kind: metrics.Gauge, Value: float64(g.lastReclaim.Load()), Labels: labels},
		{Name: "incomplete_view", Help: "1 when the last cycle abstained because this node does not hold every live volume's extent map.", Kind: metrics.Gauge, Value: float64(g.incomplete.Load()), Labels: labels},
		{Name: "unaccounted_volumes", Help: "Live volumes whose extent maps this node could not see at the last cycle.", Kind: metrics.Gauge, Value: float64(g.unaccounted.Load()), Labels: labels},
		{Name: "deferred_chunks", Help: "Reclaimable orphans the last sweep left behind after spending its per-sweep delete budget.", Kind: metrics.Gauge, Value: float64(g.deferred.Load()), Labels: labels},
		{Name: "diverged_maps", Help: "1 when the last cycle abstained because this node's extent maps disagreed with its replicas'.", Kind: metrics.Gauge, Value: float64(g.diverged.Load()), Labels: labels},
	}
}
