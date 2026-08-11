package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/metrics"
)

// DefaultScrubInterval is how often the scrubber re-checks local chunks for
// under-replication when no interval is configured.
const DefaultScrubInterval = 30 * time.Second

// ChunkCatalog is the scrubber's read view of the local store: enumerate
// the chunks held here and load one to re-push. chunkstore.FileStore
// satisfies it.
type ChunkCatalog interface {
	List(ctx context.Context) ([]string, error)
	Get(ctx context.Context, id string) ([]byte, chunkstore.Info, error)
}

// ReplicaProbe checks whether a peer holds a chunk and pushes it if not.
// *GRPCPeers satisfies it.
type ReplicaProbe interface {
	Stat(ctx context.Context, addr, id string) (chunkstore.Info, error)
	Store(ctx context.Context, addr, id string, data []byte) (chunkstore.Info, error)
}

// Scrubber re-forms missing replicas. On a paced loop it walks the local
// chunks and, for each one this node is the highest-priority holder of,
// pushes a copy to any ring replica that is missing it. Electing the
// highest-priority *holder* — rather than just the ring primary — means
// healing still happens when the primary was down during the original
// write and therefore never received the chunk itself.
//
// Only referenced chunks are healed. An orphan is on its way out — the chunk GC
// reclaims it — so re-replicating one is not just wasted work, it actively
// undoes the GC: a node sweeps an orphan, and the next scrub copies it back from
// a peer that has not swept yet. With a large backlog the two never settle. The
// live set comes from the same two reference sources the GC uses, and carries
// the same completeness caveat, handled in runOnce.
type Scrubber struct {
	place    Placement
	catalog  ChunkCatalog
	ns       NamespaceRefSource
	ext      ExtentRefSource
	probe    ReplicaProbe
	rf       int
	interval time.Duration
	logger   *slog.Logger

	// shortfall is the number of chunks this node was responsible for healing
	// that were under-replicated at the last completed scrub — the operator's
	// "is my data at full replication?" signal. repairs is the cumulative count
	// of replicas this node has re-pushed. skipped is how many local chunks the
	// last scrub passed over as unreferenced, and incomplete is 1 when it could
	// not filter at all because its view of the extent maps was partial. All are
	// read by the metrics scrape.
	shortfall  atomic.Int64
	repairs    atomic.Int64
	skipped    atomic.Int64
	incomplete atomic.Int64

	stop chan struct{}
	done chan struct{}
}

// NewScrubber builds a scrubber. rf < 1 clamps to 1 and a non-positive
// interval falls back to DefaultScrubInterval, so a misconfiguration paces
// conservatively rather than spinning. ns and ext supply the live reference set
// that keeps healing off orphaned chunks.
func NewScrubber(place Placement, catalog ChunkCatalog, ns NamespaceRefSource, ext ExtentRefSource, probe ReplicaProbe, rf int, interval time.Duration, logger *slog.Logger) *Scrubber {
	if rf < 1 {
		rf = 1
	}
	if interval <= 0 {
		interval = DefaultScrubInterval
	}
	return &Scrubber{
		place:    place,
		catalog:  catalog,
		ns:       ns,
		ext:      ext,
		probe:    probe,
		rf:       rf,
		interval: interval,
		logger:   logger,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Name identifies the subsystem in silod's lifecycle logs.
func (s *Scrubber) Name() string { return "scrubber" }

// Start runs the scrub loop until Shutdown, returning nil on a clean stop
// so silod can treat it like every other subsystem.
func (s *Scrubber) Start() error {
	defer close(s.done)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel the in-flight cycle's context the moment Shutdown is called so
	// a slow scrub does not hold up the daemon's exit.
	go func() {
		<-s.stop
		cancel()
	}()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return nil
		case <-ticker.C:
			s.runOnce(ctx)
		}
	}
}

// Shutdown stops the loop, waiting for an in-flight cycle to unwind or the
// context to expire.
func (s *Scrubber) Shutdown(ctx context.Context) error {
	close(s.stop)
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("scrubber did not stop within the shutdown deadline (%w); a scrub cycle may be stuck on an unresponsive peer", ctx.Err())
	}
}

// runOnce walks every local chunk once, healing the referenced ones this node
// owns and passing over the orphans.
func (s *Scrubber) runOnce(ctx context.Context) {
	ids, err := s.catalog.List(ctx)
	if err != nil {
		s.logger.Warn("scrubber could not list local chunks; will retry next cycle", "error", err)
		return
	}

	// On an incomplete view a chunk can look unreferenced here purely because
	// the map that binds it lives on a node whose copy we do not hold. Healing
	// an orphan wastes a copy; skipping a live chunk can cost its last replica,
	// so when in doubt heal everything — the same asymmetry the GC resolves by
	// abstaining.
	keep, unaccounted := liveSet(s.ns, s.ext)
	s.incomplete.Store(0)
	if unaccounted > 0 {
		s.incomplete.Store(1)
		s.logger.Warn("scrubber cannot tell orphaned chunks from live ones: it does not hold the extent maps of every live volume, so it is healing every local chunk this cycle", "unaccounted_volumes", unaccounted)
	}

	self := s.place.SelfID()
	var short, skipped int64
	for _, id := range ids {
		if ctx.Err() != nil {
			return // shutting down mid-cycle; leave the last full count in place
		}
		if unaccounted == 0 {
			if _, live := keep[id]; !live {
				skipped++
				continue // orphaned: the GC owns this one
			}
		}
		if s.scrubChunk(ctx, self, id) {
			short++
		}
	}
	s.shortfall.Store(short)
	s.skipped.Store(skipped)
}

// scrubChunk ensures one locally-held chunk reaches its full replica set,
// but only when this node is the highest-ranked replica that actually holds
// it — so exactly one node pushes, avoiding a re-replication stampede. It
// reports whether the chunk was under-replicated (some target was missing it),
// which the caller tallies into the shortfall gauge.
func (s *Scrubber) scrubChunk(ctx context.Context, self, id string) (underReplicated bool) {
	targets := s.place.Replicas(id, s.rf)
	myRank := indexOf(targets, self)
	if myRank < 0 {
		return false // no longer a replica for this chunk (rebalanced away)
	}
	for _, higher := range targets[:myRank] {
		if s.holds(ctx, higher, id) {
			return false // a higher-priority holder will do the pushing
		}
	}

	var (
		data   []byte
		loaded bool
	)
	for _, t := range targets {
		if t == self || s.holds(ctx, t, id) {
			continue
		}
		// A target that should hold this chunk does not: the chunk is
		// under-replicated, whether or not the push below succeeds.
		underReplicated = true
		if !loaded {
			d, _, err := s.catalog.Get(ctx, id)
			if err != nil {
				s.logger.Warn("scrubber could not read a local chunk to re-replicate it", "chunk", id, "error", err)
				return underReplicated
			}
			data, loaded = d, true
		}
		addr, ok := s.place.DataAddr(t)
		if !ok {
			continue // peer has not advertised a data address yet
		}
		if _, err := s.probe.Store(ctx, addr, id, data); err != nil {
			s.logger.Warn("scrubber could not re-replicate a chunk to a peer; will retry next cycle", "chunk", id, "peer", t, "error", err)
			continue
		}
		s.repairs.Add(1)
		s.logger.Info("scrubber re-replicated a chunk", "chunk", id, "peer", t)
	}
	return underReplicated
}

// holds reports whether node holds id, via a local Stat on that peer. Any
// error — NotFound or an unreachable peer — counts as "does not hold", so a
// missing or down higher-priority replica never blocks healing.
func (s *Scrubber) holds(ctx context.Context, node, id string) bool {
	addr, ok := s.place.DataAddr(node)
	if !ok {
		return false
	}
	_, err := s.probe.Stat(ctx, addr, id)
	return err == nil
}

// MetricPrefix namespaces the scrubber's replication-health readings.
func (s *Scrubber) MetricPrefix() string { return "silo_replication" }

// CollectMetrics reports the under-replication shortfall observed at the last
// completed scrub and the cumulative re-replications this node has performed,
// labelled by node. Summed across the cluster (each chunk has exactly one
// responsible healer), the shortfall is the count of under-replicated chunks.
func (s *Scrubber) CollectMetrics() []metrics.Metric {
	labels := [][2]string{{"node", s.place.SelfID()}}
	return []metrics.Metric{
		{Name: "shortfall_chunks", Help: "Chunks this node is responsible for that were under-replicated at the last scrub.", Kind: metrics.Gauge, Value: float64(s.shortfall.Load()), Labels: labels},
		{Name: "repairs_total", Help: "Replicas this node has re-pushed to heal under-replication.", Kind: metrics.Counter, Value: float64(s.repairs.Load()), Labels: labels},
		{Name: "unreferenced_skipped", Help: "Local chunks the last scrub passed over as unreferenced, leaving them to the chunk GC.", Kind: metrics.Gauge, Value: float64(s.skipped.Load()), Labels: labels},
		{Name: "incomplete_view", Help: "1 when the last scrub could not filter orphans because this node does not hold every live volume's extent map, so it healed everything.", Kind: metrics.Gauge, Value: float64(s.incomplete.Load()), Labels: labels},
	}
}

func indexOf(ids []string, target string) int {
	for i, id := range ids {
		if id == target {
			return i
		}
	}
	return -1
}
