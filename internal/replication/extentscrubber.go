package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/metrics"
)

// DefaultExtentScrubInterval is how often the extent scrubber re-checks the
// extent maps held on this node for under-replication when no interval is
// configured. It is gentler than the chunk scrubber's default: a volume's extent
// map is a small, long-lived metadata object that changes far less than bulk
// chunk data, so a slower cadence still heals a node loss promptly without
// needless cross-node Stat traffic on an idle cluster.
const DefaultExtentScrubInterval = time.Minute

// ExtentCatalog is the scrubber's read view of the local extent-map store:
// enumerate the volumes whose maps live here and snapshot one to re-push.
// *extentmap.Store satisfies it via Volumes and Snapshot.
type ExtentCatalog interface {
	Volumes() []string
	Snapshot(volumeID string) []crdt.MapEntry[uint64, string]
}

// ExtentReplicaProbe checks whether a peer holds a volume's extent map and
// pushes the map if not. *ExtentGRPCPeers satisfies it. The push uses Apply with
// ensure=true so even an empty map (a created-but-never-written volume) is
// established on the peer, matching the write path's EnsureMap semantics.
type ExtentReplicaProbe interface {
	Stat(ctx context.Context, addr, volumeID string) (has bool, count int64, err error)
	Apply(ctx context.Context, addr, volumeID string, entries []crdt.MapEntry[uint64, string], ensure bool) error
}

// ExtentScrubber re-forms missing extent-map replicas — the metadata analog of
// the chunk Scrubber. On a paced loop it walks the extent maps this node holds
// and, for each one this node is the highest-ranked holder of within the
// volume's MetaReplica set, pushes a copy to any replica that is missing it.
//
// Electing the highest-ranked *holder* — rather than only the ring primary —
// means an idle volume's map is still re-replicated after a node loss even when
// the node that was lost was the primary. It complements the synchronous quorum
// fan-out on the write path (ExtentCoordinator.ApplyDelta), which keeps replicas
// in step only while writes flow: a volume written once and then left idle would
// otherwise stay under-replicated forever after a later node loss.
type ExtentScrubber struct {
	place    MetaPlacement
	catalog  ExtentCatalog
	probe    ExtentReplicaProbe
	rf       int
	interval time.Duration
	logger   *slog.Logger

	// shortfall is the number of extent maps this node was responsible for
	// healing that were under-replicated at the last completed scrub. repairs is
	// the cumulative count of map replicas this node has re-pushed. Both are read
	// by the metrics scrape.
	shortfall atomic.Int64
	repairs   atomic.Int64

	stop chan struct{}
	done chan struct{}
}

// NewExtentScrubber builds an extent scrubber. rf < 1 clamps to 1 and a
// non-positive interval falls back to DefaultExtentScrubInterval, so a
// misconfiguration paces conservatively rather than spinning.
func NewExtentScrubber(place MetaPlacement, catalog ExtentCatalog, probe ExtentReplicaProbe, rf int, interval time.Duration, logger *slog.Logger) *ExtentScrubber {
	if rf < 1 {
		rf = 1
	}
	if interval <= 0 {
		interval = DefaultExtentScrubInterval
	}
	return &ExtentScrubber{
		place:    place,
		catalog:  catalog,
		probe:    probe,
		rf:       rf,
		interval: interval,
		logger:   logger,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Name identifies the subsystem in silod's lifecycle logs.
func (s *ExtentScrubber) Name() string { return "extent-scrubber" }

// Start runs the scrub loop until Shutdown, returning nil on a clean stop so
// silod can treat it like every other subsystem.
func (s *ExtentScrubber) Start() error {
	defer close(s.done)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel the in-flight cycle's context the moment Shutdown is called so a
	// slow scrub does not hold up the daemon's exit.
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
func (s *ExtentScrubber) Shutdown(ctx context.Context) error {
	close(s.stop)
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("extent-scrubber did not stop within the shutdown deadline (%w); a scrub cycle may be stuck on an unresponsive peer", ctx.Err())
	}
}

// runOnce walks every locally-held extent map once, healing those this node owns.
func (s *ExtentScrubber) runOnce(ctx context.Context) {
	vols := s.catalog.Volumes()
	self := s.place.SelfID()
	var short int64
	for _, v := range vols {
		if ctx.Err() != nil {
			return // shutting down mid-cycle; leave the last full count in place
		}
		if s.scrubMap(ctx, self, v) {
			short++
		}
	}
	s.shortfall.Store(short)
}

// scrubMap ensures one locally-held extent map reaches its full replica set, but
// only when this node is the highest-ranked replica that actually holds it — so
// exactly one node pushes, avoiding a re-replication stampede. It reports whether
// the map was under-replicated (some target was missing it), which the caller
// tallies into the shortfall gauge.
func (s *ExtentScrubber) scrubMap(ctx context.Context, self, volumeID string) (underReplicated bool) {
	targets := s.place.MetaReplicas(volumeID, s.rf)
	myRank := indexOf(targets, self)
	if myRank < 0 {
		return false // not a placement replica (a warmed serving copy) — never heal from here
	}
	for _, higher := range targets[:myRank] {
		if s.holds(ctx, higher, volumeID) {
			return false // a higher-ranked holder will do the pushing
		}
	}

	var (
		entries []crdt.MapEntry[uint64, string]
		loaded  bool
	)
	for _, t := range targets {
		if t == self || s.holds(ctx, t, volumeID) {
			continue
		}
		// A target that should hold this map does not: it is under-replicated,
		// whether or not the push below succeeds.
		underReplicated = true
		if !loaded {
			entries = s.catalog.Snapshot(volumeID)
			loaded = true
		}
		addr, ok := s.place.DataAddr(t)
		if !ok {
			continue // peer has not advertised a data address yet
		}
		if err := s.probe.Apply(ctx, addr, volumeID, entries, true); err != nil {
			s.logger.Warn("extent-scrubber could not re-replicate a map to a peer; will retry next cycle", "volume", volumeID, "peer", t, "error", err)
			continue
		}
		s.repairs.Add(1)
		s.logger.Info("extent-scrubber re-replicated an extent map", "volume", volumeID, "peer", t)
	}
	return underReplicated
}

// holds reports whether node holds volume's map, via an extent Stat on that peer.
// A reachable peer that does not hold the map (has=false) and any error — a
// NotFound or an unreachable peer — both count as "does not hold", so a missing
// or down higher-priority replica never blocks healing.
func (s *ExtentScrubber) holds(ctx context.Context, node, volumeID string) bool {
	addr, ok := s.place.DataAddr(node)
	if !ok {
		return false
	}
	has, _, err := s.probe.Stat(ctx, addr, volumeID)
	return err == nil && has
}

// MetricPrefix namespaces the extent scrubber's readings alongside the extent
// reaper's, under silo_extentmap.
func (s *ExtentScrubber) MetricPrefix() string { return "silo_extentmap" }

// CollectMetrics reports the under-replication shortfall observed at the last
// completed scrub and the cumulative re-replications this node has performed,
// labelled by node. Summed across the cluster (each map has exactly one
// responsible healer), the shortfall is the count of under-replicated maps.
func (s *ExtentScrubber) CollectMetrics() []metrics.Metric {
	labels := [][2]string{{"node", s.place.SelfID()}}
	return []metrics.Metric{
		{Name: "scrub_shortfall_maps", Help: "Extent maps this node is responsible for that were under-replicated at the last scrub.", Kind: metrics.Gauge, Value: float64(s.shortfall.Load()), Labels: labels},
		{Name: "scrub_repairs_total", Help: "Extent-map replicas this node has re-pushed to heal under-replication.", Kind: metrics.Counter, Value: float64(s.repairs.Load()), Labels: labels},
	}
}
