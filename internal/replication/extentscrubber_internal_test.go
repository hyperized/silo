package replication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/metrics"
)

// fakeExtCatalog is the scrubber's local-store view: a fixed set of volumes and
// their snapshots. listed (when non-nil) is closed on the first Volumes call so
// a test can await the loop running.
type fakeExtCatalog struct {
	mu     sync.Mutex
	vols   []string
	snap   map[string][]crdt.MapEntry[uint64, string]
	merged []string

	listOnce sync.Once
	listed   chan struct{}
}

func (c *fakeExtCatalog) Volumes() []string {
	if c.listed != nil {
		c.listOnce.Do(func() { close(c.listed) })
	}
	return c.vols
}

func (c *fakeExtCatalog) Snapshot(v string) []crdt.MapEntry[uint64, string] { return c.snap[v] }

// Digest reports sameDigest so peers agree by default; a test that wants a
// diverged replica marks it on the peer fake instead.
func (c *fakeExtCatalog) Digest(v string) []byte {
	if _, ok := c.snap[v]; !ok && !hasVol(c.vols, v) {
		return nil
	}
	return sameDigest
}

// Merge records what a reconciliation folded in, and makes it visible to later
// Snapshot calls so a test can assert the union is what gets pushed back out.
func (c *fakeExtCatalog) Merge(v string, entries []crdt.MapEntry[uint64, string]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snap == nil {
		c.snap = map[string][]crdt.MapEntry[uint64, string]{}
	}
	c.merged = append(c.merged, v)
	seen := map[uint64]bool{}
	for _, e := range c.snap[v] {
		seen[e.Key] = true
	}
	for _, e := range entries {
		if !seen[e.Key] {
			c.snap[v] = append(c.snap[v], e)
		}
	}
}

func hasVol(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// applies to addr for vol, scanning the shared fakeEpeers record.
func appliedTo(recs []appliedRec, addr, vol string) (appliedRec, bool) {
	for _, r := range recs {
		if r.addr == addr && r.vol == vol {
			return r, true
		}
	}
	return appliedRec{}, false
}

func extScrubMetric(t *testing.T, s *ExtentScrubber, name string) metrics.Metric {
	t.Helper()
	for _, m := range s.CollectMetrics() {
		if m.Name == name {
			return m
		}
	}
	t.Fatalf("extent scrubber did not report metric %q", name)
	return metrics.Metric{}
}

func TestNewExtentScrubber_ClampsAndName(t *testing.T) {
	s := NewExtentScrubber(&fakeMetaPlace{self: "a"}, &fakeExtCatalog{}, newFakeEpeers(), 0, 0, quietLog())
	if s.rf != 1 {
		t.Errorf("rf clamp: got %d, want 1", s.rf)
	}
	if s.interval != DefaultExtentScrubInterval {
		t.Errorf("interval default: got %v, want %v", s.interval, DefaultExtentScrubInterval)
	}
	if s.Name() != "extent-scrubber" {
		t.Errorf("Name: got %q, want extent-scrubber", s.Name())
	}
}

func TestExtentScrubber_HealsMissingReplicaAsPrimary(t *testing.T) {
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b", "c"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{
		"v1": {{Key: 0, Value: "c0"}},
	}}
	peers := newFakeEpeers()
	peers.statHas["c:7000"] = true // c holds it, b does not
	s := NewExtentScrubber(place, cat, peers, 3, time.Hour, quietLog())

	s.runOnce(context.Background())

	recs := peers.snapApplied()
	if r, ok := appliedTo(recs, "b:7000", "v1"); !ok {
		t.Error("scrubber should re-replicate v1 to b (missing)")
	} else if !r.ensure || len(r.entries) != 1 || r.entries[0].Value != "c0" {
		t.Errorf("push to b should carry the snapshot with ensure=true, got %+v", r)
	}
	if _, ok := appliedTo(recs, "c:7000", "v1"); ok {
		t.Error("scrubber should not push v1 to c (already holds it)")
	}
}

func TestExtentScrubber_DefersToHigherPriorityHolder(t *testing.T) {
	place := &fakeMetaPlace{self: "b", replicas: []string{"a", "b", "c"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{"v1": {{Key: 0, Value: "c0"}}}}
	peers := newFakeEpeers()
	peers.statHas["a:7000"] = true // a (rank 0) holds it
	s := NewExtentScrubber(place, cat, peers, 3, time.Hour, quietLog())

	s.runOnce(context.Background())

	if n := len(peers.snapApplied()); n != 0 {
		t.Errorf("b should defer to higher-priority holder a, made %d pushes", n)
	}
}

func TestExtentScrubber_TakesOverWhenHigherPriorityMissing(t *testing.T) {
	place := &fakeMetaPlace{self: "b", replicas: []string{"a", "b", "c"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{"v1": {{Key: 0, Value: "c0"}}}}
	peers := newFakeEpeers() // a does not hold it -> b takes over
	s := NewExtentScrubber(place, cat, peers, 3, time.Hour, quietLog())

	s.runOnce(context.Background())

	recs := peers.snapApplied()
	if _, ok := appliedTo(recs, "a:7000", "v1"); !ok {
		t.Error("b should re-replicate to a")
	}
	if _, ok := appliedTo(recs, "c:7000", "v1"); !ok {
		t.Error("b should re-replicate to c")
	}
}

func TestExtentScrubber_SkipsMapNotReplicatedHere(t *testing.T) {
	place := &fakeMetaPlace{self: "z", replicas: []string{"a", "b", "c"}} // z is not a replica
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{"v1": {{Key: 0, Value: "c0"}}}}
	peers := newFakeEpeers()
	s := NewExtentScrubber(place, cat, peers, 3, time.Hour, quietLog())

	s.runOnce(context.Background())

	if n := len(peers.snapApplied()); n != 0 {
		t.Errorf("a warmed serving copy (not a replica) must not heal, made %d pushes", n)
	}
}

func TestExtentScrubber_SkipsTargetWithoutDataAddress(t *testing.T) {
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "d"}, noAddr: map[string]bool{"d": true}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{"v1": {{Key: 0, Value: "c0"}}}}
	peers := newFakeEpeers()
	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())

	s.runOnce(context.Background())

	if n := len(peers.snapApplied()); n != 0 {
		t.Error("a target without a data address must be skipped")
	}
	// It still counts as under-replicated: the shortfall gauge reflects the gap.
	if got := extScrubMetric(t, s, "scrub_shortfall_maps").Value; got != 1 {
		t.Errorf("shortfall = %v, want 1 (a target was missing the map)", got)
	}
}

func TestExtentScrubber_ReachablePeerWithoutMapCountsAsMissing(t *testing.T) {
	// b answers Stat (err==nil) but reports has=false: it is reachable yet does
	// not hold the map, so the scrubber must still push to it.
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{"v1": {{Key: 0, Value: "c0"}}}}
	peers := newFakeEpeers() // statHas["b:7000"] defaults to false, statErr nil
	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())

	s.runOnce(context.Background())

	if _, ok := appliedTo(peers.snapApplied(), "b:7000", "v1"); !ok {
		t.Error("a reachable peer that does not hold the map must still be healed")
	}
}

func TestExtentScrubber_EmptyMapPushesEnsure(t *testing.T) {
	// A created-but-never-written volume has an empty snapshot; the scrubber must
	// still establish it on a missing replica via ensure.
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{"v1": nil}}
	peers := newFakeEpeers()
	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())

	s.runOnce(context.Background())

	r, ok := appliedTo(peers.snapApplied(), "b:7000", "v1")
	if !ok {
		t.Fatal("an empty map must still be established on a missing replica")
	}
	if !r.ensure || len(r.entries) != 0 {
		t.Errorf("empty-map push must be ensure=true with no entries, got %+v", r)
	}
}

func TestExtentScrubber_ApplyErrorIsLoggedNotFatal(t *testing.T) {
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{"v1": {{Key: 0, Value: "c0"}}}}
	peers := newFakeEpeers()
	peers.applyErr["b:7000"] = errors.New("peer down")
	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())

	s.runOnce(context.Background()) // must not panic

	if got := extScrubMetric(t, s, "scrub_repairs_total").Value; got != 0 {
		t.Errorf("a failed Apply must not count as a repair, got %v", got)
	}
	// The gap is still observed as a shortfall so the operator sees it.
	if got := extScrubMetric(t, s, "scrub_shortfall_maps").Value; got != 1 {
		t.Errorf("shortfall = %v, want 1", got)
	}
}

func TestExtentScrubber_StopsMidCycleOnCancel(t *testing.T) {
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{"v1": {{Key: 0, Value: "c0"}}}}
	peers := newFakeEpeers()
	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.runOnce(ctx)

	if n := len(peers.snapApplied()); n != 0 {
		t.Error("a cancelled cycle must stop before doing work")
	}
}

func TestExtentScrubber_StartTicksAndShutsDown(t *testing.T) {
	place := &fakeMetaPlace{self: "a", replicas: []string{"a"}}
	cat := &fakeExtCatalog{listed: make(chan struct{})}
	s := NewExtentScrubber(place, cat, newFakeEpeers(), 1, time.Millisecond, quietLog())

	go func() { _ = s.Start() }()

	select {
	case <-cat.listed:
	case <-time.After(2 * time.Second):
		t.Fatal("extent scrubber never ran a cycle")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func TestExtentScrubber_ShutdownDeadlineExpires(t *testing.T) {
	s := NewExtentScrubber(&fakeMetaPlace{self: "a"}, &fakeExtCatalog{}, newFakeEpeers(), 1, time.Hour, quietLog())
	// Never started, so done never closes; an already-expired context must
	// surface the deadline error rather than block forever.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := s.Shutdown(ctx); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want a deadline-exceeded error", err)
	}
}

func TestExtentScrubber_Metrics(t *testing.T) {
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b", "c"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{"v1": {{Key: 0, Value: "c0"}}}}
	peers := newFakeEpeers()
	peers.statHas["c:7000"] = true // b is missing v1
	s := NewExtentScrubber(place, cat, peers, 3, time.Hour, quietLog())

	if s.MetricPrefix() != "silo_extentmap" {
		t.Errorf("prefix = %q", s.MetricPrefix())
	}

	s.runOnce(context.Background())

	short := extScrubMetric(t, s, "scrub_shortfall_maps")
	if short.Value != 1 || short.Kind != metrics.Gauge {
		t.Errorf("shortfall = %v (kind %v), want 1 gauge", short.Value, short.Kind)
	}
	if len(short.Labels) != 1 || short.Labels[0] != [2]string{"node", "a"} {
		t.Errorf("shortfall labels = %v, want node=a", short.Labels)
	}
	repairs := extScrubMetric(t, s, "scrub_repairs_total")
	if repairs.Value != 1 || repairs.Kind != metrics.Counter {
		t.Errorf("repairs = %v (kind %v), want 1 counter", repairs.Value, repairs.Kind)
	}

	// A second cycle now finds v1 fully replicated: shortfall clears, repairs is
	// cumulative and stays at 1.
	peers.statHas["b:7000"] = true
	s.runOnce(context.Background())
	if got := extScrubMetric(t, s, "scrub_shortfall_maps").Value; got != 0 {
		t.Errorf("shortfall after healing = %v, want 0", got)
	}
	if got := extScrubMetric(t, s, "scrub_repairs_total").Value; got != 1 {
		t.Errorf("repairs after healing = %v, want a cumulative 1", got)
	}
}

// --- divergence reconciliation ----------------------------------------------

func TestExtentScrubber_ReconcilesDivergedReplicaBothWays(t *testing.T) {
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}}
	// a holds extent 0; b holds extent 1 and is reachable, so the old presence
	// check saw nothing wrong. Each side has a binding the other never received,
	// which is what a quorum ack leaves behind.
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{
		"v1": {{Key: 0, Value: "c0"}},
	}}
	peers := newFakeEpeers()
	peers.statHas["b:7000"] = true
	peers.statDiverged["b:7000"] = true
	peers.fetchRes["b:7000"] = []crdt.MapEntry[uint64, string]{{Key: 1, Value: "c1"}}
	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())

	if !s.scrubMap(context.Background(), "a", "v1") {
		t.Fatal("a diverged replica should count as under-replicated")
	}

	// The peer's binding must have been folded in locally, not just overwritten.
	got := cat.Snapshot("v1")
	keys := map[uint64]bool{}
	for _, e := range got {
		keys[e.Key] = true
	}
	if !keys[0] || !keys[1] {
		t.Errorf("local map = %+v, want the union of both sides", got)
	}
	// And b gains what only a had, but only that. Pushing the whole map back
	// would size the message by the map rather than by the divergence, which on a
	// large volume overruns gRPC's limit and never lands at all.
	rec, ok := appliedTo(peers.snapApplied(), "b:7000", "v1")
	if !ok {
		t.Fatal("the reconciled map was never pushed back to the diverged replica")
	}
	if len(rec.entries) != 1 || rec.entries[0].Key != 0 {
		t.Errorf("pushed %+v, want only extent 0, the binding b was missing", rec.entries)
	}
}

func TestExtentScrubber_LeavesAgreeingReplicasAlone(t *testing.T) {
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{
		"v1": {{Key: 0, Value: "c0"}},
	}}
	peers := newFakeEpeers()
	peers.statHas["b:7000"] = true // holds it and agrees

	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())
	if s.scrubMap(context.Background(), "a", "v1") {
		t.Error("a replica that already agrees must not be reported under-replicated")
	}
	if len(peers.snapApplied()) != 0 {
		t.Errorf("nothing should be pushed to an agreeing replica: %+v", peers.snapApplied())
	}
	if len(cat.merged) != 0 {
		t.Errorf("no merge should happen when replicas agree: %v", cat.merged)
	}
}

func TestExtentScrubber_FetchFailureLeavesTheMapAlone(t *testing.T) {
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{
		"v1": {{Key: 0, Value: "c0"}},
	}}
	peers := newFakeEpeers()
	peers.statHas["b:7000"] = true
	peers.statDiverged["b:7000"] = true
	peers.fetchErr["b:7000"] = errors.New("peer went away")

	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())
	s.scrubMap(context.Background(), "a", "v1")

	if len(cat.merged) != 0 {
		t.Errorf("a failed fetch must not merge anything: %v", cat.merged)
	}
}

// --- the GC's currency guard -------------------------------------------------

func TestExtentScrubber_MapsConverged(t *testing.T) {
	vols := map[string]struct{}{"v1": {}}
	base := func() (*fakeMetaPlace, *fakeExtCatalog) {
		return &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}},
			&fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{
				"v1": {{Key: 0, Value: "c0"}},
			}}
	}

	t.Run("agreeing replicas converge", func(t *testing.T) {
		place, cat := base()
		peers := newFakeEpeers()
		peers.statHas["b:7000"] = true
		s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())
		if ok, who := s.MapsConverged(context.Background(), vols); !ok {
			t.Errorf("MapsConverged = false (%q), want true", who)
		}
	})

	t.Run("a diverged replica blocks the sweep", func(t *testing.T) {
		place, cat := base()
		peers := newFakeEpeers()
		peers.statHas["b:7000"] = true
		peers.statDiverged["b:7000"] = true
		s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())
		ok, who := s.MapsConverged(context.Background(), vols)
		if ok || who != "v1" {
			t.Errorf("MapsConverged = (%v,%q), want (false,\"v1\")", ok, who)
		}
	})

	t.Run("an unreachable replica counts as not converged", func(t *testing.T) {
		place, cat := base()
		peers := newFakeEpeers()
		peers.statErr["b:7000"] = errors.New("unreachable")
		s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())
		if ok, _ := s.MapsConverged(context.Background(), vols); ok {
			t.Error("an unreachable replica must not read as agreement: the sweep would run half-blind")
		}
	})

	t.Run("cancellation stops the walk", func(t *testing.T) {
		place, cat := base()
		peers := newFakeEpeers()
		peers.statHas["b:7000"] = true
		s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if ok, _ := s.MapsConverged(ctx, vols); ok {
			t.Error("a cancelled check must not report convergence")
		}
	})
}

func TestExtentScrubber_SkipsPeersWithNoAdvertisedAddress(t *testing.T) {
	// A replica that has not advertised a data address yet cannot be probed,
	// reconciled, or pushed to. It must be passed over rather than crashed on.
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}, noAddr: map[string]bool{"b": true}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{
		"v1": {{Key: 0, Value: "c0"}},
	}}
	peers := newFakeEpeers()
	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())

	s.scrubMap(context.Background(), "a", "v1")

	if len(peers.snapApplied()) != 0 {
		t.Errorf("nothing can be pushed to a peer with no address: %+v", peers.snapApplied())
	}
	if ok, _ := s.MapsConverged(context.Background(), map[string]struct{}{"v1": {}}); ok {
		t.Error("a peer that cannot be reached must not read as agreement")
	}
}

func TestExtentScrubber_FallsBackToCountWhenNoDigest(t *testing.T) {
	// A peer older than the digest field returns none, leaving the extent count
	// as the only comparison available.
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{
		"v1": {{Key: 0, Value: "c0"}, {Key: 1, Value: "c1"}},
	}}
	peers := newFakeEpeers()
	peers.statHas["b:7000"] = true
	peers.noDigest["b:7000"] = true
	peers.fetchRes["b:7000"] = []crdt.MapEntry[uint64, string]{{Key: 0, Value: "c0"}} // 1 entry vs our 2

	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())
	same, known := s.agrees(context.Background(), "b", "v1")
	if !known || same {
		t.Errorf("agrees = (%v,%v), want a known disagreement from the count", same, known)
	}

	// Matching counts read as agreement, which is the limit of this fallback.
	peers.fetchRes["b:7000"] = []crdt.MapEntry[uint64, string]{{Key: 0, Value: "c0"}, {Key: 7, Value: "c7"}}
	if same, known := s.agrees(context.Background(), "b", "v1"); !known || !same {
		t.Errorf("agrees = (%v,%v), want equal counts to read as agreement", same, known)
	}
}

func TestExtentScrubber_SplitsLargeMapsAcrossApplies(t *testing.T) {
	// A big volume's map does not fit in one gRPC message: at roughly 60 bytes
	// per binding, six figures of entries is several times the 4 MiB default, so
	// a single Apply is rejected outright and the replica never converges. This
	// is what a real 40 GiB volume did.
	restore := maxEntriesPerApply
	maxEntriesPerApply = 100
	defer func() { maxEntriesPerApply = restore }()

	entries := make([]crdt.MapEntry[uint64, string], 250)
	for i := range entries {
		entries[i] = crdt.MapEntry[uint64, string]{Key: uint64(i), Value: fmt.Sprintf("c%d", i)}
	}
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{"v1": entries}}
	peers := newFakeEpeers() // b holds nothing, so the whole map has to travel

	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())
	s.scrubMap(context.Background(), "a", "v1")

	applied := peers.snapApplied()
	total := 0
	for _, r := range applied {
		if r.addr != "b:7000" {
			continue
		}
		if len(r.entries) > maxEntriesPerApply {
			t.Errorf("one Apply carried %d entries, over the %d cap", len(r.entries), maxEntriesPerApply)
		}
		total += len(r.entries)
	}
	if total != len(entries) {
		t.Errorf("delivered %d of %d entries across %d messages", total, len(entries), len(applied))
	}
	if len(applied) < 3 {
		t.Errorf("250 entries at a cap of 100 should take 3 messages, got %d", len(applied))
	}
}

func TestExtentScrubber_EstablishesAnEmptyMapOnAPeerThatHasNone(t *testing.T) {
	// A created-but-never-written volume still needs its (empty) map on every
	// replica, so the push happens even with nothing to send.
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{"v1": {}}}
	peers := newFakeEpeers()

	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())
	s.scrubMap(context.Background(), "a", "v1")

	rec, ok := appliedTo(peers.snapApplied(), "b:7000", "v1")
	if !ok {
		t.Fatal("an empty map must still be established on a replica that has none")
	}
	if !rec.ensure {
		t.Error("the push must set ensure, or the peer creates nothing")
	}
}
