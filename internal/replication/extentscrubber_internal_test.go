package replication

import (
	"context"
	"errors"
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
	vols []string
	snap map[string][]crdt.MapEntry[uint64, string]

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
