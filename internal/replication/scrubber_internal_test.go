package replication

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/metrics"
)

// newTestScrubber builds a scrubber whose live set is exactly what the catalog
// holds, so nothing is filtered as orphaned. That is the world these cases were
// written for — the filter itself is covered separately below.
func newTestScrubber(place Placement, cat *fakeCatalog, probe ReplicaProbe, rf int, interval time.Duration, logger *slog.Logger) *Scrubber {
	return NewScrubber(place, cat, fakeNSRefs{chunks: cat.ids}, fakeExtRefs{}, probe, rf, interval, logger)
}

func nopLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// stubPlace is a fixed ring view: Replicas ignores the chunk id and returns
// the configured order, which is all the scrubber logic needs to exercise.
type stubPlace struct {
	self     string
	replicas []string
	addrs    map[string]string
}

func (p stubPlace) Replicas(_ string, n int) []string {
	if n > len(p.replicas) {
		n = len(p.replicas)
	}
	out := make([]string, n)
	copy(out, p.replicas[:n])
	return out
}

func (p stubPlace) DataAddr(id string) (string, bool) {
	a, ok := p.addrs[id]
	return a, ok
}

func (p stubPlace) SelfID() string { return p.self }

type fakeCatalog struct {
	ids     []string
	listErr error
	data    map[string][]byte
	getErr  map[string]error

	listOnce sync.Once
	listed   chan struct{}
}

func (c *fakeCatalog) List(context.Context) ([]string, error) {
	if c.listed != nil {
		c.listOnce.Do(func() { close(c.listed) })
	}
	if c.listErr != nil {
		return nil, c.listErr
	}
	return c.ids, nil
}

func (c *fakeCatalog) Get(_ context.Context, id string) ([]byte, chunkstore.Info, error) {
	if err := c.getErr[id]; err != nil {
		return nil, chunkstore.Info{}, err
	}
	return c.data[id], chunkstore.Info{ID: id}, nil
}

type fakeProbe struct {
	mu       sync.Mutex
	present  map[string]bool  // addr|id -> peer holds it
	storeErr map[string]error // addr -> error on Store
	stored   map[string]bool  // addr|id -> pushed by the scrubber
}

func key(addr, id string) string { return addr + "|" + id }

func (p *fakeProbe) Stat(_ context.Context, addr, id string) (chunkstore.Info, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.present[key(addr, id)] {
		return chunkstore.Info{ID: id}, nil
	}
	return chunkstore.Info{}, errors.New("not found")
}

func (p *fakeProbe) Store(_ context.Context, addr, id string, _ []byte) (chunkstore.Info, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.storeErr[addr]; err != nil {
		return chunkstore.Info{}, err
	}
	if p.stored == nil {
		p.stored = map[string]bool{}
	}
	p.stored[key(addr, id)] = true
	if p.present == nil {
		p.present = map[string]bool{}
	}
	p.present[key(addr, id)] = true
	return chunkstore.Info{ID: id}, nil
}

func (p *fakeProbe) pushed(addr, id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stored[key(addr, id)]
}

func (p *fakeProbe) pushCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.stored)
}

func TestNewScrubber_ClampsAndName(t *testing.T) {
	s := newTestScrubber(stubPlace{self: "a"}, &fakeCatalog{}, &fakeProbe{}, 0, 0, nopLogger())
	if s.rf != 1 {
		t.Errorf("rf clamp: got %d, want 1", s.rf)
	}
	if s.interval != DefaultScrubInterval {
		t.Errorf("interval default: got %v, want %v", s.interval, DefaultScrubInterval)
	}
	if s.Name() != "scrubber" {
		t.Errorf("Name: got %q, want scrubber", s.Name())
	}
}

func TestScrubber_HealsMissingReplicaAsPrimary(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "b", "c"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000", "c": "c:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{key("c:7000", "c1"): true}} // c holds it, b does not
	s := newTestScrubber(place, cat, probe, 3, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if !probe.pushed("b:7000", "c1") {
		t.Error("scrubber should re-replicate c1 to b (missing)")
	}
	if probe.pushed("c:7000", "c1") {
		t.Error("scrubber should not push c1 to c (already holds it)")
	}
}

func TestScrubber_DefersToHigherPriorityHolder(t *testing.T) {
	place := stubPlace{
		self:     "b",
		replicas: []string{"a", "b", "c"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000", "c": "c:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{key("a:7000", "c1"): true}} // a (rank 0) holds it
	s := newTestScrubber(place, cat, probe, 3, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if probe.pushCount() != 0 {
		t.Errorf("b should defer to higher-priority holder a, made %d pushes", probe.pushCount())
	}
}

func TestScrubber_TakesOverWhenHigherPriorityMissing(t *testing.T) {
	place := stubPlace{
		self:     "b",
		replicas: []string{"a", "b", "c"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000", "c": "c:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{}} // a does not hold it -> b takes over
	s := newTestScrubber(place, cat, probe, 3, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if !probe.pushed("a:7000", "c1") || !probe.pushed("c:7000", "c1") {
		t.Errorf("b should re-replicate to both a and c; stored=%d", probe.pushCount())
	}
}

func TestScrubber_SkipsChunkNotReplicatedHere(t *testing.T) {
	place := stubPlace{
		self:     "z",
		replicas: []string{"a", "b", "c"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000", "c": "c:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{}}
	s := newTestScrubber(place, cat, probe, 3, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if probe.pushCount() != 0 {
		t.Errorf("a chunk this node is not a replica of must be skipped, made %d pushes", probe.pushCount())
	}
}

func TestScrubber_ListErrorIsLoggedNotFatal(t *testing.T) {
	cat := &fakeCatalog{listErr: errors.New("disk gone")}
	probe := &fakeProbe{}
	s := newTestScrubber(stubPlace{self: "a"}, cat, probe, 1, time.Hour, nopLogger())
	s.runOnce(context.Background()) // must not panic
	if probe.pushCount() != 0 {
		t.Error("a list failure must do no work")
	}
}

func TestScrubber_GetErrorAbortsPush(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "b"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, getErr: map[string]error{"c1": errors.New("read failed")}}
	probe := &fakeProbe{present: map[string]bool{}}
	s := newTestScrubber(place, cat, probe, 2, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if probe.pushCount() != 0 {
		t.Error("a failed local read must abort the push, not send empty data")
	}
}

func TestScrubber_SkipsTargetWithoutDataAddress(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "d"},
		addrs:    map[string]string{"a": "a:7000"}, // d has no advertised address
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{}}
	s := newTestScrubber(place, cat, probe, 2, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if probe.pushCount() != 0 {
		t.Error("a target without a data address must be skipped")
	}
}

func TestScrubber_StoreErrorIsLoggedNotFatal(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "b"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{}, storeErr: map[string]error{"b:7000": errors.New("peer down")}}
	s := newTestScrubber(place, cat, probe, 2, time.Hour, nopLogger())

	s.runOnce(context.Background()) // must not panic
	if probe.pushed("b:7000", "c1") {
		t.Error("a failed Store must not be recorded as pushed")
	}
}

func TestScrubber_StopsMidCycleOnCancel(t *testing.T) {
	place := stubPlace{self: "a", replicas: []string{"a", "b"}, addrs: map[string]string{"a": "a:7000", "b": "b:7000"}}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{}}
	s := newTestScrubber(place, cat, probe, 2, time.Hour, nopLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.runOnce(ctx)

	if probe.pushCount() != 0 {
		t.Error("a cancelled cycle must stop before doing work")
	}
}

func TestScrubber_StartTicksAndShutsDown(t *testing.T) {
	place := stubPlace{self: "a", replicas: []string{"a"}, addrs: map[string]string{"a": "a:7000"}}
	cat := &fakeCatalog{ids: nil, listed: make(chan struct{})}
	s := newTestScrubber(place, cat, &fakeProbe{}, 1, time.Millisecond, nopLogger())

	go func() { _ = s.Start() }()

	select {
	case <-cat.listed:
	case <-time.After(2 * time.Second):
		t.Fatal("scrubber never ran a cycle")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func TestScrubber_ShutdownDeadlineExpires(t *testing.T) {
	s := newTestScrubber(stubPlace{self: "a"}, &fakeCatalog{}, &fakeProbe{}, 1, time.Hour, nopLogger())
	// Never started, so done never closes; an already-expired context must
	// surface the deadline error rather than block forever.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := s.Shutdown(ctx); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want a deadline-exceeded error", err)
	}
}

func scrubberMetric(t *testing.T, s *Scrubber, name string) metrics.Metric {
	t.Helper()
	for _, m := range s.CollectMetrics() {
		if m.Name == name {
			return m
		}
	}
	t.Fatalf("scrubber did not report metric %q", name)
	return metrics.Metric{}
}

func TestScrubber_ReplicationMetrics(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "b", "c"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000", "c": "c:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{key("c:7000", "c1"): true}} // b is missing c1
	s := newTestScrubber(place, cat, probe, 3, time.Hour, nopLogger())

	if s.MetricPrefix() != "silo_replication" {
		t.Errorf("prefix = %q", s.MetricPrefix())
	}

	s.runOnce(context.Background())

	short := scrubberMetric(t, s, "shortfall_chunks")
	if short.Value != 1 || short.Kind != metrics.Gauge {
		t.Errorf("shortfall = %v (kind %v), want 1 gauge", short.Value, short.Kind)
	}
	if len(short.Labels) != 1 || short.Labels[0] != [2]string{"node", "a"} {
		t.Errorf("shortfall labels = %v, want node=a", short.Labels)
	}
	repairs := scrubberMetric(t, s, "repairs_total")
	if repairs.Value != 1 || repairs.Kind != metrics.Counter {
		t.Errorf("repairs = %v (kind %v), want 1 counter", repairs.Value, repairs.Kind)
	}

	// A second cycle now finds c1 fully replicated: shortfall clears to 0, while
	// repairs is cumulative and stays at 1.
	s.runOnce(context.Background())
	if got := scrubberMetric(t, s, "shortfall_chunks").Value; got != 0 {
		t.Errorf("shortfall after healing = %v, want 0", got)
	}
	if got := scrubberMetric(t, s, "repairs_total").Value; got != 1 {
		t.Errorf("repairs after healing = %v, want a cumulative 1", got)
	}
}

// --- live-set filter --------------------------------------------------------

func TestScrubber_SkipsUnreferencedChunks(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "b"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000"},
	}
	cat := &fakeCatalog{
		ids:  []string{"live1", "orphan1", "orphan2"},
		data: map[string][]byte{"live1": []byte("x"), "orphan1": []byte("y"), "orphan2": []byte("z")},
	}
	probe := &fakeProbe{} // b holds nothing, so everything looks under-replicated
	// Only live1 is referenced; the two orphans are the GC's to reclaim.
	s := NewScrubber(place, cat, fakeNSRefs{chunks: []string{"live1"}}, fakeExtRefs{}, probe, 2, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if !probe.pushed("b:7000", "live1") {
		t.Error("live1 is referenced and under-replicated; it should have been healed")
	}
	for _, orphan := range []string{"orphan1", "orphan2"} {
		if probe.pushed("b:7000", orphan) {
			t.Errorf("%s is unreferenced; healing it would undo the GC's reclamation", orphan)
		}
	}
	if got := scrubberMetric(t, s, "unreferenced_skipped").Value; got != 2 {
		t.Errorf("unreferenced_skipped = %v, want 2", got)
	}
	// Only the referenced chunk counts toward the replication-health signal.
	if got := scrubberMetric(t, s, "shortfall_chunks").Value; got != 1 {
		t.Errorf("shortfall = %v, want 1 (orphans must not inflate it)", got)
	}
	if got := scrubberMetric(t, s, "incomplete_view").Value; got != 0 {
		t.Errorf("incomplete_view = %v, want 0", got)
	}
}

func TestScrubber_CountsExtentMapRefsAsLive(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "b"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("x")}}
	probe := &fakeProbe{}
	// The namespace names no chunks; c1's only reference is an out-of-band
	// extent map this node holds. It is live all the same.
	ext := fakeExtRefs{vols: map[string][]crdt.MapEntry[uint64, string]{
		"vol-1": {{Key: 0, Value: "c1"}},
	}}
	s := NewScrubber(place, cat, fakeNSRefs{volumes: []string{"vol-1"}}, ext, probe, 2, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if !probe.pushed("b:7000", "c1") {
		t.Error("c1 is bound by a held extent map; it should have been healed")
	}
	if got := scrubberMetric(t, s, "unreferenced_skipped").Value; got != 0 {
		t.Errorf("unreferenced_skipped = %v, want 0", got)
	}
}

func TestScrubber_HealsEverythingOnIncompleteView(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "b"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("x")}}
	probe := &fakeProbe{}
	// vol-1 is live but this node holds no extent map for it, so c1 could be
	// referenced by a map it cannot see. Skipping it might cost the last replica.
	s := NewScrubber(place, cat, fakeNSRefs{volumes: []string{"vol-1"}}, fakeExtRefs{}, probe, 2, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if !probe.pushed("b:7000", "c1") {
		t.Error("on an incomplete view the scrubber must heal rather than risk dropping a replica")
	}
	if got := scrubberMetric(t, s, "incomplete_view").Value; got != 1 {
		t.Errorf("incomplete_view = %v, want 1", got)
	}
	if got := scrubberMetric(t, s, "unreferenced_skipped").Value; got != 0 {
		t.Errorf("unreferenced_skipped = %v, want 0 when the filter is off", got)
	}
}

func TestScrubber_ClearsIncompleteViewOnceMapsArrive(t *testing.T) {
	place := stubPlace{self: "a", replicas: []string{"a"}, addrs: map[string]string{"a": "a:7000"}}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("x")}}
	ns := fakeNSRefs{volumes: []string{"vol-1"}}
	ext := fakeExtRefs{}
	s := NewScrubber(place, cat, ns, ext, &fakeProbe{}, 1, time.Hour, nopLogger())

	s.runOnce(context.Background())
	if got := scrubberMetric(t, s, "incomplete_view").Value; got != 1 {
		t.Fatalf("incomplete_view before the map lands = %v, want 1", got)
	}

	// The map replicates in; the blind spot closes and the filter engages.
	s.ext = fakeExtRefs{vols: map[string][]crdt.MapEntry[uint64, string]{"vol-1": nil}}
	s.runOnce(context.Background())

	if got := scrubberMetric(t, s, "incomplete_view").Value; got != 0 {
		t.Errorf("incomplete_view after the map lands = %v, want 0", got)
	}
	if got := scrubberMetric(t, s, "unreferenced_skipped").Value; got != 1 {
		t.Errorf("unreferenced_skipped = %v, want 1 (c1 is bound by nothing)", got)
	}
}
