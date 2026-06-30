package replication

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/metrics"
)

// --- fakes ------------------------------------------------------------------

type fakeChunkLister struct {
	mu        sync.Mutex
	ids       []string
	listErr   error
	listCalls int
	statErr   map[string]error
	statCalls int
	mtimes    map[string]time.Time // per-chunk CreatedAt; zero value => epoch (old)
	delErr    map[string]error
	deleted   []string
	listedCh  chan struct{} // optional: one signal per List call
}

func newFakeChunkLister(ids ...string) *fakeChunkLister {
	return &fakeChunkLister{ids: ids, statErr: map[string]error{}, mtimes: map[string]time.Time{}, delErr: map[string]error{}}
}

func (f *fakeChunkLister) List(context.Context) ([]string, error) {
	f.mu.Lock()
	f.listCalls++
	f.mu.Unlock()
	if f.listedCh != nil {
		select {
		case f.listedCh <- struct{}{}:
		default:
		}
	}
	return f.ids, f.listErr
}

func (f *fakeChunkLister) Stat(_ context.Context, id string) (chunkstore.Info, error) {
	f.mu.Lock()
	f.statCalls++
	f.mu.Unlock()
	if err := f.statErr[id]; err != nil {
		return chunkstore.Info{}, err
	}
	return chunkstore.Info{ID: id, CreatedAt: f.mtimes[id]}, nil
}

func (f *fakeChunkLister) Delete(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.delErr[id]; err != nil {
		return err
	}
	f.deleted = append(f.deleted, id)
	return nil
}

func (f *fakeChunkLister) snapDeleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

// fakeNSRefs returns fresh copies each call, mirroring *namespace.Namespace —
// the GC mutates the returned chunk set, so a shared instance would leak state.
type fakeNSRefs struct {
	chunks  []string
	volumes []string
}

func (f fakeNSRefs) LiveChunkRefs() (map[string]struct{}, map[string]struct{}) {
	c := make(map[string]struct{}, len(f.chunks))
	for _, x := range f.chunks {
		c[x] = struct{}{}
	}
	v := make(map[string]struct{}, len(f.volumes))
	for _, x := range f.volumes {
		v[x] = struct{}{}
	}
	return c, v
}

type fakeExtRefs struct {
	vols map[string][]crdt.MapEntry[uint64, string] // volumeID -> entries held here
}

func (f fakeExtRefs) Volumes() []string {
	out := make([]string, 0, len(f.vols))
	for v := range f.vols {
		out = append(out, v)
	}
	return out
}

func (f fakeExtRefs) Snapshot(v string) []crdt.MapEntry[uint64, string] { return f.vols[v] }

func gcMetric(t *testing.T, g *ChunkGC, name string) metrics.Metric {
	t.Helper()
	for _, m := range g.CollectMetrics() {
		if m.Name == name {
			return m
		}
	}
	t.Fatalf("metric %q not found", name)
	return metrics.Metric{}
}

// --- tests ------------------------------------------------------------------

func TestChunkGC_ClampsAndName(t *testing.T) {
	g := NewChunkGC(newFakeChunkLister(), fakeNSRefs{}, fakeExtRefs{}, "a", 0, 0, false, quietLog())
	if g.grace != DefaultChunkGCGrace || g.interval != DefaultChunkGCInterval {
		t.Errorf("clamp: grace=%v interval=%v, want defaults", g.grace, g.interval)
	}
	if g.Name() != "chunk-gc" || g.MetricPrefix() != "silo_chunkgc" {
		t.Errorf("name=%q prefix=%q", g.Name(), g.MetricPrefix())
	}
}

func TestChunkGC_DryRunReportsOrphansWhenComplete(t *testing.T) {
	// keep = {f1} (manifest) ∪ {k1} (extent map of held volume v1). orphan is
	// unreferenced and old. Dry run: counted, not deleted.
	lister := newFakeChunkLister("f1", "k1", "orphan")
	ns := fakeNSRefs{chunks: []string{"f1"}, volumes: []string{"v1"}}
	ext := fakeExtRefs{vols: map[string][]crdt.MapEntry[uint64, string]{"v1": {{Key: 0, Value: "k1"}}}}
	g := NewChunkGC(lister, ns, ext, "a", time.Hour, time.Hour, false, quietLog())

	g.runOnce(context.Background())

	if got := gcMetric(t, g, "orphan_chunks").Value; got != 1 {
		t.Errorf("orphan_chunks = %v, want 1", got)
	}
	if got := gcMetric(t, g, "reclaimed_total").Value; got != 0 {
		t.Errorf("reclaimed_total = %v, want 0 in a dry run", got)
	}
	if got := gcMetric(t, g, "incomplete_view").Value; got != 0 {
		t.Errorf("incomplete_view = %v, want 0 (complete)", got)
	}
	if len(lister.snapDeleted()) != 0 {
		t.Errorf("a dry run must not delete: %v", lister.snapDeleted())
	}
}

func TestChunkGC_EnabledDeletesOrphans(t *testing.T) {
	lister := newFakeChunkLister("f1", "orphan")
	ns := fakeNSRefs{chunks: []string{"f1"}}
	g := NewChunkGC(lister, ns, fakeExtRefs{}, "a", time.Hour, time.Hour, true, quietLog())

	g.runOnce(context.Background())

	if del := lister.snapDeleted(); len(del) != 1 || del[0] != "orphan" {
		t.Errorf("deleted = %v, want [orphan]", del)
	}
	if got := gcMetric(t, g, "reclaimed_total").Value; got != 1 {
		t.Errorf("reclaimed_total = %v, want 1", got)
	}
	if got := gcMetric(t, g, "last_reclaimed").Value; got != 1 {
		t.Errorf("last_reclaimed = %v, want 1", got)
	}
}

func TestChunkGC_GraceProtectsYoungChunks(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	lister := newFakeChunkLister("young")
	lister.mtimes["young"] = base // exactly now: within the grace window
	g := NewChunkGC(lister, fakeNSRefs{}, fakeExtRefs{}, "a", time.Hour, time.Hour, true, quietLog())
	g.now = func() time.Time { return base }

	g.runOnce(context.Background())

	if got := gcMetric(t, g, "orphan_chunks").Value; got != 0 {
		t.Errorf("orphan_chunks = %v, want 0 (young chunk protected)", got)
	}
	if len(lister.snapDeleted()) != 0 {
		t.Errorf("a within-grace chunk must not be deleted: %v", lister.snapDeleted())
	}
}

func TestChunkGC_IncompleteViewAbstains(t *testing.T) {
	// Two live volumes, but this node holds only v1's map: v2 is a blind spot.
	lister := newFakeChunkLister("orphan")
	ns := fakeNSRefs{volumes: []string{"v1", "v2"}}
	ext := fakeExtRefs{vols: map[string][]crdt.MapEntry[uint64, string]{"v1": nil}}
	g := NewChunkGC(lister, ns, ext, "a", time.Hour, time.Hour, true, quietLog())

	g.runOnce(context.Background())

	if got := gcMetric(t, g, "incomplete_view").Value; got != 1 {
		t.Errorf("incomplete_view = %v, want 1", got)
	}
	if got := gcMetric(t, g, "unaccounted_volumes").Value; got != 1 {
		t.Errorf("unaccounted_volumes = %v, want 1", got)
	}
	if lister.listCalls != 0 {
		t.Errorf("an incomplete view must abstain before listing chunks, listCalls=%d", lister.listCalls)
	}
	if len(lister.snapDeleted()) != 0 {
		t.Errorf("an incomplete view must not delete: %v", lister.snapDeleted())
	}
}

func TestChunkGC_ListErrorIsLoggedNotFatal(t *testing.T) {
	lister := newFakeChunkLister("orphan")
	lister.listErr = errors.New("readdir boom")
	g := NewChunkGC(lister, fakeNSRefs{}, fakeExtRefs{}, "a", time.Hour, time.Hour, true, quietLog())

	g.runOnce(context.Background()) // must not panic

	if got := gcMetric(t, g, "orphan_chunks").Value; got != 0 {
		t.Errorf("orphan_chunks = %v, want 0 after a list error", got)
	}
	if len(lister.snapDeleted()) != 0 {
		t.Error("a list error must not delete anything")
	}
}

func TestChunkGC_StatErrorSkipsChunk(t *testing.T) {
	lister := newFakeChunkLister("x")
	lister.statErr["x"] = errors.New("stat boom") // raced with a delete
	g := NewChunkGC(lister, fakeNSRefs{}, fakeExtRefs{}, "a", time.Hour, time.Hour, true, quietLog())

	g.runOnce(context.Background())

	if got := gcMetric(t, g, "orphan_chunks").Value; got != 0 {
		t.Errorf("orphan_chunks = %v, want 0 (un-stattable chunk skipped)", got)
	}
	if len(lister.snapDeleted()) != 0 {
		t.Error("an un-stattable chunk must not be deleted")
	}
}

func TestChunkGC_DeleteErrorIsLoggedNotReclaimed(t *testing.T) {
	lister := newFakeChunkLister("orphan")
	lister.delErr["orphan"] = errors.New("unlink boom")
	g := NewChunkGC(lister, fakeNSRefs{}, fakeExtRefs{}, "a", time.Hour, time.Hour, true, quietLog())

	g.runOnce(context.Background())

	if got := gcMetric(t, g, "orphan_chunks").Value; got != 1 {
		t.Errorf("orphan_chunks = %v, want 1 (still counted as an orphan)", got)
	}
	if got := gcMetric(t, g, "reclaimed_total").Value; got != 0 {
		t.Errorf("reclaimed_total = %v, want 0 (delete failed)", got)
	}
}

func TestChunkGC_StopsMidCycleOnCancel(t *testing.T) {
	lister := newFakeChunkLister("orphan")
	g := NewChunkGC(lister, fakeNSRefs{}, fakeExtRefs{}, "a", time.Hour, time.Hour, true, quietLog())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g.runOnce(ctx)

	if lister.statCalls != 0 {
		t.Errorf("a cancelled sweep must stop before stat-ing chunks, statCalls=%d", lister.statCalls)
	}
	if len(lister.snapDeleted()) != 0 {
		t.Error("a cancelled sweep must not delete")
	}
}

func TestChunkGC_StartTicksAndShutsDown(t *testing.T) {
	lister := newFakeChunkLister()
	lister.listedCh = make(chan struct{}, 1)
	g := NewChunkGC(lister, fakeNSRefs{}, fakeExtRefs{}, "a", time.Hour, time.Millisecond, false, quietLog())

	go func() { _ = g.Start() }()

	select {
	case <-lister.listedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("chunk-gc never ran a sweep")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func TestChunkGC_ShutdownDeadlineExpires(t *testing.T) {
	g := NewChunkGC(newFakeChunkLister(), fakeNSRefs{}, fakeExtRefs{}, "a", time.Hour, time.Hour, false, quietLog())
	// Never started, so done never closes; an already-expired context must
	// surface the deadline error rather than block forever.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := g.Shutdown(ctx); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want a deadline-exceeded error", err)
	}
}

func TestChunkGC_Metrics(t *testing.T) {
	lister := newFakeChunkLister("orphan")
	g := NewChunkGC(lister, fakeNSRefs{}, fakeExtRefs{}, "node-x", time.Hour, time.Hour, false, quietLog())
	g.runOnce(context.Background())

	for _, tc := range []struct {
		name string
		kind metrics.Kind
		val  float64
	}{
		{"orphan_chunks", metrics.Gauge, 1},
		{"reclaimed_total", metrics.Counter, 0},
		{"last_reclaimed", metrics.Gauge, 0},
		{"incomplete_view", metrics.Gauge, 0},
		{"unaccounted_volumes", metrics.Gauge, 0},
	} {
		m := gcMetric(t, g, tc.name)
		if m.Kind != tc.kind || m.Value != tc.val {
			t.Errorf("%s = %v (kind %v), want %v (kind %v)", tc.name, m.Value, m.Kind, tc.val, tc.kind)
		}
		if len(m.Labels) != 1 || m.Labels[0] != [2]string{"node", "node-x"} {
			t.Errorf("%s labels = %v, want node=node-x", tc.name, m.Labels)
		}
	}
}
