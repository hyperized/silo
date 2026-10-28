package backup_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/backup"
)

type subFixture struct {
	tgt    *memTarget
	chunks *fakeChunks
	clock  *fakeClock
	sub    *backup.Subsystem
	logs   *syncBuffer
	// lastRun is Status().LastRun as runScheduled saw it after the final
	// counted run.
	lastRun *backup.Run
}

// syncBuffer collects log output from the subsystem's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newSub(t *testing.T, interval time.Duration, opts ...backup.SubsystemOption) *subFixture {
	t.Helper()
	f := &subFixture{tgt: newMemTarget(), chunks: threeChunks(), clock: newClock(t0, 0), logs: &syncBuffer{}}
	f.tgt.clock = f.clock.Now
	e := backup.NewExporter(f.chunks, fakeNS{snap: []byte("{}")}, testNode, backup.WithClock(f.clock.Now), backup.WithConcurrency(2))
	opts = append([]backup.SubsystemOption{backup.WithSubsystemClock(f.clock.Now)}, opts...)
	f.sub = backup.NewSubsystem(e, f.tgt, interval, slog.New(slog.NewTextHandler(f.logs, nil)), opts...)
	return f
}

// start runs the schedule and returns a stop function that shuts it down and
// checks both ends returned cleanly.
func (f *subFixture) start(t *testing.T) (stop func()) {
	t.Helper()
	errc := make(chan error, 1)
	go func() { errc <- f.sub.Start() }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := f.sub.Shutdown(ctx); err != nil {
				t.Errorf("Shutdown: %v", err)
			}
			if err := <-errc; err != nil {
				t.Errorf("Start: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func metricValues(f *subFixture) map[string]float64 {
	out := map[string]float64{}
	for _, m := range f.sub.CollectMetrics() {
		out[m.Name] = m.Value
	}
	return out
}

func TestSubsystemBasics(t *testing.T) {
	f := newSub(t, 0)
	if f.sub.Name() != "backup" || f.sub.MetricPrefix() != "silo_backup" {
		t.Fatalf("name %q prefix %q", f.sub.Name(), f.sub.MetricPrefix())
	}
	st := f.sub.Status()
	if !st.Configured || st.Target != "mem://test" || st.LastRun != nil || st.Running || !st.NextRun.IsZero() {
		t.Fatalf("status before any run = %+v", st)
	}
	if st.Interval != backup.DefaultInterval {
		t.Fatalf("Interval = %v, want the default %v", st.Interval, backup.DefaultInterval)
	}
	if got := newSub(t, -time.Second).sub.Status().Interval; got != backup.DefaultInterval {
		t.Fatalf("negative interval = %v", got)
	}
}

func TestSubsystemNextRunAfterStart(t *testing.T) {
	f := newSub(t, time.Hour)
	f.start(t)
	eventually(t, "NextRun", func() bool { return !f.sub.Status().NextRun.IsZero() })
	if got, want := f.sub.Status().NextRun, t0.Add(time.Hour); !got.Equal(want) {
		t.Fatalf("NextRun = %v, want %v", got, want)
	}
}

func TestSubsystemScheduledRun(t *testing.T) {
	f := newSub(t, 5*time.Millisecond)
	stop := f.start(t)
	eventually(t, "a scheduled run", func() bool { return f.sub.Status().LastRun != nil })
	stop()
	last := f.sub.Status().LastRun
	if !last.OK || last.ChunksUploaded != 3 || last.Error != "" {
		t.Fatalf("last run = %+v", last)
	}
	if v := metricValues(f)["runs_total"]; v < 1 {
		t.Fatalf("runs_total = %v", v)
	}
}

func TestSubsystemRunNowSuccess(t *testing.T) {
	f := newSub(t, time.Hour)
	f.start(t)
	run, err := f.sub.RunNow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !run.OK || run.Error != "" || run.ChunksUploaded != 3 || run.ChunksSkipped != 0 || run.BytesUploaded != 6 {
		t.Fatalf("run = %+v", run)
	}
	if run.Manifest != "manifests/node-a/"+stamp0+".json" || !f.tgt.has(run.Manifest) {
		t.Fatalf("manifest = %q", run.Manifest)
	}
	st := f.sub.Status()
	if st.Running || st.LastRun == nil || *st.LastRun != run {
		t.Fatalf("status = %+v", st)
	}

	m := metricValues(f)
	want := map[string]float64{
		"runs_total": 1, "failures_total": 0, "last_chunks": 3,
		"last_success_timestamp_seconds": float64(t0.Unix()), "last_duration_seconds": 0,
		"chunks_uploaded_total": 3, "chunks_skipped_total": 0, "bytes_uploaded_total": 6,
		"pruned_objects_total": 0, "prune_failures_total": 0,
	}
	for name, v := range want {
		got, ok := m[name]
		if !ok || got != v {
			t.Errorf("metric %s = %v (present %v), want %v", name, got, ok, v)
		}
	}
	if len(m) != len(want) {
		t.Errorf("unexpected metric set: %v", m)
	}
}

func TestSubsystemRunNowFailure(t *testing.T) {
	f := newSub(t, time.Hour)
	f.chunks.listErr = errors.New("disk on fire")
	f.start(t)
	run, err := f.sub.RunNow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if run.OK || !strings.Contains(run.Error, "siloctl backup run") || !strings.Contains(run.Error, "mem://test") {
		t.Fatalf("run = %+v", run)
	}
	m := metricValues(f)
	if m["runs_total"] != 1 || m["failures_total"] != 1 || m["last_success_timestamp_seconds"] != 0 || m["chunks_uploaded_total"] != 0 {
		t.Fatalf("metrics = %v", m)
	}
	if f.sub.Status().LastRun.OK {
		t.Fatal("status reports success")
	}
}

func TestSubsystemSecondRunSkips(t *testing.T) {
	f := newSub(t, time.Hour)
	f.start(t)
	if _, err := f.sub.RunNow(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(time.Second)
	run, err := f.sub.RunNow(context.Background())
	if err != nil || run.ChunksUploaded != 0 || run.ChunksSkipped != 3 {
		t.Fatalf("run = %+v err = %v", run, err)
	}
	if m := metricValues(f); m["chunks_skipped_total"] != 3 || m["chunks_uploaded_total"] != 3 || m["runs_total"] != 2 {
		t.Fatalf("metrics = %v", m)
	}
}

// blockChunkPuts makes chunk uploads wait for release (or for the run's context).
func blockChunkPuts(f *subFixture) (release func()) {
	gate := make(chan struct{})
	var once sync.Once
	f.tgt.setPutHook(func(ctx context.Context, name string) error {
		if !strings.HasPrefix(name, "chunks/") {
			return nil
		}
		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	return func() { once.Do(func() { close(gate) }) }
}

func TestSubsystemRunInProgress(t *testing.T) {
	f := newSub(t, time.Hour)
	f.start(t)
	release := blockChunkPuts(f)
	defer release()

	first := make(chan backup.Run, 1)
	go func() {
		run, err := f.sub.RunNow(context.Background())
		if err != nil {
			t.Errorf("first RunNow: %v", err)
		}
		first <- run
	}()
	eventually(t, "the run to start", func() bool { return f.sub.Status().Running })

	if _, err := f.sub.RunNow(context.Background()); !errors.Is(err, backup.ErrRunInProgress) {
		t.Fatalf("second RunNow err = %v, want ErrRunInProgress", err)
	}
	release()
	if run := <-first; !run.OK {
		t.Fatalf("first run = %+v", run)
	}
	if f.sub.Status().Running {
		t.Fatal("still running after the run finished")
	}
}

func TestSubsystemScheduledTickSkippedDuringManualRun(t *testing.T) {
	f := newSub(t, 5*time.Millisecond)
	release := blockChunkPuts(f)
	defer release()

	first := make(chan backup.Run, 1)
	go func() {
		run, _ := f.sub.RunNow(context.Background())
		first <- run
	}()
	eventually(t, "the manual run to start", func() bool { return f.sub.Status().Running })
	stop := f.start(t)

	// Several ticks fire while the manual run holds the claim.
	time.Sleep(50 * time.Millisecond)
	if v := metricValues(f)["runs_total"]; v != 1 {
		t.Fatalf("runs_total = %v while a manual run was in flight, want 1", v)
	}
	release()
	<-first
	stop()
}

func TestSubsystemRunNowContextExpires(t *testing.T) {
	f := newSub(t, time.Hour)
	f.start(t)
	release := blockChunkPuts(f)
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := f.sub.RunNow(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	release()
	eventually(t, "the abandoned run to finish", func() bool { return f.sub.Status().LastRun != nil })
	if !f.sub.Status().LastRun.OK {
		t.Fatalf("last run = %+v", f.sub.Status().LastRun)
	}
}

func TestSubsystemShutdownInterruptsRun(t *testing.T) {
	f := newSub(t, time.Hour)
	stop := f.start(t)
	blockChunkPuts(f)

	got := make(chan backup.Run, 1)
	go func() {
		run, _ := f.sub.RunNow(context.Background())
		got <- run
	}()
	eventually(t, "the run to start", func() bool { return f.sub.Status().Running })
	stop()

	run := <-got
	if run.OK || !strings.Contains(run.Error, "shutting down") {
		t.Fatalf("run = %+v", run)
	}
	if _, err := f.sub.RunNow(context.Background()); !errors.Is(err, backup.ErrStopped) {
		t.Fatalf("RunNow after Shutdown err = %v", err)
	}
	stop() // second call is a no-op
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := f.sub.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func TestSubsystemShutdownDeadline(t *testing.T) {
	f := newSub(t, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := f.sub.Shutdown(ctx)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "shutdown deadline") {
		t.Fatalf("err = %v", err)
	}
	if _, err := f.sub.RunNow(context.Background()); !errors.Is(err, backup.ErrStopped) {
		t.Fatalf("RunNow err = %v", err)
	}
}

// runScheduled lets the schedule fire exactly n times and returns the metrics
// as they stood once run n had finished, retention included. Run n+1 is held
// at its marker write until shutdown, which keeps it from touching the target
// and from being counted as a failure in the returned snapshot (runs_total
// includes it). afterManifest, when set, is called with k just before run k
// writes latest.json, so a test can change the clock or the local chunks for
// the runs after it.
func (f *subFixture) runScheduled(t *testing.T, n int, afterManifest func(k int)) map[string]float64 {
	t.Helper()
	var (
		markers, manifests int
		mu                 sync.Mutex
		once               sync.Once
		reached            = make(chan struct{})
	)
	f.tgt.setPutHook(func(ctx context.Context, name string) error {
		switch {
		case strings.HasPrefix(name, "runs/"):
			mu.Lock()
			markers++
			hold := markers > n
			mu.Unlock()
			if hold {
				once.Do(func() { close(reached) })
				<-ctx.Done()
				return ctx.Err()
			}
		case name == "manifests/node-a/latest.json" && afterManifest != nil:
			mu.Lock()
			manifests++
			k := manifests
			mu.Unlock()
			afterManifest(k)
		}
		return nil
	})
	stop := f.start(t)
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %d scheduled runs; logs:\n%s", n, f.logs.String())
	}
	snapshot := metricValues(f)
	f.lastRun = f.sub.Status().LastRun
	stop()
	return snapshot
}

func TestSubsystemRetention(t *testing.T) {
	manifests := func(f *subFixture) int { return len(f.tgt.names("manifests/node-a/20")) }
	hourly := func(f *subFixture) func(int) { return func(int) { f.clock.Advance(time.Hour) } }
	t.Run("negative keeps the default", func(t *testing.T) {
		f := newSub(t, time.Millisecond, backup.WithRetain(-1), backup.WithRetainFor(0))
		m := f.runScheduled(t, backup.DefaultRetain+1, hourly(f))
		if got := manifests(f); got != backup.DefaultRetain {
			t.Fatalf("manifests = %d, want %d", got, backup.DefaultRetain)
		}
		if m["pruned_objects_total"] < 1 {
			t.Fatalf("metrics = %v", m)
		}
	})
	t.Run("zero disables pruning", func(t *testing.T) {
		f := newSub(t, time.Millisecond, backup.WithRetain(0), backup.WithRetainFor(0))
		f.runScheduled(t, 3, hourly(f))
		if got := manifests(f); got != 3 {
			t.Fatalf("manifests = %d, want 3", got)
		}
	})
	t.Run("explicit retain", func(t *testing.T) {
		f := newSub(t, time.Millisecond, backup.WithRetain(2), backup.WithRetainFor(0))
		f.runScheduled(t, 4, hourly(f))
		if got := manifests(f); got != 2 {
			t.Fatalf("manifests = %d, want 2", got)
		}
	})
	t.Run("the default age floor keeps recent manifests", func(t *testing.T) {
		f := newSub(t, time.Millisecond, backup.WithRetain(1))
		m := f.runScheduled(t, 3, hourly(f))
		if got := manifests(f); got != 3 {
			t.Fatalf("manifests = %d, want all 3 (they are hours old, the floor is days)", got)
		}
		if m["pruned_objects_total"] != 0 {
			t.Fatalf("metrics = %v", m)
		}
	})
}

func TestSubsystemRunNowNeverPrunes(t *testing.T) {
	f := newSub(t, time.Hour, backup.WithRetain(1), backup.WithRetainFor(0))
	f.start(t)
	for range 2 {
		f.clock.Advance(100 * time.Hour)
		if run, err := f.sub.RunNow(context.Background()); err != nil || !run.OK {
			t.Fatalf("run = %+v err = %v", run, err)
		}
	}
	if got := f.tgt.names("manifests/node-a/20"); len(got) != 2 {
		t.Fatalf("manifests = %v, want both runs kept", got)
	}
	if len(f.tgt.deletedWith("manifests/")) != 0 || len(f.tgt.deletedWith("namespace/")) != 0 {
		t.Fatalf("a manual run deleted %v", f.tgt.deletes)
	}
	if m := metricValues(f); m["pruned_objects_total"] != 0 {
		t.Fatalf("metrics = %v", m)
	}
}

func TestSubsystemRetainFor(t *testing.T) {
	const (
		oneDayOld     = "2026-10-27T12:00:00.000Z"
		twentyDaysOld = "2026-10-08T12:00:00.000Z"
	)
	day := 24 * time.Hour
	cases := []struct {
		name       string
		opts       []backup.SubsystemOption
		wantKept   bool // the one-day-old manifest
		wantKept20 bool
	}{
		{"default is seven days", nil, true, false},
		{"negative keeps the default", []backup.SubsystemOption{backup.WithRetainFor(-time.Hour)}, true, false},
		{"zero disables the floor", []backup.SubsystemOption{backup.WithRetainFor(0)}, false, false},
		{"thirty days keeps both", []backup.SubsystemOption{backup.WithRetainFor(30 * day)}, true, true},
		{"half a day keeps neither", []backup.SubsystemOption{backup.WithRetainFor(12 * time.Hour)}, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newSub(t, time.Millisecond, append([]backup.SubsystemOption{backup.WithRetain(1)}, c.opts...)...)
			putManifest(t, f.tgt, testNode, oneDayOld, nil, t0)
			putManifest(t, f.tgt, testNode, twentyDaysOld, nil, t0)
			f.runScheduled(t, 1, nil)
			if got := f.tgt.has(backup.ManifestKey(testNode, oneDayOld)); got != c.wantKept {
				t.Errorf("one-day-old manifest kept = %v, want %v", got, c.wantKept)
			}
			if got := f.tgt.has(backup.ManifestKey(testNode, twentyDaysOld)); got != c.wantKept20 {
				t.Errorf("twenty-day-old manifest kept = %v, want %v", got, c.wantKept20)
			}
			if !f.tgt.has(backup.ManifestKey(testNode, stamp0)) {
				t.Error("the new manifest must always be kept")
			}
		})
	}
}

func TestSubsystemPrunesAfterSuccess(t *testing.T) {
	f := newSub(t, time.Millisecond, backup.WithRetain(1), backup.WithRetainFor(0))
	// c2 disappears locally after the first run; once that run is older than
	// the grace window, its manifest, namespace snapshot and c2 are garbage.
	m := f.runScheduled(t, 2, func(k int) {
		if k == 1 {
			f.chunks.drop("c2")
			f.clock.Advance(100 * time.Hour)
		}
	})
	for _, gone := range []string{"manifests/node-a/" + stamp0 + ".json", "namespace/node-a/" + stamp0 + ".json", "chunks/c2"} {
		if f.tgt.has(gone) {
			t.Errorf("%s should have been pruned", gone)
		}
	}
	if !f.tgt.has("chunks/c1") || !f.tgt.has("chunks/c3") {
		t.Error("referenced chunks were pruned")
	}
	if m["pruned_objects_total"] != 3 || m["prune_failures_total"] != 0 {
		t.Fatalf("metrics = %v", m)
	}
}

func TestSubsystemPruneFailuresDoNotFailTheRun(t *testing.T) {
	boom := errors.New("boom")
	t.Run("delete failure", func(t *testing.T) {
		f := newSub(t, time.Millisecond, backup.WithRetain(1), backup.WithRetainFor(0))
		f.tgt.failDelete("chunks/c2", boom)
		m := f.runScheduled(t, 2, func(k int) {
			if k == 1 {
				f.chunks.drop("c2")
				f.clock.Advance(100 * time.Hour)
			}
		})
		if m["prune_failures_total"] != 1 || m["failures_total"] != 0 {
			t.Fatalf("metrics = %v", m)
		}
		if !strings.Contains(f.logs.String(), "retention skipped some objects") {
			t.Fatalf("logs do not mention the skipped objects:\n%s", f.logs.String())
		}
	})
	t.Run("pass cannot start", func(t *testing.T) {
		f := newSub(t, time.Millisecond, backup.WithRetain(1))
		f.tgt.failList("manifests/node-a/", boom)
		m := f.runScheduled(t, 1, nil)
		if m["prune_failures_total"] != 1 || m["failures_total"] != 0 {
			t.Fatalf("metrics = %v", m)
		}
		if f.lastRun == nil || !f.lastRun.OK {
			t.Fatalf("last run = %+v", f.lastRun)
		}
	})
	t.Run("listing lags behind the new manifest", func(t *testing.T) {
		f := newSub(t, time.Millisecond, backup.WithRetain(1), backup.WithRetainFor(0))
		f.tgt.hide(backup.ManifestKey(testNode, stamp0))
		m := f.runScheduled(t, 1, nil)
		if m["prune_failures_total"] != 1 || m["failures_total"] != 0 {
			t.Fatalf("metrics = %v", m)
		}
		if len(f.tgt.deletedWith("manifests/")) != 0 {
			t.Fatalf("deleted %v despite the incomplete listing", f.tgt.deletes)
		}
		if !strings.Contains(f.logs.String(), "retention could not run") {
			t.Fatalf("logs:\n%s", f.logs.String())
		}
	})
}

func TestSubsystemLogsChunksHeldBack(t *testing.T) {
	f := newSub(t, time.Millisecond, backup.WithRetain(1))
	// An unreferenced, old chunk would be deleted, but another node is
	// mid-run, so retention waits.
	f.tgt.set("chunks/orphan", []byte("x"), t0.Add(-100*time.Hour))
	f.tgt.set("runs/node-b/2026-10-28T11:59:00.000Z.json", []byte("{}"), t0)
	m := f.runScheduled(t, 1, nil)
	if !f.tgt.has("chunks/orphan") {
		t.Fatal("chunk deleted while another node had a run in progress")
	}
	if m["prune_failures_total"] != 0 {
		t.Fatalf("metrics = %v", m)
	}
	if logs := f.logs.String(); !strings.Contains(logs, "kept unreferenced chunks") || !strings.Contains(logs, "node-b") {
		t.Fatalf("logs do not explain the held-back chunks:\n%s", logs)
	}
}
