package clusterstatus

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/diskusage"
	"github.com/hyperized/silo/internal/membership"
)

type noMembers struct{}

func (noMembers) Members() []membership.Node { return nil }

type noChunks struct{}

func (noChunks) List(context.Context) ([]string, error) { return nil, nil }

func quietBuilder() *Builder {
	return New(noMembers{}, noChunks{}, "/d", "n", "v", slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithDiskUsage(func(string) (diskusage.Usage, error) { return diskusage.Usage{}, nil }))
}

// fakeClock is advanced by hand so the cache window is deterministic.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

func (c *fakeClock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func errorText(t *testing.T, body []byte) string {
	t.Helper()
	var e struct{ Error string }
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("body %q is not a JSON error: %v", body, err)
	}
	return e.Error
}

func TestSnapshotCache_Window(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	var builds atomic.Int32
	c := &snapshotCache{ttl: 2 * time.Second, now: clk.now, build: func(context.Context) ([]byte, error) {
		builds.Add(1)
		return []byte("x"), nil
	}}
	for _, tc := range []struct {
		name       string
		advance    time.Duration
		wantBuilds int32
	}{
		{name: "first request builds", wantBuilds: 1},
		{name: "inside the window reuses", advance: 1900 * time.Millisecond, wantBuilds: 1},
		{name: "at the window edge rebuilds", advance: 100 * time.Millisecond, wantBuilds: 2},
		{name: "fresh again", advance: time.Second, wantBuilds: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk.advance(tc.advance)
			body, err := c.get(context.Background())
			if err != nil || string(body) != "x\n" {
				t.Fatalf("get = (%q, %v)", body, err)
			}
			if got := builds.Load(); got != tc.wantBuilds {
				t.Errorf("builds = %d, want %d", got, tc.wantBuilds)
			}
		})
	}
}

func TestSnapshotCache_BurstBuildsOnce(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	release := make(chan struct{})
	var builds atomic.Int32
	c := &snapshotCache{ttl: 2 * time.Second, now: clk.now, build: func(context.Context) ([]byte, error) {
		builds.Add(1)
		<-release
		return []byte("x"), nil
	}}

	const callers = 32
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body, err := c.get(context.Background())
			if err == nil && string(body) != "x\n" {
				err = errors.New("unexpected body " + string(body))
			}
			errs <- err
		}()
	}
	// The clock never moves, so a caller arriving after the build finished
	// hits the cache: the count is one whatever the scheduling.
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	if got := builds.Load(); got != 1 {
		t.Errorf("builds = %d, want 1 for a burst", got)
	}
}

func TestSnapshotCache_ErrorsAreNotCached(t *testing.T) {
	var builds atomic.Int32
	c := &snapshotCache{ttl: time.Hour, now: time.Now, build: func(context.Context) ([]byte, error) {
		builds.Add(1)
		return nil, errors.New("boom")
	}}
	for range 2 {
		if _, err := c.get(context.Background()); err == nil {
			t.Fatal("get should return the build error")
		}
	}
	if got := builds.Load(); got != 2 {
		t.Errorf("builds = %d, want 2 (a failure must not be served from cache)", got)
	}
}

func TestSnapshotCache_WaiterGivesUp(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	c := &snapshotCache{ttl: time.Hour, now: time.Now, build: func(context.Context) ([]byte, error) {
		<-release
		return []byte("x"), nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.get(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("get with a cancelled context = %v, want context.Canceled", err)
	}
}

func testConfig() handlerConfig {
	return handlerConfig{ttl: time.Hour, timeout: 5 * time.Second, maxInFlight: 8, now: time.Now}
}

func TestHandler_EncodeFailure(t *testing.T) {
	prev := marshalJSON
	t.Cleanup(func() { marshalJSON = prev })
	marshalJSON = func(proto.Message) ([]byte, error) { return nil, errors.New("boom") }

	rec := httptest.NewRecorder()
	newHandler(quietBuilder(), testConfig()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500", rec.Code)
	}
	if msg := errorText(t, rec.Body.Bytes()); !strings.Contains(msg, "logs") {
		t.Errorf("error = %q, want an instruction pointing at the logs", msg)
	}
}

func TestHandler_Timeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	h := &statusHandler{cache: &snapshotCache{ttl: time.Hour, now: time.Now, build: func(context.Context) ([]byte, error) {
		<-release
		return []byte("{}"), nil
	}}}
	l := &limiter{slots: make(chan struct{}, 1), next: http.TimeoutHandler(h, 20*time.Millisecond, msgTimeout)}

	rec := httptest.NewRecorder()
	l.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
	if msg := errorText(t, rec.Body.Bytes()); !strings.Contains(msg, "retry") {
		t.Errorf("error = %q, want a retry instruction", msg)
	}
}

func TestHandler_RejectsWhenBusy(t *testing.T) {
	l := &limiter{slots: make(chan struct{}, 2), next: http.NotFoundHandler()}
	l.slots <- struct{}{}
	l.slots <- struct{}{}
	rec := httptest.NewRecorder()
	l.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("got %d Retry-After=%q, want 503 with Retry-After: 1", rec.Code, rec.Header().Get("Retry-After"))
	}
	if msg := errorText(t, rec.Body.Bytes()); !strings.Contains(msg, "retry") {
		t.Errorf("error = %q, want a retry instruction", msg)
	}
	// A freed slot lets the next request through.
	<-l.slots
	rec = httptest.NewRecorder()
	l.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("code after a slot frees = %d, want the wrapped handler's 404", rec.Code)
	}
}

func TestHandler_ClientGoneWritesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := httptest.NewRecorder()
	h := &statusHandler{cache: &snapshotCache{ttl: time.Hour, now: time.Now, build: func(context.Context) ([]byte, error) {
		return []byte("{}"), nil
	}}}
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want nothing written for a departed client", rec.Body.String())
	}
}

func TestProjected_BlanksDataDir(t *testing.T) {
	prev := marshalJSON
	t.Cleanup(func() { marshalJSON = prev })
	var seen *statusv1.GetStatusResponse
	marshalJSON = func(m proto.Message) ([]byte, error) {
		seen, _ = m.(*statusv1.GetStatusResponse)
		return []byte("{}"), nil
	}
	b := quietBuilder()
	if _, err := projected(b)(context.Background()); err != nil {
		t.Fatalf("projected: %v", err)
	}
	if seen.GetStorage().GetDataDir() != "" {
		t.Errorf("projected dataDir = %q, want empty", seen.GetStorage().GetDataDir())
	}
	if got := b.Build(context.Background()).GetStorage().GetDataDir(); got != "/d" {
		t.Errorf("Build dataDir = %q after projecting; the projection must not mutate the source", got)
	}
}

func TestProtoNodeState(t *testing.T) {
	for in, want := range map[membership.State]statusv1.NodeState{
		membership.StateAlive:   statusv1.NodeState_NODE_STATE_ALIVE,
		membership.StateSuspect: statusv1.NodeState_NODE_STATE_SUSPECT,
		membership.StateDead:    statusv1.NodeState_NODE_STATE_DEAD,
		membership.StateLeft:    statusv1.NodeState_NODE_STATE_LEFT,
		membership.State(99):    statusv1.NodeState_NODE_STATE_UNSPECIFIED,
	} {
		if got := protoNodeState(in); got != want {
			t.Errorf("protoNodeState(%v) = %v, want %v", in, got, want)
		}
	}
}

type fakeBackupReporter struct{ st backup.Status }

func (f fakeBackupReporter) Status() backup.Status { return f.st }

func TestBackupStatus(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	next := time.Unix(1_700_021_600, 0)
	run := &backup.Run{
		StartedAt: started, FinishedAt: started.Add(time.Minute), OK: false, Error: "target unreachable",
		ChunksUploaded: 5, ChunksSkipped: 3, BytesUploaded: 8192, Manifest: "silo-a/manifest.json",
	}
	cases := []struct {
		name     string
		reporter BackupReporter
		check    func(t *testing.T, b *statusv1.BackupStatus)
	}{
		{"no reporter means not configured", nil, func(t *testing.T, b *statusv1.BackupStatus) {
			if b == nil || b.GetConfigured() || b.GetTarget() != "" || b.GetLastRun() != nil || b.GetNextRunUnix() != 0 {
				t.Errorf("backup = %+v, want zero value with configured=false", b)
			}
		}},
		{"configured, never ran, nothing scheduled", fakeBackupReporter{backup.Status{
			Configured: true, Target: "s3://bucket/p", Interval: 6 * time.Hour,
		}}, func(t *testing.T, b *statusv1.BackupStatus) {
			if !b.GetConfigured() || b.GetTarget() != "s3://bucket/p" || b.GetIntervalSeconds() != 21600 || b.GetRunning() {
				t.Errorf("backup = %+v", b)
			}
			if b.GetNextRunUnix() != 0 || b.GetLastRun() != nil {
				t.Errorf("next=%d last=%v, want 0 and nil", b.GetNextRunUnix(), b.GetLastRun())
			}
		}},
		{"running with schedule and last run", fakeBackupReporter{backup.Status{
			Configured: true, Target: "gs://b", Interval: time.Hour, Running: true, NextRun: next, LastRun: run,
		}}, func(t *testing.T, b *statusv1.BackupStatus) {
			if !b.GetRunning() || b.GetIntervalSeconds() != 3600 || b.GetNextRunUnix() != next.Unix() {
				t.Errorf("backup = %+v", b)
			}
			r := b.GetLastRun()
			if r.GetStartedUnix() != started.Unix() || r.GetFinishedUnix() != started.Add(time.Minute).Unix() ||
				r.GetOk() || r.GetError() != "target unreachable" || r.GetChunksUploaded() != 5 ||
				r.GetChunksSkipped() != 3 || r.GetBytesUploaded() != 8192 || r.GetManifest() != "silo-a/manifest.json" {
				t.Errorf("last run = %+v", r)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.check(t, backupStatus(tc.reporter))
			b := quietBuilder()
			WithBackup(tc.reporter)(b)
			tc.check(t, b.Build(context.Background()).GetBackup())
		})
	}
}

func TestProjected_KeepsBackup(t *testing.T) {
	b := quietBuilder()
	WithBackup(fakeBackupReporter{backup.Status{Configured: true, Target: "s3://bucket/p", Interval: time.Hour}})(b)
	raw, err := projected(b)(context.Background())
	if err != nil {
		t.Fatalf("projected: %v", err)
	}
	var body struct {
		Backup struct {
			Configured bool   `json:"configured"`
			Target     string `json:"target"`
		} `json:"backup"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if !body.Backup.Configured || body.Backup.Target != "s3://bucket/p" {
		t.Errorf("projected backup = %+v, want it passed through untouched", body.Backup)
	}
}
