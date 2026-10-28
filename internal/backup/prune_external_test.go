package backup_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/backup"
)

var (
	pruneNow = time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	old      = time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	young    = pruneNow.Add(-time.Hour)
)

func pruneOpts() backup.PruneOptions {
	return backup.PruneOptions{NodeID: testNode, Retain: 2, Grace: 24 * time.Hour, Now: pruneNow, Concurrency: 2}
}

func TestPruneGrace(t *testing.T) {
	cases := []struct{ in, want time.Duration }{
		{time.Hour, 24 * time.Hour},
		{24 * time.Hour, 48 * time.Hour},
		{12 * time.Hour, 24 * time.Hour},
		{0, 24 * time.Hour},
	}
	for _, c := range cases {
		if got := backup.PruneGrace(c.in); got != c.want {
			t.Errorf("PruneGrace(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestPruneRejectsBadOptions(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*backup.PruneOptions)
		wantIs error
		want   string
	}{
		{"retain zero", func(o *backup.PruneOptions) { o.Retain = 0 }, nil, "SILO_BACKUP_RETAIN"},
		{"retain negative", func(o *backup.PruneOptions) { o.Retain = -1 }, nil, "at least one"},
		{"bad node", func(o *backup.PruneOptions) { o.NodeID = "../x" }, backup.ErrInvalidNodeID, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tgt := newMemTarget()
			opts := pruneOpts()
			c.mutate(&opts)
			_, err := backup.Prune(context.Background(), tgt, opts)
			if err == nil {
				t.Fatal("Prune succeeded, want an error")
			}
			if c.wantIs != nil && !errors.Is(err, c.wantIs) {
				t.Fatalf("err = %v, want %v", err, c.wantIs)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err %q does not contain %q", err, c.want)
			}
		})
	}
}

// seedScenario builds a target with four runs of node-a, one of node-b and a
// spread of chunks and orphan objects.
func seedScenario(t *testing.T) *memTarget {
	t.Helper()
	tgt := newMemTarget()
	const (
		s1 = "2026-10-25T12:00:00.000Z"
		s2 = "2026-10-26T12:00:00.000Z"
		s3 = "2026-10-27T12:00:00.000Z"
		s4 = "2026-10-28T12:00:00.000Z"
	)
	putManifest(t, tgt, "node-a", s1, []string{"c1", "c2"}, old)
	putManifest(t, tgt, "node-a", s2, []string{"c2", "c3"}, old)
	putManifest(t, tgt, "node-a", s3, []string{"c3", "c4"}, old)
	putManifest(t, tgt, "node-a", s4, []string{"c4", "c5"}, old)
	putManifest(t, tgt, "node-b", s1, []string{"c6"}, old)
	// latest.json references c9, which no timestamped manifest lists.
	tgt.set("manifests/node-a/latest.json", []byte(`{"chunks":[{"id":"c5"},{"id":"c9"}]}`), old)
	tgt.set("manifests/node-b/latest.json", []byte(`{"chunks":[{"id":"c6"}]}`), old)

	for _, id := range []string{"c1", "c2", "c3", "c4", "c5", "c6", "c8", "c9"} {
		putChunk(tgt, id, old)
	}
	putChunk(tgt, "c7", young)

	tgt.set("namespace/node-a/2026-10-29T12:00:00.000Z.json", []byte("{}"), young)
	tgt.set("namespace/node-a/2026-10-20T12:00:00.000Z.json", []byte("{}"), old)
	tgt.set("namespace/node-a.json", []byte("{}"), old)
	tgt.set("extents/node-a/x.json", []byte("{}"), old)
	tgt.set("extents/node-a/legacy/x.json", []byte("{}"), old)
	tgt.set("chunks/nested/x", []byte("x"), old)
	tgt.set("manifests/README.txt", []byte("x"), old)
	return tgt
}

func TestPruneRetention(t *testing.T) {
	tgt := seedScenario(t)
	res, err := backup.Prune(context.Background(), tgt, pruneOpts())
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	// 2 manifests, 2 namespaces + 2 extents of pruned runs, 1 old orphan
	// namespace, chunks c1, c2 and c8.
	if res.ManifestsDeleted != 2 || res.ObjectsDeleted != 8 || res.Failures != 0 || res.FirstError != nil {
		t.Fatalf("result = %+v", res)
	}

	wantGone := []string{
		"manifests/node-a/2026-10-25T12:00:00.000Z.json",
		"manifests/node-a/2026-10-26T12:00:00.000Z.json",
		"namespace/node-a/2026-10-25T12:00:00.000Z.json",
		"namespace/node-a/2026-10-26T12:00:00.000Z.json",
		"extents/node-a/2026-10-25T12:00:00.000Z/dm9s.json",
		"extents/node-a/2026-10-26T12:00:00.000Z/dm9s.json",
		"namespace/node-a/2026-10-20T12:00:00.000Z.json",
		"chunks/c1", "chunks/c2", "chunks/c8",
	}
	for _, n := range wantGone {
		if tgt.has(n) {
			t.Errorf("%s should have been deleted", n)
		}
	}
	wantKept := []string{
		"manifests/node-a/2026-10-27T12:00:00.000Z.json",
		"manifests/node-a/2026-10-28T12:00:00.000Z.json",
		"manifests/node-a/latest.json",
		"manifests/node-b/2026-10-25T12:00:00.000Z.json",
		"manifests/node-b/latest.json",
		"namespace/node-a/2026-10-27T12:00:00.000Z.json",
		"namespace/node-a/2026-10-28T12:00:00.000Z.json",
		"extents/node-a/2026-10-27T12:00:00.000Z/dm9s.json",
		"extents/node-a/2026-10-28T12:00:00.000Z/dm9s.json",
		"namespace/node-b/2026-10-25T12:00:00.000Z.json",
		"extents/node-b/2026-10-25T12:00:00.000Z/dm9s.json",
		"namespace/node-a/2026-10-29T12:00:00.000Z.json", // young orphan
		"namespace/node-a.json",
		"extents/node-a/x.json",
		"extents/node-a/legacy/x.json",
		"chunks/c3", "chunks/c4", "chunks/c5", "chunks/c6",
		"chunks/c7", // young and unreferenced: another node may be mid-run
		"chunks/c9", // referenced only by latest.json
		"chunks/nested/x",
	}
	for _, n := range wantKept {
		if !tgt.has(n) {
			t.Errorf("%s should have been kept", n)
		}
	}
}

func TestPruneNeverTouchesOtherNodes(t *testing.T) {
	tgt := seedScenario(t)
	if _, err := backup.Prune(context.Background(), tgt, pruneOpts()); err != nil {
		t.Fatal(err)
	}
	for _, d := range tgt.deletes {
		if strings.Contains(d, "node-b") || strings.HasSuffix(d, "latest.json") {
			t.Fatalf("deleted %s", d)
		}
	}
}

func TestPruneDefaultConcurrency(t *testing.T) {
	tgt := seedScenario(t)
	opts := pruneOpts()
	opts.Concurrency = 0
	res, err := backup.Prune(context.Background(), tgt, opts)
	if err != nil || res.ObjectsDeleted != 8 {
		t.Fatalf("res = %+v err = %v", res, err)
	}
}

func TestPruneRetainMoreThanExists(t *testing.T) {
	tgt := seedScenario(t)
	opts := pruneOpts()
	opts.Retain = 10
	res, err := backup.Prune(context.Background(), tgt, opts)
	if err != nil || res.ManifestsDeleted != 0 {
		t.Fatalf("res = %+v err = %v", res, err)
	}
}

func TestPruneSkipsChunksWhenManifestsUntrustworthy(t *testing.T) {
	cases := []struct {
		name  string
		setup func(tgt *memTarget)
	}{
		{"manifest cannot be read", func(tgt *memTarget) { tgt.failGet("manifests/node-b/*", errors.New("denied")) }},
		{"manifest is not JSON", func(tgt *memTarget) {
			tgt.set("manifests/node-b/2026-10-25T12:00:00.000Z.json", []byte("{broken"), old)
		}},
		{"manifest listing fails", func(tgt *memTarget) { tgt.failList("manifests/", errors.New("denied")) }},
		{"chunk listing fails", func(tgt *memTarget) { tgt.failList("chunks/", errors.New("denied")) }},
		{"no manifests at all", func(tgt *memTarget) {
			for _, n := range tgt.names("manifests/") {
				tgt.remove(n)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tgt := seedScenario(t)
			c.setup(tgt)
			res, err := backup.Prune(context.Background(), tgt, pruneOpts())
			if err != nil {
				t.Fatalf("Prune: %v", err)
			}
			if res.Failures < 1 || res.FirstError == nil {
				t.Fatalf("result = %+v, want a recorded failure", res)
			}
			for _, d := range tgt.deletes {
				if strings.HasPrefix(d, "chunks/") {
					t.Fatalf("chunk %s deleted despite untrustworthy manifests", d)
				}
			}
		})
	}
}

func TestPruneDeleteFailures(t *testing.T) {
	boom := errors.New("boom")
	t.Run("chunk delete failure does not stop the pass", func(t *testing.T) {
		tgt := seedScenario(t)
		tgt.failDelete("chunks/c1", boom)
		res, err := backup.Prune(context.Background(), tgt, pruneOpts())
		if err != nil {
			t.Fatal(err)
		}
		if res.Failures != 1 || !errors.Is(res.FirstError, boom) || res.ObjectsDeleted != 7 {
			t.Fatalf("result = %+v", res)
		}
		if tgt.has("chunks/c2") {
			t.Fatal("c2 should still have been deleted")
		}
	})
	t.Run("failed manifest delete keeps the run's objects", func(t *testing.T) {
		tgt := seedScenario(t)
		failed := backup.ManifestKey(testNode, "2026-10-25T12:00:00.000Z")
		tgt.failDelete(failed, boom)
		res, err := backup.Prune(context.Background(), tgt, pruneOpts())
		if err != nil {
			t.Fatal(err)
		}
		if res.ManifestsDeleted != 1 || res.Failures != 1 {
			t.Fatalf("result = %+v", res)
		}
		for _, n := range []string{"namespace/node-a/2026-10-25T12:00:00.000Z.json", "extents/node-a/2026-10-25T12:00:00.000Z/dm9s.json", "chunks/c1"} {
			if !tgt.has(n) {
				t.Errorf("%s should have been kept", n)
			}
		}
	})
	t.Run("run-object listing failure is counted", func(t *testing.T) {
		tgt := seedScenario(t)
		tgt.failList("namespace/node-a/", boom)
		res, err := backup.Prune(context.Background(), tgt, pruneOpts())
		if err != nil || res.Failures != 1 || !errors.Is(res.FirstError, boom) {
			t.Fatalf("res = %+v err = %v", res, err)
		}
		if tgt.has("extents/node-a/2026-10-25T12:00:00.000Z/dm9s.json") {
			t.Fatal("extent objects should still be pruned")
		}
	})
}

func TestPruneManifestListFailureReturnsError(t *testing.T) {
	tgt := seedScenario(t)
	tgt.failList("manifests/node-a/", errors.New("denied"))
	if _, err := backup.Prune(context.Background(), tgt, pruneOpts()); err == nil || !strings.Contains(err.Error(), "for pruning") {
		t.Fatalf("err = %v", err)
	}
	if len(tgt.deletes) != 0 {
		t.Fatalf("deleted %v despite the failure", tgt.deletes)
	}
}

func TestPruneIsIdempotent(t *testing.T) {
	tgt := seedScenario(t)
	if _, err := backup.Prune(context.Background(), tgt, pruneOpts()); err != nil {
		t.Fatal(err)
	}
	before := tgt.names("")
	res, err := backup.Prune(context.Background(), tgt, pruneOpts())
	if err != nil || res.ManifestsDeleted+res.ObjectsDeleted != 0 {
		t.Fatalf("second pass: %+v %v", res, err)
	}
	if !reflect.DeepEqual(before, tgt.names("")) {
		t.Fatal("second pass changed the target")
	}
}

const seedS1 = "2026-10-25T12:00:00.000Z"

func TestPruneRefusesWhenListingLacksCurrentStamp(t *testing.T) {
	t.Run("stamp missing", func(t *testing.T) {
		tgt := seedScenario(t)
		opts := pruneOpts()
		opts.CurrentStamp = "2026-10-29T12:00:00.000Z"
		res, err := backup.Prune(context.Background(), tgt, opts)
		if !errors.Is(err, backup.ErrListingIncomplete) {
			t.Fatalf("err = %v, want ErrListingIncomplete", err)
		}
		if !strings.Contains(err.Error(), "retention is skipped") {
			t.Fatalf("err %q does not say what happens next", err)
		}
		if len(tgt.deletes) != 0 || res.ManifestsDeleted+res.ObjectsDeleted != 0 {
			t.Fatalf("deleted %v (result %+v) despite the incomplete listing", tgt.deletes, res)
		}
	})
	t.Run("hidden by a lagging listing", func(t *testing.T) {
		tgt := seedScenario(t)
		tgt.hide(backup.ManifestKey(testNode, "2026-10-28T12:00:00.000Z"))
		opts := pruneOpts()
		opts.CurrentStamp = "2026-10-28T12:00:00.000Z"
		if _, err := backup.Prune(context.Background(), tgt, opts); !errors.Is(err, backup.ErrListingIncomplete) {
			t.Fatalf("err = %v, want ErrListingIncomplete", err)
		}
		if len(tgt.deletes) != 0 {
			t.Fatalf("deleted %v", tgt.deletes)
		}
	})
	t.Run("stamp present", func(t *testing.T) {
		tgt := seedScenario(t)
		opts := pruneOpts()
		opts.CurrentStamp = "2026-10-28T12:00:00.000Z"
		res, err := backup.Prune(context.Background(), tgt, opts)
		if err != nil || res.ManifestsDeleted != 2 {
			t.Fatalf("res = %+v err = %v", res, err)
		}
	})
}

func TestPruneRetainFor(t *testing.T) {
	// pruneNow is 2026-10-30T00:00Z; with Retain 2 only the runs of
	// 10-25 12:00 and 10-26 12:00 are candidates.
	day := 24 * time.Hour
	cases := []struct {
		name      string
		retainFor time.Duration
		deleted   []string
	}{
		{"disabled", 0, []string{"2026-10-25T12:00:00.000Z", "2026-10-26T12:00:00.000Z"}},
		{"negative behaves as disabled", -time.Hour, []string{"2026-10-25T12:00:00.000Z", "2026-10-26T12:00:00.000Z"}},
		{"shorter than both", 3 * day, []string{"2026-10-25T12:00:00.000Z", "2026-10-26T12:00:00.000Z"}},
		{"between the two", 4 * day, []string{"2026-10-25T12:00:00.000Z"}},
		{"exactly the age of the older", 4*day + 12*time.Hour, nil},
		{"longer than both", 30 * day, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tgt := seedScenario(t)
			opts := pruneOpts()
			opts.RetainFor = c.retainFor
			res, err := backup.Prune(context.Background(), tgt, opts)
			if err != nil {
				t.Fatal(err)
			}
			if res.ManifestsDeleted != len(c.deleted) {
				t.Fatalf("ManifestsDeleted = %d, want %d", res.ManifestsDeleted, len(c.deleted))
			}
			for _, s := range []string{"2026-10-25T12:00:00.000Z", "2026-10-26T12:00:00.000Z"} {
				gone := !tgt.has(backup.ManifestKey(testNode, s))
				want := false
				for _, d := range c.deleted {
					want = want || d == s
				}
				if gone != want {
					t.Errorf("manifest %s deleted = %v, want %v", s, gone, want)
				}
			}
			for _, s := range []string{"2026-10-27T12:00:00.000Z", "2026-10-28T12:00:00.000Z"} {
				if !tgt.has(backup.ManifestKey(testNode, s)) {
					t.Errorf("manifest %s inside the retention count was deleted", s)
				}
			}
		})
	}
}

func TestPruneNeverDeletesObjectsWithoutModTime(t *testing.T) {
	tgt := seedScenario(t)
	var zero time.Time
	putChunk(tgt, "c10", zero)
	tgt.set("namespace/node-a/2026-10-21T12:00:00.000Z.json", []byte("{}"), zero)
	tgt.set("extents/node-a/2026-10-21T12:00:00.000Z/dm9s.json", []byte("{}"), zero)
	// The run objects of a pruned manifest are unreferenced but have no age.
	tgt.set("namespace/node-a/"+seedS1+".json", []byte("{}"), zero)
	res, err := backup.Prune(context.Background(), tgt, pruneOpts())
	if err != nil {
		t.Fatal(err)
	}
	if res.Failures != 0 {
		t.Fatalf("result = %+v", res)
	}
	for _, n := range []string{
		"chunks/c10",
		"namespace/node-a/2026-10-21T12:00:00.000Z.json",
		"extents/node-a/2026-10-21T12:00:00.000Z/dm9s.json",
		"namespace/node-a/" + seedS1 + ".json",
	} {
		if !tgt.has(n) {
			t.Errorf("%s has no modification time and must be kept", n)
		}
	}
	if tgt.has("chunks/c8") {
		t.Error("chunks with a known age should still be pruned")
	}
}

func TestPruneActiveRuns(t *testing.T) {
	boom := errors.New("boom")
	marker := func(node string) string { return "runs/" + node + "/2026-10-29T23:00:00.000Z.json" }
	cases := []struct {
		name           string
		setup          func(tgt *memTarget)
		heldBack       string // substring of ChunksHeldBack; empty means chunks are pruned
		wantFailures   int
		wantMarkerGone string
		wantObjects    int
	}{
		{name: "no markers", wantObjects: 8},
		{name: "another node's fresh marker", setup: func(tgt *memTarget) {
			tgt.set(marker("node-b"), []byte("{}"), pruneNow.Add(-10*time.Minute))
		}, heldBack: "node-b", wantObjects: 5},
		{name: "marker exactly at the stale limit is still fresh", setup: func(tgt *memTarget) {
			tgt.set(marker("node-b"), []byte("{}"), pruneNow.Add(-time.Hour))
		}, heldBack: "node-b", wantObjects: 5},
		{name: "marker without a modification time counts as fresh", setup: func(tgt *memTarget) {
			tgt.set(marker("node-b"), []byte("{}"), time.Time{})
		}, heldBack: "node-b", wantObjects: 5},
		{name: "one fresh marker among stale ones", setup: func(tgt *memTarget) {
			tgt.set(marker("node-b"), []byte("{}"), pruneNow.Add(-3*time.Hour))
			tgt.set(marker("node-c"), []byte("{}"), pruneNow.Add(-time.Minute))
		}, heldBack: "node-c", wantMarkerGone: marker("node-b"), wantObjects: 6},
		{name: "another node's stale marker is deleted", setup: func(tgt *memTarget) {
			tgt.set(marker("node-b"), []byte("{}"), pruneNow.Add(-2*time.Hour))
		}, wantMarkerGone: marker("node-b"), wantObjects: 9},
		{name: "own fresh marker is ignored", setup: func(tgt *memTarget) {
			tgt.set(marker(testNode), []byte("{}"), pruneNow.Add(-time.Minute))
		}, wantObjects: 8},
		{name: "own stale marker is left alone", setup: func(tgt *memTarget) {
			tgt.set(marker(testNode), []byte("{}"), pruneNow.Add(-5*time.Hour))
		}, wantObjects: 8},
		{
			name: "marker listing fails", setup: func(tgt *memTarget) { tgt.failList("runs/", boom) },
			heldBack: "run markers", wantFailures: 1, wantObjects: 5,
		},
		{name: "stale marker delete fails", setup: func(tgt *memTarget) {
			tgt.set(marker("node-b"), []byte("{}"), pruneNow.Add(-2*time.Hour))
			tgt.failDelete(marker("node-b"), boom)
		}, wantFailures: 1, wantObjects: 8},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tgt := seedScenario(t)
			if c.setup != nil {
				c.setup(tgt)
			}
			res, err := backup.Prune(context.Background(), tgt, pruneOpts())
			if err != nil {
				t.Fatal(err)
			}
			if res.ManifestsDeleted != 2 || res.Failures != c.wantFailures || res.ObjectsDeleted != c.wantObjects {
				t.Fatalf("result = %+v, want 2 manifests, %d objects, %d failures", res, c.wantObjects, c.wantFailures)
			}
			if c.wantFailures > 0 && !errors.Is(res.FirstError, boom) {
				t.Fatalf("FirstError = %v", res.FirstError)
			}
			chunksDeleted := tgt.deletedWith("chunks/")
			if c.heldBack != "" {
				if !strings.Contains(res.ChunksHeldBack, c.heldBack) {
					t.Fatalf("ChunksHeldBack = %q, want it to mention %q", res.ChunksHeldBack, c.heldBack)
				}
				if len(chunksDeleted) != 0 {
					t.Fatalf("chunks %v deleted while held back", chunksDeleted)
				}
				for _, id := range []string{"c1", "c2", "c8"} {
					if !tgt.has("chunks/" + id) {
						t.Errorf("chunk %s should have survived", id)
					}
				}
			} else {
				if res.ChunksHeldBack != "" {
					t.Fatalf("ChunksHeldBack = %q, want empty", res.ChunksHeldBack)
				}
				if len(chunksDeleted) != 3 {
					t.Fatalf("chunks deleted = %v", chunksDeleted)
				}
			}
			if c.wantMarkerGone != "" && tgt.has(c.wantMarkerGone) {
				t.Errorf("stale marker %s should have been deleted", c.wantMarkerGone)
			}
			if got := tgt.has(marker(testNode)); strings.Contains(c.name, "own") && !got {
				t.Errorf("own marker was deleted by prune")
			}
		})
	}
}

func TestPruneDoesNotLookAtRunsWithoutDoomedChunks(t *testing.T) {
	tgt := seedScenario(t)
	for _, id := range []string{"c1", "c2", "c8"} {
		tgt.remove("chunks/" + id)
	}
	tgt.failList("runs/", errors.New("must not be listed"))
	res, err := backup.Prune(context.Background(), tgt, pruneOpts())
	if err != nil || res.Failures != 0 || res.ChunksHeldBack != "" {
		t.Fatalf("res = %+v err = %v", res, err)
	}
}

func TestPruneChunksHeldBackWhenManifestsUntrustworthy(t *testing.T) {
	tgt := seedScenario(t)
	tgt.failList("chunks/", errors.New("denied"))
	res, err := backup.Prune(context.Background(), tgt, pruneOpts())
	if err != nil || res.ChunksHeldBack == "" {
		t.Fatalf("res = %+v err = %v", res, err)
	}
}
