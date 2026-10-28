package backup_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/chunkstore"
)

func chunkIDs(refs []backup.ChunkRef) []string {
	ids := make([]string, len(refs))
	for i, r := range refs {
		ids[i] = r.ID
	}
	return ids
}

func newTestExporter(chunks backup.ChunkSource, opts ...backup.ExporterOption) *backup.Exporter {
	base := []backup.ExporterOption{backup.WithClock(newClock(t0, 0).Now)}
	return backup.NewExporter(chunks, fakeNS{snap: []byte(`{"ns":1}`)}, testNode, append(base, opts...)...)
}

func TestExportFirstRun(t *testing.T) {
	tgt := newMemTarget()
	e := backup.NewExporter(
		threeChunks(), fakeNS{snap: []byte(`{"ns":1}`)}, testNode,
		backup.WithClock(newClock(t0, 2_000_000_000).Now),
		backup.WithVersion("v9.9.9"),
		backup.WithExtentSource(twoVolumes()),
	)
	res := export(t, e, tgt)

	manifestKey := "manifests/node-a/" + stamp0 + ".json"
	if res.Manifest != manifestKey {
		t.Fatalf("Manifest = %q, want %q", res.Manifest, manifestKey)
	}
	if res.Chunks != 3 || res.ChunkBytes != 6 || res.Uploaded != 3 || res.Skipped != 0 || res.BytesUploaded != 6 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if !res.StartedAt.Equal(t0) || !res.FinishedAt.After(res.StartedAt) {
		t.Fatalf("unexpected times: %v .. %v", res.StartedAt, res.FinishedAt)
	}

	if got := string(tgt.bytesOf("namespace/node-a/" + stamp0 + ".json")); got != `{"ns":1}` {
		t.Fatalf("namespace object = %q", got)
	}
	for vol := range twoVolumes().vols {
		key := "extents/node-a/" + stamp0 + "/" + base64.RawURLEncoding.EncodeToString([]byte(vol)) + ".json"
		if !tgt.has(key) {
			t.Fatalf("missing extent object %s", key)
		}
		if !strings.Contains(string(tgt.bytesOf(key)), fmt.Sprintf("%q", vol)) {
			t.Fatalf("extent object %s does not name volume %q", key, vol)
		}
	}
	for id, want := range map[string]string{"c1": "aaa", "c2": "bb", "c3": "c"} {
		if got := string(tgt.bytesOf("chunks/" + id)); got != want {
			t.Fatalf("chunk %s = %q, want %q", id, got, want)
		}
	}

	raw := tgt.bytesOf(manifestKey)
	if !bytes.Equal(raw, tgt.bytesOf("manifests/node-a/latest.json")) {
		t.Fatal("latest.json differs from the timestamped manifest")
	}
	m := decodeManifest(t, raw)
	if m.Format != 1 || m.SiloVersion != "v9.9.9" || m.NodeID != testNode {
		t.Fatalf("header wrong: %+v", m)
	}
	if !m.StartedAt.Equal(t0) || !m.FinishedAt.Equal(res.FinishedAt) {
		t.Fatalf("times wrong: %v %v", m.StartedAt, m.FinishedAt)
	}
	wantNS := backup.ObjectRef{Key: "namespace/node-a/" + stamp0 + ".json", Size: int64(len(`{"ns":1}`)), SHA256: sha256Of([]byte(`{"ns":1}`))}
	if m.Namespace != wantNS {
		t.Fatalf("namespace = %+v, want %+v", m.Namespace, wantNS)
	}
	var wantExt []backup.ObjectRef
	for _, vol := range []string{"vol-2", "vol/1"} {
		key := "extents/node-a/" + stamp0 + "/" + base64.RawURLEncoding.EncodeToString([]byte(vol)) + ".json"
		body := tgt.bytesOf(key)
		wantExt = append(wantExt, backup.ObjectRef{Key: key, Size: int64(len(body)), SHA256: sha256Of(body)})
	}
	if !reflect.DeepEqual(m.Extents, wantExt) {
		t.Fatalf("extents = %+v, want %+v", m.Extents, wantExt)
	}
	wantChunks := []backup.ChunkRef{
		{ID: "c1", Size: 3, SHA256: sha256Of([]byte("aaa"))},
		{ID: "c2", Size: 2, SHA256: sha256Of([]byte("bb"))},
		{ID: "c3", Size: 1, SHA256: sha256Of([]byte("c"))},
	}
	if !reflect.DeepEqual(m.Chunks, wantChunks) {
		t.Fatalf("chunks = %+v, want %+v", m.Chunks, wantChunks)
	}
	if m.ChunkCount != 3 || m.ChunkBytes != 6 || m.ChunksUploaded != 3 || m.ChunksSkipped != 0 || m.BytesUploaded != 6 {
		t.Fatalf("counts wrong: %+v", m)
	}
	if res.Stamp != stamp0 || m.Stamp() != stamp0 {
		t.Fatalf("Stamp = %q / %q, want %q", res.Stamp, m.Stamp(), stamp0)
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("exported manifest does not validate: %v", err)
	}
}

func TestExportDefaultVersionAndNoExtentSource(t *testing.T) {
	tgt := newMemTarget()
	export(t, newTestExporter(threeChunks()), tgt)
	m := decodeManifest(t, tgt.bytesOf("manifests/node-a/latest.json"))
	if m.SiloVersion != "dev" {
		t.Fatalf("default version = %q", m.SiloVersion)
	}
	if len(m.Extents) != 0 {
		t.Fatalf("extents = %v, want none", m.Extents)
	}
	if got := tgt.names("extents/"); len(got) != 0 {
		t.Fatalf("extent objects written without a source: %v", got)
	}
}

func TestExportSecondRunIsIncremental(t *testing.T) {
	tgt := newMemTarget()
	clock := newClock(t0, 0)
	e := backup.NewExporter(threeChunks(), fakeNS{snap: []byte("{}")}, testNode, backup.WithClock(clock.Now))
	export(t, e, tgt)

	tgt.resetPuts()
	clock.Advance(1_000_000_000)
	res := export(t, e, tgt)
	if n := tgt.putCount("chunks/"); n != 0 {
		t.Fatalf("second run uploaded %d chunks, want 0", n)
	}
	if res.Uploaded != 0 || res.Skipped != 3 || res.BytesUploaded != 0 || res.Chunks != 3 || res.ChunkBytes != 6 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestExportReuploadsZeroSizeChunk(t *testing.T) {
	tgt := newMemTarget()
	tgt.set("chunks/c1", nil, t0)
	res := export(t, newTestExporter(threeChunks()), tgt)
	if res.Uploaded != 3 || res.Skipped != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if got := string(tgt.bytesOf("chunks/c1")); got != "aaa" {
		t.Fatalf("chunk c1 = %q, want re-uploaded data", got)
	}
}

func TestExportIgnoresForeignObjects(t *testing.T) {
	tgt := newMemTarget()
	tgt.set("chunks/a/b", []byte("nested"), t0)
	tgt.set("chunks/bad.id", []byte("dots"), t0)
	tgt.set("chunks/c1 x", []byte("space"), t0)
	res := export(t, newTestExporter(threeChunks()), tgt)
	if res.Uploaded != 3 || res.Skipped != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	m := decodeManifest(t, tgt.bytesOf(res.Manifest))
	if got := chunkIDs(m.Chunks); !reflect.DeepEqual(got, []string{"c1", "c2", "c3"}) {
		t.Fatalf("chunks = %v", got)
	}
}

func TestExportWritesManifestLast(t *testing.T) {
	tgt := newMemTarget()
	e := newTestExporter(threeChunks(), backup.WithExtentSource(twoVolumes()), backup.WithConcurrency(3))
	res := export(t, e, tgt)
	order := tgt.putOrder()
	if len(order) < 3 {
		t.Fatalf("too few puts: %v", order)
	}
	last := order[len(order)-2:]
	if last[0] != res.Manifest || last[1] != "manifests/node-a/latest.json" {
		t.Fatalf("last puts = %v, want the manifest then latest.json", last)
	}
	for _, name := range order[:len(order)-2] {
		if strings.HasPrefix(name, "manifests/") {
			t.Fatalf("manifest written before other uploads finished: %v", order)
		}
	}
}

func TestExportFailures(t *testing.T) {
	boom := errors.New("boom")
	leftover := "runs/node-a/2026-01-01T00:00:00.000Z.json"
	type tc struct {
		name     string
		setup    func(tgt *memTarget, ch *fakeChunks, ns *fakeNS)
		node     string
		ctx      func() context.Context
		want     string
		wantIs   error
		manifest bool // a timestamped manifest is expected to exist
	}
	cancelled := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	cases := []tc{
		{name: "invalid node id", node: "../x", want: "invalid node id", wantIs: backup.ErrInvalidNodeID},
		{name: "marker list error", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) { tgt.failList("runs/node-a/", boom) }, want: "list this node's run markers", wantIs: boom},
		{name: "marker delete error", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) {
			tgt.set(leftover, []byte("{}"), t0)
			tgt.failDelete(leftover, boom)
		}, want: "leftover run marker", wantIs: boom},
		{name: "marker put error", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) { tgt.failPut("runs/*", boom) }, want: "write the run marker", wantIs: boom},
		{name: "snapshot error", setup: func(_ *memTarget, _ *fakeChunks, ns *fakeNS) { ns.err = boom }, want: "snapshot the namespace", wantIs: nil},
		{name: "namespace put error", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) { tgt.failPut("namespace/*", boom) }, want: "namespace snapshot", wantIs: boom},
		{name: "extent put error", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) { tgt.failPut("extents/*", boom) }, want: "extent map of volume", wantIs: boom},
		{name: "cancelled before extents", ctx: cancelled, want: "stopped before the extent maps", wantIs: context.Canceled},
		{name: "chunk list error", setup: func(_ *memTarget, ch *fakeChunks, _ *fakeNS) { ch.listErr = boom }, want: "list the local chunks", wantIs: boom},
		{name: "target list error", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) { tgt.failList("chunks/", boom) }, want: "already on mem://test", wantIs: boom},
		{name: "invalid chunk id", setup: func(_ *memTarget, ch *fakeChunks, _ *fakeNS) { ch.ids = []string{"c1", "../bad"} }, want: "invalid chunk id"},
		{name: "raw chunk error", setup: func(_ *memTarget, ch *fakeChunks, _ *fakeNS) { ch.rawErr["c2"] = boom }, want: "read chunk c2", wantIs: boom},
		{name: "chunk larger than a restore accepts", setup: func(_ *memTarget, ch *fakeChunks, _ *fakeNS) {
			ch.data["c2"] = make([]byte, backup.MaxChunkBytes+1)
		}, want: "chunk c2 is"},
		{name: "empty remote object cannot be deleted", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) {
			tgt.set("chunks/c1", nil, t0)
			tgt.failDelete("chunks/c1", boom)
		}, want: "replace the empty object for chunk c1", wantIs: boom},
		{name: "second upload of an emptied object fails", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) {
			tgt.set("chunks/c1", nil, t0)
			tgt.setPutIfAbsentHook(func(_ *memTarget, name string, call int) error {
				if name == "chunks/c1" && call == 2 {
					return boom
				}
				return nil
			})
		}, want: "upload chunk c1", wantIs: boom},
		{name: "remote copy unreadable after losing the race", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) {
			tgt.setPutIfAbsentHook(func(m *memTarget, name string, call int) error {
				if name == "chunks/c2" && call == 1 {
					m.set(name, []byte("theirs"), t0)
				}
				return nil
			})
			tgt.failGet("chunks/c2", boom)
		}, want: "read chunk c2 back", wantIs: boom},
		{name: "unreadable object with an unknown checksum", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) {
			tgt.set("chunks/c1", []byte("aaa"), t0)
			tgt.failGet("chunks/c1", boom)
		}, want: "read chunk c1 back", wantIs: boom},
		{name: "chunk listing fails when confirming", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) {
			tgt.setListHook(func(m *memTarget, prefix string, call int) {
				if prefix == "chunks/" && call == 2 {
					m.failList("chunks/", boom)
				}
			})
		}, want: "already on mem://test", wantIs: boom},
		{name: "chunk disappears before the manifest", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) {
			tgt.setListHook(func(m *memTarget, prefix string, call int) {
				if prefix == "chunks/" && call == 2 {
					m.remove("chunks/c1")
				}
			})
		}, want: "chunk c1 disappeared"},
		{name: "chunk changes size before the manifest", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) {
			tgt.setListHook(func(m *memTarget, prefix string, call int) {
				if prefix == "chunks/" && call == 2 {
					m.set("chunks/c1", []byte("much longer"), t0)
				}
			})
		}, want: "chunk c1 disappeared"},
		{name: "chunk put error", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) { tgt.failPut("chunks/c2", boom) }, want: "upload chunk c2", wantIs: boom},
		{name: "manifest put error", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) { tgt.failPut("manifests/node-a/2*", boom) }, want: "write the manifest", wantIs: boom},
		{name: "latest put error", setup: func(tgt *memTarget, _ *fakeChunks, _ *fakeNS) {
			tgt.failPut("manifests/node-a/latest.json", boom)
		}, want: "latest.json", wantIs: boom, manifest: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tgt := newMemTarget()
			ch := threeChunks()
			ns := fakeNS{snap: []byte("{}")}
			if c.setup != nil {
				c.setup(tgt, ch, &ns)
			}
			node := testNode
			if c.node != "" {
				node = c.node
			}
			ctx := context.Background()
			if c.ctx != nil {
				ctx = c.ctx()
			}
			e := backup.NewExporter(ch, ns, node, backup.WithClock(newClock(t0, 0).Now), backup.WithExtentSource(twoVolumes()))
			_, err := e.Export(ctx, tgt)
			if err == nil {
				t.Fatal("Export succeeded, want an error")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not contain %q", err, c.want)
			}
			if c.wantIs != nil && !errors.Is(err, c.wantIs) {
				t.Fatalf("error %v does not wrap %v", err, c.wantIs)
			}
			if tgt.has("runs/node-a/" + stamp0 + ".json") {
				t.Fatal("the run marker was left behind after the failure")
			}
			got := tgt.names("manifests/")
			if c.manifest {
				if len(got) != 1 || got[0] != "manifests/node-a/"+stamp0+".json" {
					t.Fatalf("manifests = %v, want only the timestamped one", got)
				}
			} else if len(got) != 0 {
				t.Fatalf("a manifest was written despite the failure: %v", got)
			}
		})
	}
}

func TestExportLeavesOutChunkRemovedMeanwhile(t *testing.T) {
	tgt := newMemTarget()
	ch := threeChunks()
	ch.rawErr["c2"] = fmt.Errorf("gone: %w", chunkstore.ErrNotFound)
	res := export(t, newTestExporter(ch), tgt)
	if res.Chunks != 2 || res.Uploaded != 2 {
		t.Fatalf("unexpected result: %+v", res)
	}
	m := decodeManifest(t, tgt.bytesOf(res.Manifest))
	if got := chunkIDs(m.Chunks); !reflect.DeepEqual(got, []string{"c1", "c3"}) {
		t.Fatalf("chunks = %v", got)
	}
}

func TestExportConcurrency(t *testing.T) {
	for _, n := range []int{-3, 0, 1, 2, 50} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			tgt := newMemTarget()
			res := export(t, newTestExporter(threeChunks(), backup.WithConcurrency(n)), tgt)
			if res.Uploaded != 3 {
				t.Fatalf("Uploaded = %d, want 3", res.Uploaded)
			}
		})
	}
}

func TestExportStoppedByContextDuringChunks(t *testing.T) {
	tgt := newMemTarget()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tgt.setPutHook(func(_ context.Context, name string) error {
		if strings.HasPrefix(name, "chunks/") {
			cancel()
		}
		return nil
	})
	_, err := newTestExporter(threeChunks(), backup.WithConcurrency(1)).Export(ctx, tgt)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := tgt.names("manifests/"); len(got) != 0 {
		t.Fatalf("manifest written after cancellation: %v", got)
	}
	if got := tgt.names("runs/"); len(got) != 0 {
		t.Fatalf("run marker left behind after cancellation: %v", got)
	}
}

func TestExportRunMarker(t *testing.T) {
	marker := "runs/node-a/" + stamp0 + ".json"
	t.Run("present during the run and gone afterwards", func(t *testing.T) {
		tgt := newMemTarget()
		var (
			once sync.Once
			body []byte
		)
		tgt.setPutHook(func(_ context.Context, name string) error {
			if strings.HasPrefix(name, "chunks/") {
				once.Do(func() { body = tgt.bytesOf(marker) })
			}
			return nil
		})
		export(t, newTestExporter(threeChunks()), tgt)
		var got struct {
			NodeID      string    `json:"node_id"`
			Stamp       string    `json:"stamp"`
			StartedAt   time.Time `json:"started_at"`
			HeartbeatAt time.Time `json:"heartbeat_at"`
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("marker during the run = %q: %v", body, err)
		}
		if got.NodeID != testNode || got.Stamp != stamp0 || !got.StartedAt.Equal(t0) || got.HeartbeatAt.IsZero() {
			t.Fatalf("marker = %+v", got)
		}
		if left := tgt.names("runs/"); len(left) != 0 {
			t.Fatalf("markers left after a successful run: %v", left)
		}
	})
	t.Run("leftover markers of this node are removed, other nodes are untouched", func(t *testing.T) {
		tgt := newMemTarget()
		own1 := "runs/node-a/2026-01-01T00:00:00.000Z.json"
		own2 := "runs/node-a/2026-01-02T00:00:00.000Z.json"
		other := "runs/node-b/2026-01-01T00:00:00.000Z.json"
		for _, n := range []string{own1, own2, other} {
			tgt.set(n, []byte("{}"), t0)
		}
		export(t, newTestExporter(threeChunks()), tgt)
		if got := tgt.deletedWith("runs/node-a/"); !reflect.DeepEqual(got[:2], []string{own1, own2}) {
			t.Fatalf("deleted markers = %v, want %s and %s first", got, own1, own2)
		}
		if got := tgt.names("runs/"); !reflect.DeepEqual(got, []string{other}) {
			t.Fatalf("markers after the run = %v, want only %s", got, other)
		}
	})
	t.Run("heartbeat refreshes the marker", func(t *testing.T) {
		tgt := newMemTarget()
		var (
			once sync.Once
			body []byte
		)
		tgt.setPutHook(func(ctx context.Context, name string) error {
			if !strings.HasPrefix(name, "chunks/") {
				return nil
			}
			deadline := time.Now().Add(5 * time.Second)
			for tgt.putCount("runs/") < 2 {
				if time.Now().After(deadline) {
					return errors.New("the heartbeat never rewrote the run marker")
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Millisecond):
				}
			}
			once.Do(func() { body = tgt.bytesOf(marker) })
			return nil
		})
		e := newTestExporterStepping(time.Second, threeChunks(), backup.WithHeartbeat(time.Millisecond))
		export(t, e, tgt)
		if n := tgt.putCount("runs/"); n < 2 {
			t.Fatalf("marker written %d times, want at least 2", n)
		}
		var got struct {
			StartedAt   time.Time `json:"started_at"`
			HeartbeatAt time.Time `json:"heartbeat_at"`
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if !got.HeartbeatAt.After(got.StartedAt) {
			t.Fatalf("heartbeat_at %v is not after started_at %v", got.HeartbeatAt, got.StartedAt)
		}
		if left := tgt.names("runs/"); len(left) != 0 {
			t.Fatalf("markers left after the run: %v", left)
		}
	})
	t.Run("non-positive heartbeat keeps the default", func(t *testing.T) {
		tgt := newMemTarget()
		export(t, newTestExporter(threeChunks(), backup.WithHeartbeat(0)), tgt)
		export(t, newTestExporter(threeChunks(), backup.WithHeartbeat(-time.Second)), tgt)
		if n := tgt.putCount("runs/"); n != 2 {
			t.Fatalf("marker written %d times for two runs, want 2", n)
		}
	})
}

func newTestExporterStepping(step time.Duration, chunks backup.ChunkSource, opts ...backup.ExporterOption) *backup.Exporter {
	base := []backup.ExporterOption{backup.WithClock(newClock(t0, step).Now)}
	return backup.NewExporter(chunks, fakeNS{snap: []byte(`{"ns":1}`)}, testNode, append(base, opts...)...)
}

func TestExportReusesKnownChecksums(t *testing.T) {
	latest := backup.ManifestKey(testNode, "latest")
	// secondRun exports twice into one target after prepare altered the first
	// run's latest.json, and returns the second run's result.
	secondRun := func(t *testing.T, prepare func(tgt *memTarget, first backup.Manifest)) (*memTarget, backup.Result, backup.Manifest) {
		t.Helper()
		tgt := newMemTarget()
		clock := newClock(t0, 0)
		e := backup.NewExporter(threeChunks(), fakeNS{snap: []byte("{}")}, testNode, backup.WithClock(clock.Now))
		first := decodeManifest(t, tgt.bytesOf(export(t, e, tgt).Manifest))
		if prepare != nil {
			prepare(tgt, first)
		}
		tgt.resetGets()
		clock.Advance(time.Second)
		res := export(t, e, tgt)
		return tgt, res, decodeManifest(t, tgt.bytesOf(res.Manifest))
	}
	rewriteLatest := func(t *testing.T, tgt *memTarget, first backup.Manifest, edit func(*backup.Manifest)) {
		t.Helper()
		edit(&first)
		raw, err := json.Marshal(first)
		if err != nil {
			t.Fatal(err)
		}
		tgt.set(latest, raw, t0)
	}

	t.Run("downloads nothing when every checksum is known", func(t *testing.T) {
		tgt, res, m := secondRun(t, nil)
		if n := tgt.getCount("chunks/"); n != 0 {
			t.Fatalf("second run downloaded %d chunks, want 0", n)
		}
		if res.Skipped != 3 || res.Uploaded != 0 || m.Chunks[0].SHA256 != sha256Of([]byte("aaa")) {
			t.Fatalf("result = %+v manifest chunks = %+v", res, m.Chunks)
		}
	})
	t.Run("a recorded size that differs forces a download", func(t *testing.T) {
		tgt, res, m := secondRun(t, func(tgt *memTarget, first backup.Manifest) {
			rewriteLatest(t, tgt, first, func(m *backup.Manifest) { m.Chunks[0].Size = 99 })
		})
		if n := tgt.getCount("chunks/"); n != 1 || tgt.getCount("chunks/c1") != 1 {
			t.Fatalf("chunk downloads = %d, want only chunks/c1", n)
		}
		if res.Skipped != 3 || m.Chunks[0].Size != 3 || m.Chunks[0].SHA256 != sha256Of([]byte("aaa")) {
			t.Fatalf("result = %+v chunks = %+v", res, m.Chunks)
		}
	})
	t.Run("a chunk the manifests do not know is downloaded and hashed", func(t *testing.T) {
		tgt, _, m := secondRun(t, func(tgt *memTarget, first backup.Manifest) {
			rewriteLatest(t, tgt, first, func(m *backup.Manifest) { m.Chunks = m.Chunks[1:] })
		})
		if tgt.getCount("chunks/") != 1 || tgt.getCount("chunks/c1") != 1 {
			t.Fatalf("downloads = %d, want only chunks/c1", tgt.getCount("chunks/"))
		}
		if m.Chunks[0].SHA256 != sha256Of([]byte("aaa")) {
			t.Fatalf("chunks = %+v", m.Chunks)
		}
	})
	t.Run("only latest.json counts, not older manifests", func(t *testing.T) {
		tgt, _, m := secondRun(t, func(tgt *memTarget, first backup.Manifest) {
			tgt.remove(latest)
			tgt.remove(backup.ManifestKey(testNode, stamp0))
			first.Chunks[0].SHA256 = strings.Repeat("0", 64)
			raw, _ := json.Marshal(first)
			tgt.set(backup.ManifestKey("node-b", "2026-10-01T00:00:00.000Z"), raw, t0)
		})
		if tgt.getCount("chunks/") != 3 {
			t.Fatalf("downloads = %d, want all 3", tgt.getCount("chunks/"))
		}
		if m.Chunks[0].SHA256 != sha256Of([]byte("aaa")) {
			t.Fatalf("recorded a checksum from a non-latest manifest: %+v", m.Chunks[0])
		}
	})
	for _, c := range []struct {
		name   string
		mutate func(tgt *memTarget, first backup.Manifest)
	}{
		{"undecodable latest.json", func(tgt *memTarget, _ backup.Manifest) { tgt.set(latest, []byte("not json"), t0) }},
		{"latest.json cannot be read", func(tgt *memTarget, _ backup.Manifest) { tgt.failGet("manifests/*", errors.New("denied")) }},
		{"latest.json is over the size cap", func(tgt *memTarget, _ backup.Manifest) {
			tgt.failGet("manifests/*", fmt.Errorf("%w: planted", blobstore.ErrTooLarge))
		}},
		{"manifests cannot be listed", func(tgt *memTarget, _ backup.Manifest) { tgt.failList("manifests/", errors.New("denied")) }},
		{"entries with a malformed checksum", func(tgt *memTarget, first backup.Manifest) {
			rewriteLatest(t, tgt, first, func(m *backup.Manifest) {
				for i := range m.Chunks {
					m.Chunks[i].SHA256 = "XYZ"
				}
			})
		}},
		{"entries with an uppercase checksum", func(tgt *memTarget, first backup.Manifest) {
			rewriteLatest(t, tgt, first, func(m *backup.Manifest) {
				for i := range m.Chunks {
					m.Chunks[i].SHA256 = strings.ToUpper(m.Chunks[i].SHA256)
				}
			})
		}},
		{"entries without a size", func(tgt *memTarget, first backup.Manifest) {
			rewriteLatest(t, tgt, first, func(m *backup.Manifest) {
				for i := range m.Chunks {
					m.Chunks[i].Size = 0
				}
			})
		}},
	} {
		t.Run("ignores "+c.name, func(t *testing.T) {
			tgt, res, m := secondRun(t, c.mutate)
			if tgt.getCount("chunks/") != 3 {
				t.Fatalf("downloads = %d, want all 3", tgt.getCount("chunks/"))
			}
			if res.Skipped != 3 || len(m.Chunks) != 3 || m.Chunks[2].SHA256 != sha256Of([]byte("c")) {
				t.Fatalf("result = %+v chunks = %+v", res, m.Chunks)
			}
		})
	}
	t.Run("uses another node's latest.json", func(t *testing.T) {
		tgt := newMemTarget()
		export(t, newTestExporter(threeChunks()), tgt)
		tgt.resetGets()
		b := backup.NewExporter(threeChunks(), fakeNS{snap: []byte("{}")}, "node-b", backup.WithClock(newClock(t0, 0).Now))
		res := export(t, b, tgt)
		if n := tgt.getCount("chunks/"); n != 0 {
			t.Fatalf("node-b downloaded %d chunks, want 0", n)
		}
		if res.Skipped != 3 || res.Uploaded != 0 {
			t.Fatalf("result = %+v", res)
		}
	})
}

func TestExportKeepsTheCopyAnotherNodeWroteFirst(t *testing.T) {
	tgt := newMemTarget()
	theirs := []byte("the other node's ciphertext")
	tgt.setPutIfAbsentHook(func(m *memTarget, name string, call int) error {
		if name == "chunks/c2" && call == 1 {
			m.set(name, theirs, t0)
		}
		return nil
	})
	res := export(t, newTestExporter(threeChunks()), tgt)
	if res.Uploaded != 2 || res.Skipped != 1 || res.BytesUploaded != 4 {
		t.Fatalf("result = %+v", res)
	}
	if !bytes.Equal(tgt.bytesOf("chunks/c2"), theirs) {
		t.Fatal("the other node's object was overwritten")
	}
	m := decodeManifest(t, tgt.bytesOf(res.Manifest))
	want := backup.ChunkRef{ID: "c2", Size: int64(len(theirs)), SHA256: sha256Of(theirs)}
	if m.Chunks[1] != want {
		t.Fatalf("chunk c2 = %+v, want the remote copy %+v", m.Chunks[1], want)
	}
	if m.ChunkBytes != 3+int64(len(theirs))+1 {
		t.Fatalf("ChunkBytes = %d", m.ChunkBytes)
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestExportReplacesEmptyRemoteObject(t *testing.T) {
	tgt := newMemTarget()
	tgt.set("chunks/c1", nil, t0)
	res := export(t, newTestExporter(threeChunks()), tgt)
	if got := tgt.deletedWith("chunks/c1"); len(got) != 1 {
		t.Fatalf("deletes of chunks/c1 = %v, want 1", got)
	}
	if res.Uploaded != 3 || string(tgt.bytesOf("chunks/c1")) != "aaa" {
		t.Fatalf("result = %+v, chunk c1 = %q", res, tgt.bytesOf("chunks/c1"))
	}
	m := decodeManifest(t, tgt.bytesOf(res.Manifest))
	if m.Chunks[0] != (backup.ChunkRef{ID: "c1", Size: 3, SHA256: sha256Of([]byte("aaa"))}) {
		t.Fatalf("chunk c1 = %+v", m.Chunks[0])
	}
}

func TestExportDoesNotDownloadWhatItUploaded(t *testing.T) {
	tgt := newMemTarget()
	export(t, newTestExporter(threeChunks()), tgt)
	if n := tgt.getCount("chunks/"); n != 0 {
		t.Fatalf("first run downloaded %d chunks, want 0", n)
	}
}

func TestExportSanitizesVersion(t *testing.T) {
	cases := []struct{ in, want string }{
		{"v1 \x1b[31m", "v1___31m"},
		{"v1.2.3+build_4-rc", "v1.2.3+build_4-rc"},
		{"v\u00e9\n1", "v__1"},
		{"", ""},
		{strings.Repeat("a", 100), strings.Repeat("a", 64)},
	}
	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			tgt := newMemTarget()
			export(t, newTestExporter(threeChunks(), backup.WithVersion(c.in)), tgt)
			m := decodeManifest(t, tgt.bytesOf("manifests/node-a/latest.json"))
			if m.SiloVersion != c.want {
				t.Fatalf("SiloVersion = %q, want %q", m.SiloVersion, c.want)
			}
			if err := m.Validate(); err != nil {
				t.Fatalf("manifest with a sanitized version does not validate: %v", err)
			}
		})
	}
}
