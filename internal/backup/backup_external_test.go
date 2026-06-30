package backup_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

type fakeExtents struct {
	vols map[string][]crdt.MapEntry[uint64, string]
}

func (f fakeExtents) Volumes() []string {
	out := make([]string, 0, len(f.vols))
	for v := range f.vols {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func (f fakeExtents) Snapshot(vol string) []crdt.MapEntry[uint64, string] { return f.vols[vol] }

type fakeChunks struct {
	ids     []string
	data    map[string][]byte
	listErr error
	rawErr  map[string]error
}

func (f fakeChunks) List(context.Context) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.ids, nil
}

func (f fakeChunks) RawChunk(_ context.Context, id string) ([]byte, error) {
	if err := f.rawErr[id]; err != nil {
		return nil, err
	}
	return f.data[id], nil
}

type fakeNS struct {
	snap []byte
	err  error
}

func (f fakeNS) Snapshot() ([]byte, error) { return f.snap, f.err }

type captureTarget struct {
	puts   map[string][]byte
	putErr map[string]error
	anyErr error
}

func newCapture() *captureTarget {
	return &captureTarget{puts: map[string][]byte{}, putErr: map[string]error{}}
}

func (c *captureTarget) Put(_ context.Context, name string, data []byte) error {
	if c.anyErr != nil {
		return c.anyErr
	}
	if err := c.putErr[name]; err != nil {
		return err
	}
	c.puts[name] = append([]byte(nil), data...)
	return nil
}

func (c *captureTarget) Name() string { return "capture://test" }

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestExporter_Export(t *testing.T) {
	chunks := fakeChunks{
		ids:  []string{"c1", "c2"},
		data: map[string][]byte{"c1": []byte("ciphertext-1"), "c2": []byte("ciphertext-22")},
	}
	exp := backup.NewExporter(chunks, fakeNS{snap: []byte("namespace-state")}, "node-a")
	tgt := newCapture()

	stats, err := exp.Export(context.Background(), tgt)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if stats.Chunks != 2 || stats.Bytes != int64(len("ciphertext-1")+len("ciphertext-22")) {
		t.Errorf("stats = %+v", stats)
	}
	if string(tgt.puts["namespace/node-a.json"]) != "namespace-state" {
		t.Errorf("namespace not written: %q", tgt.puts["namespace/node-a.json"])
	}
	if string(tgt.puts["chunks/c1"]) != "ciphertext-1" || string(tgt.puts["chunks/c2"]) != "ciphertext-22" {
		t.Errorf("chunks not written: %v", keys(tgt.puts))
	}
}

func TestExporter_ExportsExtentMaps(t *testing.T) {
	vol := "inode-100.0.node/odd:chars" // a node id with filesystem-unsafe runes
	ext := fakeExtents{vols: map[string][]crdt.MapEntry[uint64, string]{
		vol: {{Key: 0, Value: "c0", TS: hlc.Timestamp{Wall: 1}}, {Key: 5, Value: "c5", TS: hlc.Timestamp{Wall: 2}}},
	}}
	exp := backup.NewExporter(fakeChunks{}, fakeNS{snap: []byte("ns")}, "node-a", backup.WithExtentSource(ext))
	tgt := newCapture()
	if _, err := exp.Export(context.Background(), tgt); err != nil {
		t.Fatalf("Export: %v", err)
	}

	name := "extents/node-a/" + base64.RawURLEncoding.EncodeToString([]byte(vol)) + ".json"
	raw, ok := tgt.puts[name]
	if !ok {
		t.Fatalf("extent map not written; got %v", keys(tgt.puts))
	}
	var got struct {
		VolumeID string                          `json:"volume_id"`
		Entries  []crdt.MapEntry[uint64, string] `json:"entries"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal extent backup: %v", err)
	}
	if got.VolumeID != vol || len(got.Entries) != 2 || got.Entries[1].Value != "c5" {
		t.Errorf("extent backup wrong: %+v", got)
	}
}

func TestExporter_ExtentPutErrorAndCancel(t *testing.T) {
	vol := "v1"
	ext := fakeExtents{vols: map[string][]crdt.MapEntry[uint64, string]{vol: {{Key: 0, Value: "c0"}}}}
	name := "extents/node-a/" + base64.RawURLEncoding.EncodeToString([]byte(vol)) + ".json"

	// Put failure on the extent map aborts the run.
	tgt := newCapture()
	tgt.putErr[name] = errors.New("boom")
	exp := backup.NewExporter(fakeChunks{}, fakeNS{snap: []byte("ns")}, "node-a", backup.WithExtentSource(ext))
	if _, err := exp.Export(context.Background(), tgt); err == nil {
		t.Error("a failed extent-map upload should abort the backup")
	}

	// A cancelled context is observed before writing an extent map.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := exp.Export(ctx, newCapture()); err == nil {
		t.Error("a cancelled context should stop the export")
	}
}

func TestExporter_Errors(t *testing.T) {
	boom := errors.New("boom")
	ok := fakeChunks{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("x")}}

	// Snapshot failure.
	if _, err := backup.NewExporter(ok, fakeNS{err: boom}, "n").Export(context.Background(), newCapture()); err == nil {
		t.Error("snapshot error should abort")
	}
	// Namespace upload failure.
	nsFail := newCapture()
	nsFail.putErr["namespace/n.json"] = boom
	if _, err := backup.NewExporter(ok, fakeNS{}, "n").Export(context.Background(), nsFail); err == nil {
		t.Error("namespace put error should abort")
	}
	// List failure.
	if _, err := backup.NewExporter(fakeChunks{listErr: boom}, fakeNS{}, "n").Export(context.Background(), newCapture()); err == nil {
		t.Error("list error should abort")
	}
	// Raw read failure.
	rawFail := fakeChunks{ids: []string{"c1"}, rawErr: map[string]error{"c1": boom}}
	if _, err := backup.NewExporter(rawFail, fakeNS{}, "n").Export(context.Background(), newCapture()); err == nil {
		t.Error("raw read error should abort")
	}
	// Chunk upload failure.
	chunkFail := newCapture()
	chunkFail.putErr["chunks/c1"] = boom
	if _, err := backup.NewExporter(ok, fakeNS{}, "n").Export(context.Background(), chunkFail); err == nil {
		t.Error("chunk put error should abort")
	}
	// Cancelled context mid-run.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := backup.NewExporter(ok, fakeNS{}, "n").Export(ctx, newCapture()); err == nil {
		t.Error("a cancelled context should abort the loop")
	}
}

func TestSubsystem_RunOnceAndMetrics(t *testing.T) {
	chunks := fakeChunks{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("x")}}
	exp := backup.NewExporter(chunks, fakeNS{snap: []byte("ns")}, "n")

	// Success path via a short-interval ticker.
	sub := backup.NewSubsystem(exp, newCapture(), 5*time.Millisecond, discardLogger())
	if sub.Name() != "backup" || sub.MetricPrefix() != "silo_backup" {
		t.Errorf("identity = %q/%q", sub.Name(), sub.MetricPrefix())
	}
	go func() { _ = sub.Start() }()
	time.Sleep(40 * time.Millisecond)
	if err := sub.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if metricValue(sub, "runs_total") < 1 {
		t.Error("expected at least one run")
	}
	if metricValue(sub, "last_chunks") != 1 {
		t.Errorf("last_chunks = %v, want 1", metricValue(sub, "last_chunks"))
	}

	// Failure path: the target errors, so failures climb and last_chunks stays 0.
	failTarget := newCapture()
	failTarget.anyErr = errors.New("upload denied")
	failSub := backup.NewSubsystem(exp, failTarget, 5*time.Millisecond, discardLogger())
	go func() { _ = failSub.Start() }()
	time.Sleep(40 * time.Millisecond)
	_ = failSub.Shutdown(context.Background())
	if metricValue(failSub, "failures_total") < 1 || metricValue(failSub, "last_chunks") != 0 {
		t.Errorf("failure metrics = runs=%v failures=%v chunks=%v",
			metricValue(failSub, "runs_total"), metricValue(failSub, "failures_total"), metricValue(failSub, "last_chunks"))
	}
}

func TestSubsystem_DefaultIntervalAndShutdownDeadline(t *testing.T) {
	sub := backup.NewSubsystem(backup.NewExporter(fakeChunks{}, fakeNS{}, "n"), newCapture(), 0, discardLogger())
	// Shutdown without Start: done never closes, so an expired ctx hits the deadline.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sub.Shutdown(ctx); err == nil {
		t.Error("Shutdown with an expired context should error")
	}
}

var _ blobstore.Target = (*captureTarget)(nil)

func metricValue(s *backup.Subsystem, name string) float64 {
	for _, m := range s.CollectMetrics() {
		if m.Name == name {
			return m.Value
		}
	}
	return -1
}

func keys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
