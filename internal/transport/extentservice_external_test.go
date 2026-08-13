package transport_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	extentv1 "github.com/hyperized/silo/api/proto/silo/extent/v1"
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/transport"
)

func hlcWall(w int64) hlc.Timestamp { return hlc.Timestamp{Wall: w} }

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeES is a minimal extent store for the service unit tests.
type fakeES struct {
	merged    map[string][]crdt.MapEntry[uint64, string]
	ensured   []string
	snap      map[string][]crdt.MapEntry[uint64, string]
	has       map[string]bool
	length    map[string]int
	deleted   []string
	deleteErr error
}

func newFakeES() *fakeES {
	return &fakeES{merged: map[string][]crdt.MapEntry[uint64, string]{}, snap: map[string][]crdt.MapEntry[uint64, string]{}, has: map[string]bool{}, length: map[string]int{}}
}

func (f *fakeES) Merge(vol string, e []crdt.MapEntry[uint64, string]) { f.merged[vol] = e }
func (f *fakeES) Ensure(vol string)                                   { f.ensured = append(f.ensured, vol) }
func (f *fakeES) Snapshot(vol string) []crdt.MapEntry[uint64, string] { return f.snap[vol] }
func (f *fakeES) Has(vol string) bool                                 { return f.has[vol] }
func (f *fakeES) Len(vol string) int                                  { return f.length[vol] }

// Digest stands in for a real fingerprint: any volume the fake knows about gets
// a stable non-nil value, and an unknown one gets nil, which is the distinction
// the service is expected to pass through.
func (f *fakeES) Digest(vol string) []byte {
	if !f.has[vol] {
		return nil
	}
	return []byte("digest-" + vol)
}

func (f *fakeES) Delete(vol string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleted = append(f.deleted, vol)
	return nil
}

// collectStream captures the GetResponse frames the service streams.
type collectStream struct {
	grpc.ServerStream
	frames [][]*extentv1.ExtentEntry
}

func (c *collectStream) Send(r *extentv1.GetResponse) error {
	c.frames = append(c.frames, r.GetEntries())
	return nil
}

// errStream fails on Send to exercise the stream-error path.
type errStream struct{ grpc.ServerStream }

func (errStream) Send(*extentv1.GetResponse) error { return errors.New("send boom") }

func code(err error) codes.Code { return status.Code(err) }

func TestExtentService_Apply(t *testing.T) {
	store := newFakeES()
	svc := transport.NewExtentService(store, quiet())
	ctx := context.Background()

	if _, err := svc.Apply(ctx, &extentv1.ApplyRequest{VolumeId: ""}); code(err) != codes.InvalidArgument {
		t.Errorf("empty volume id: got code %v, want InvalidArgument", code(err))
	}

	entries := []*extentv1.ExtentEntry{{Index: 0, ChunkId: "c0", Ts: &extentv1.Hlc{Wall: 1, Logical: 2, Node: "n"}}}
	if _, err := svc.Apply(ctx, &extentv1.ApplyRequest{VolumeId: "vol", Entries: entries}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got := store.merged["vol"]
	if len(got) != 1 || got[0].Key != 0 || got[0].Value != "c0" || got[0].TS.Wall != 1 || got[0].TS.Logical != 2 || got[0].TS.Node != "n" {
		t.Errorf("merged entries wrong: %+v", got)
	}

	if _, err := svc.Apply(ctx, &extentv1.ApplyRequest{VolumeId: "vol2", Ensure: true}); err != nil {
		t.Fatalf("Apply ensure: %v", err)
	}
	if len(store.ensured) != 1 || store.ensured[0] != "vol2" {
		t.Errorf("Ensure not called: %v", store.ensured)
	}

	// No entries and ensure=false is a no-op (neither Merge nor Ensure).
	if _, err := svc.Apply(ctx, &extentv1.ApplyRequest{VolumeId: "vol3"}); err != nil {
		t.Fatalf("Apply no-op: %v", err)
	}
	if _, ok := store.merged["vol3"]; ok {
		t.Error("no-op Apply should not Merge")
	}
}

func TestExtentService_Get(t *testing.T) {
	store := newFakeES()
	svc := transport.NewExtentService(store, quiet())

	if err := svc.Get(&extentv1.GetRequest{VolumeId: ""}, &collectStream{}); code(err) != codes.InvalidArgument {
		t.Errorf("empty volume id: got code %v, want InvalidArgument", code(err))
	}

	// 2500 entries stream in three batches (1024+1024+452).
	entries := make([]crdt.MapEntry[uint64, string], 2500)
	for i := range entries {
		entries[i] = crdt.MapEntry[uint64, string]{Key: uint64(i), Value: "c", TS: hlcWall(int64(i))}
	}
	store.snap["vol"] = entries
	cs := &collectStream{}
	if err := svc.Get(&extentv1.GetRequest{VolumeId: "vol"}, cs); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(cs.frames) != 3 {
		t.Errorf("want 3 frames, got %d", len(cs.frames))
	}
	total := 0
	for _, f := range cs.frames {
		total += len(f)
	}
	if total != 2500 {
		t.Errorf("streamed %d entries, want 2500", total)
	}

	// A Send failure surfaces.
	if err := svc.Get(&extentv1.GetRequest{VolumeId: "vol"}, errStream{}); err == nil {
		t.Error("a Send failure should surface")
	}
}

func TestExtentService_Delete(t *testing.T) {
	store := newFakeES()
	svc := transport.NewExtentService(store, quiet())
	ctx := context.Background()

	if _, err := svc.Delete(ctx, &extentv1.DeleteRequest{VolumeId: ""}); code(err) != codes.InvalidArgument {
		t.Errorf("empty volume id: got code %v, want InvalidArgument", code(err))
	}

	if _, err := svc.Delete(ctx, &extentv1.DeleteRequest{VolumeId: "vol"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(store.deleted) != 1 || store.deleted[0] != "vol" {
		t.Errorf("Delete not forwarded to the store: %v", store.deleted)
	}

	// A store failure becomes an Internal error so the caller can fall back on
	// the reaper.
	store.deleteErr = errors.New("read-only fs")
	if _, err := svc.Delete(ctx, &extentv1.DeleteRequest{VolumeId: "vol"}); code(err) != codes.Internal {
		t.Errorf("store failure: got code %v, want Internal", code(err))
	}
}

func TestExtentService_Stat(t *testing.T) {
	store := newFakeES()
	store.has["vol"] = true
	store.length["vol"] = 7
	svc := transport.NewExtentService(store, quiet())
	ctx := context.Background()

	if _, err := svc.Stat(ctx, &extentv1.StatRequest{VolumeId: ""}); code(err) != codes.InvalidArgument {
		t.Errorf("empty volume id: got code %v, want InvalidArgument", code(err))
	}
	resp, err := svc.Stat(ctx, &extentv1.StatRequest{VolumeId: "vol"})
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !resp.GetHas() || resp.GetCount() != 7 {
		t.Errorf("Stat = (has=%v,count=%d), want (true,7)", resp.GetHas(), resp.GetCount())
	}
}
