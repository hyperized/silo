package replication

import (
	"context"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/extentmap"
	"github.com/hyperized/silo/internal/hlc"
)

// A trimmed extent's chunk leaves the keep set, so the GC reclaims it, and the
// empty id never enters the keep set as a chunk of its own.
func TestChunkGC_ReclaimsChunkOfTrimmedExtent(t *testing.T) {
	store := extentmap.New(quietLog())
	if err := store.SetBatch("v1", []uint64{0, 1}, []string{"k0", "k1"}, hlc.Timestamp{Wall: 10}); err != nil {
		t.Fatalf("SetBatch: %v", err)
	}
	if err := store.Set("v1", 0, extentmap.Unmapped, hlc.Timestamp{Wall: 20}); err != nil {
		t.Fatalf("unmap: %v", err)
	}
	ns := fakeNSRefs{volumes: []string{"v1"}}

	keep, unaccounted := liveSet(ns, store)
	if unaccounted != 0 {
		t.Fatalf("unaccounted = %d, want a complete view", unaccounted)
	}
	if _, ok := keep[extentmap.Unmapped]; ok {
		t.Error("the empty chunk id entered the keep set")
	}
	if _, ok := keep["k0"]; ok {
		t.Error("the trimmed extent's chunk is still in the keep set")
	}

	lister := newFakeChunkLister("k0", "k1")
	g := NewChunkGC(lister, ns, store, "a", time.Hour, time.Hour, true, quietLog())
	g.runOnce(context.Background())
	if del := lister.snapDeleted(); len(del) != 1 || del[0] != "k0" {
		t.Errorf("deleted = %v, want only the trimmed extent's chunk k0", del)
	}
}

// A replica that never received an unmap still holds the old binding. The
// scrubber must push the unmap to it, and to a replica holding nothing, without
// fetching any chunk for it.
func TestExtentScrubber_PushesUnmapToReplicas(t *testing.T) {
	unmap := crdt.MapEntry[uint64, string]{Key: 0, Value: extentmap.Unmapped, TS: hlc.Timestamp{Wall: 20}}
	mapped := crdt.MapEntry[uint64, string]{Key: 1, Value: "c1", TS: hlc.Timestamp{Wall: 10}}
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b", "c"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{
		"v1": {unmap, mapped},
	}}
	peers := newFakeEpeers()
	// b is diverged: it has extent 0 still bound to the chunk the trim released.
	peers.statHas["b:7000"] = true
	peers.statDiverged["b:7000"] = true
	peers.fetchRes["b:7000"] = []crdt.MapEntry[uint64, string]{{Key: 0, Value: "c0", TS: hlc.Timestamp{Wall: 10}}, mapped}
	// c holds no copy at all.
	s := NewExtentScrubber(place, cat, peers, 3, time.Hour, quietLog())

	if !s.scrubMap(context.Background(), "a", "v1") {
		t.Fatal("replicas missing the unmap should count as under-replicated")
	}
	rec, ok := appliedTo(peers.snapApplied(), "b:7000", "v1")
	if !ok || len(rec.entries) != 1 || rec.entries[0] != unmap {
		t.Errorf("pushed to b %+v, want exactly the unmap", rec.entries)
	}
	rec, ok = appliedTo(peers.snapApplied(), "c:7000", "v1")
	if !ok || len(rec.entries) != 2 || rec.entries[0] != unmap {
		t.Errorf("pushed to c %+v, want the whole map including the unmap", rec.entries)
	}
}

// Without a digest the scrubber compares extent counts. A peer's count leaves
// unmapped extents out, so ours must too, or a trimmed map never agrees.
func TestExtentScrubber_CountFallbackIgnoresUnmapped(t *testing.T) {
	place := &fakeMetaPlace{self: "a", replicas: []string{"a", "b"}}
	cat := &fakeExtCatalog{vols: []string{"v1"}, snap: map[string][]crdt.MapEntry[uint64, string]{
		"v1": {{Key: 0, Value: extentmap.Unmapped}, {Key: 1, Value: "c1"}},
	}}
	peers := newFakeEpeers()
	peers.statHas["b:7000"] = true
	peers.noDigest["b:7000"] = true
	peers.fetchRes["b:7000"] = []crdt.MapEntry[uint64, string]{{Key: 1, Value: "c1"}} // count 1
	s := NewExtentScrubber(place, cat, peers, 2, time.Hour, quietLog())
	if same, known := s.agrees(context.Background(), "b", "v1"); !known || !same {
		t.Errorf("agrees = (%v, %v), want one mapped extent on each side to agree", same, known)
	}
}
