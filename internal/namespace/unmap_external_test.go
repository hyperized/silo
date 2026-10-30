package namespace_test

import (
	"reflect"
	"testing"

	"github.com/hyperized/silo/internal/namespace"
)

// newLeasedVolume returns a namespace holding /vol with extents 0..2 mapped to
// c0..c2, leased to "w", and the clock pointer driving it.
func newLeasedVolume(t *testing.T) (*namespace.Namespace, *int64) {
	t.Helper()
	clk := new(int64)
	*clk = 100
	ns := nsAt("a", clk)
	*clk++
	if _, err := ns.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	*clk++
	if _, err := ns.AcquireLease("/vol", "w"); err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	*clk++
	if err := ns.WriteExtents("/vol", []uint64{0, 1, 2}, []string{"c0", "c1", "c2"}, "w"); err != nil {
		t.Fatalf("WriteExtents: %v", err)
	}
	return ns, clk
}

// An empty chunk id written by a trim reads as unmapped everywhere the
// namespace exposes extents, and stops referencing the old chunk.
func TestNamespace_UnmappedExtent(t *testing.T) {
	ns, clk := newLeasedVolume(t)
	*clk++
	if err := ns.WriteExtents("/vol", []uint64{1}, []string{""}, "w"); err != nil {
		t.Fatalf("unmap: %v", err)
	}
	*clk++
	if err := ns.WriteExtent("/vol", 2, "", "w"); err != nil {
		t.Fatalf("unmap single: %v", err)
	}

	for _, idx := range []uint64{1, 2} {
		if id, ok, err := ns.Extent("/vol", idx); err != nil || ok || id != "" {
			t.Errorf("Extent(%d) = (%q, %v, %v), want unmapped", idx, id, ok, err)
		}
	}
	if got, err := ns.Extents("/vol"); err != nil || !reflect.DeepEqual(got, map[uint64]string{0: "c0"}) {
		t.Errorf("Extents = (%v, %v), want only {0: c0}", got, err)
	}
	chunks, _ := ns.LiveChunkRefs()
	if !reflect.DeepEqual(chunks, map[string]struct{}{"c0": {}}) {
		t.Errorf("LiveChunkRefs chunks = %v, want only c0 (no empty id, no trimmed chunks)", chunks)
	}
	vols := ns.Volumes()
	if len(vols) != 1 || vols[0].InlineExtents != 1 {
		t.Errorf("Volumes = %+v, want one volume with 1 mapped inline extent", vols)
	}
}

// A trim is fenced by the lease like any write.
func TestNamespace_UnmapFencedByLease(t *testing.T) {
	ns, _ := newLeasedVolume(t)
	if err := ns.WriteExtents("/vol", []uint64{0}, []string{""}, "stale"); err == nil {
		t.Fatal("a stale holder's unmap was accepted")
	}
	if _, ok, _ := ns.Extent("/vol", 0); !ok {
		t.Error("a fenced unmap changed the map")
	}
}

// The unmap survives the namespace's own serialisation and wins a merge against
// a replica that still holds the older binding.
func TestNamespace_UnmapRoundTripsAndWinsMerge(t *testing.T) {
	ns, clk := newLeasedVolume(t)
	stale, err := ns.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	*clk++
	if err := ns.WriteExtent("/vol", 0, "", "w"); err != nil {
		t.Fatalf("unmap: %v", err)
	}
	fresh, err := ns.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	for _, order := range [][2][]byte{{stale, fresh}, {fresh, stale}} {
		peerClk := int64(1)
		peer := nsAt("b", &peerClk)
		for _, b := range order {
			if err := peer.MergeBytes(b); err != nil {
				t.Fatalf("MergeBytes: %v", err)
			}
		}
		if id, ok, err := peer.Extent("/vol", 0); err != nil || ok {
			t.Errorf("after merge Extent(0) = (%q, %v, %v), want unmapped", id, ok, err)
		}
		if got, _ := peer.Extents("/vol"); len(got) != 2 {
			t.Errorf("after merge Extents = %v, want extents 1 and 2 only", got)
		}
	}
}
