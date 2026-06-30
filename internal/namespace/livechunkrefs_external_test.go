package namespace_test

import (
	"testing"
)

// LiveChunkRefs gathers chunk ids from both reference kinds the namespace owns —
// file manifests and in-namespace volume extents (the legacy/non-extent-
// replication binding) — and reports every live volume's inode id. It is the
// global half of the chunk GC's keep-set, so a missing reference here would let
// the GC reclaim a live chunk.
func TestNamespace_LiveChunkRefs(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++

	// A file with a three-chunk manifest.
	if _, err := ns.Touch("/f"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	for _, c := range []string{"fc0", "fc1", "fc2"} {
		clk++
		if err := ns.AppendChunk("/f", c); err != nil {
			t.Fatalf("AppendChunk %s: %v", c, err)
		}
	}

	// A volume with two in-namespace extent bindings (legacy write path).
	clk++
	volID, err := ns.CreateVolume("/vol", 4096)
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	clk++
	if _, err := ns.AcquireLease("/vol", "w"); err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	for _, b := range []struct {
		idx   uint64
		chunk string
	}{{0, "vc0"}, {5, "vc5"}} {
		clk++
		if err := ns.WriteExtent("/vol", b.idx, b.chunk, "w"); err != nil {
			t.Fatalf("WriteExtent %d: %v", b.idx, err)
		}
	}

	// An empty volume (created, never written): contributes no chunks but must
	// still appear in the volume id set so the GC consults its (out-of-band) map.
	clk++
	emptyID, err := ns.CreateVolume("/empty", 4096)
	if err != nil {
		t.Fatalf("CreateVolume empty: %v", err)
	}

	chunks, volumes := ns.LiveChunkRefs()

	wantChunks := []string{"fc0", "fc1", "fc2", "vc0", "vc5"}
	if len(chunks) != len(wantChunks) {
		t.Errorf("chunk set = %v, want %v", chunks, wantChunks)
	}
	for _, c := range wantChunks {
		if _, ok := chunks[c]; !ok {
			t.Errorf("chunk %q missing from the live set %v", c, chunks)
		}
	}

	if len(volumes) != 2 {
		t.Errorf("volume set size = %d, want 2 (%v)", len(volumes), volumes)
	}
	for _, id := range []string{volID, emptyID} {
		if _, ok := volumes[id]; !ok {
			t.Errorf("volume %q missing from the live volume set %v", id, volumes)
		}
	}
}

// A chunk overwritten in place (copy-on-write rebind) leaves only the current
// binding in the live set: the superseded chunk drops out, which is exactly how
// the mark-and-sweep GC reclaims overwrite orphans.
func TestNamespace_LiveChunkRefs_OverwriteDropsOldChunk(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	clk++
	if _, err := ns.AcquireLease("/vol", "w"); err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	clk++
	if err := ns.WriteExtent("/vol", 0, "old", "w"); err != nil {
		t.Fatalf("WriteExtent old: %v", err)
	}
	clk++
	if err := ns.WriteExtent("/vol", 0, "new", "w"); err != nil {
		t.Fatalf("WriteExtent new: %v", err)
	}

	chunks, _ := ns.LiveChunkRefs()
	if _, ok := chunks["new"]; !ok {
		t.Error("the current binding 'new' must be live")
	}
	if _, ok := chunks["old"]; ok {
		t.Error("the overwritten binding 'old' must not be live")
	}
}

// A removed file's manifest chunks drop out of the live set once its link is
// gone, so the GC can reclaim them.
func TestNamespace_LiveChunkRefs_RemovedFileDropsChunks(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.Touch("/f"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	clk++
	if err := ns.AppendChunk("/f", "gone"); err != nil {
		t.Fatalf("AppendChunk: %v", err)
	}
	clk++
	if err := ns.Remove("/f"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	chunks, _ := ns.LiveChunkRefs()
	if _, ok := chunks["gone"]; ok {
		t.Errorf("a removed file's chunk must not be live, got %v", chunks)
	}
}
