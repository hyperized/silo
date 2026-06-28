package namespace_test

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

func TestNamespace_VolumeInodeID(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++
	id, err := ns.CreateVolume("/vol", 4096)
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	got, err := ns.VolumeInodeID("/vol")
	if err != nil || got != id {
		t.Fatalf("VolumeInodeID = (%q,%v), want (%q,nil)", got, err, id)
	}

	clk++
	if _, err := ns.Mkdir("/dir"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if _, err := ns.VolumeInodeID("/dir"); err == nil {
		t.Error("VolumeInodeID on a directory should error")
	}
	if _, err := ns.VolumeInodeID("/missing"); err == nil {
		t.Error("VolumeInodeID on a missing path should error")
	}
}

func TestNamespace_ReferencedInodeIDs(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.Mkdir("/vols"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	clk++
	aID, err := ns.CreateVolume("/vols/a", 4096)
	if err != nil {
		t.Fatalf("CreateVolume a: %v", err)
	}
	clk++
	bID, err := ns.CreateVolume("/vols/b", 4096)
	if err != nil {
		t.Fatalf("CreateVolume b: %v", err)
	}

	live := ns.ReferencedInodeIDs()
	for _, id := range []string{aID, bID} {
		if _, ok := live[id]; !ok {
			t.Errorf("volume %s should be referenced", id)
		}
	}
	if len(live) == 0 {
		t.Error("the referenced set should always include the root")
	}

	// Removing a volume drops its inode from the referenced set; siblings stay.
	clk++
	if err := ns.Remove("/vols/b"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	live = ns.ReferencedInodeIDs()
	if _, ok := live[bID]; ok {
		t.Error("a removed volume's inode should not be referenced")
	}
	if _, ok := live[aID]; !ok {
		t.Error("a surviving volume's inode should still be referenced")
	}

	// Removing the parent directory makes everything beneath it unreachable,
	// even though the child link still exists under the now-orphaned dir inode.
	clk++
	if err := ns.Remove("/vols"); err != nil {
		t.Fatalf("Remove dir: %v", err)
	}
	if _, ok := ns.ReferencedInodeIDs()[aID]; ok {
		t.Error("a volume under a removed directory should be unreachable")
	}
}

func TestNamespace_MergeDoesNotResurrectReapedInode(t *testing.T) {
	var clkA int64 = 100
	a := nsAt("a", &clkA)
	clkA++
	if _, err := a.Mkdir("/vols"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	clkA++
	xID, err := a.CreateVolume("/vols/x", 4096)
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	// B learns the volume over gossip, so B holds it reachable.
	var clkB int64 = 100
	b := nsAt("b", &clkB)
	b.Merge(a)
	if _, ok := b.ReferencedInodeIDs()[xID]; !ok {
		t.Fatal("B should have learned the volume")
	}

	// A removes it with a newer HLC.
	clkA++
	if err := a.Remove("/vols/x"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// A merges B's STALE snapshot (B still has the live link). A's newer remove
	// wins in the children OR-Set and the prune drops the inode again — the stale
	// peer must not resurrect it.
	a.Merge(b)
	if _, ok := a.ReferencedInodeIDs()[xID]; ok {
		t.Error("a stale peer must not resurrect a removed volume's inode on A")
	}

	// And B merging A's removal reaps the orphaned inode on B too (convergence).
	b.Merge(a)
	if _, ok := b.ReferencedInodeIDs()[xID]; ok {
		t.Error("merging the removal should reap the orphaned inode on the peer")
	}
}

func TestNamespace_GossipSnapshotOmitsExtentsButKeepsScalars(t *testing.T) {
	var clk int64 = 100
	src := nsAt("a", &clk)
	clk++
	if _, err := src.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	clk++
	if _, err := src.AcquireLease("/vol", "w"); err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	clk++
	if err := src.WriteExtent("/vol", 0, "c0", "w"); err != nil {
		t.Fatalf("WriteExtent: %v", err)
	}

	// The gossip snapshot carries the volume's existence and scalars but NOT its
	// extent map (which replicates to the replica set out of band).
	gb, err := src.GossipSnapshot()
	if err != nil {
		t.Fatalf("GossipSnapshot: %v", err)
	}
	dstG := nsAt("b", &clk)
	if err := dstG.MergeBytes(gb); err != nil {
		t.Fatalf("MergeBytes(gossip): %v", err)
	}
	if got, err := dstG.Extents("/vol"); err != nil || len(got) != 0 {
		t.Errorf("gossip-merged extents = (%v,%v), want empty", got, err)
	}
	if sz, err := dstG.ExtentSize("/vol"); err != nil || sz != 4096 {
		t.Errorf("gossip should still carry extent size: (%d,%v)", sz, err)
	}
	if lease, err := dstG.Lease("/vol"); err != nil || lease.Holder != "w" {
		t.Errorf("gossip should still carry the lease: (%+v,%v)", lease, err)
	}

	// The full snapshot (persist/backup) still carries the extent map.
	sb, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dstS := nsAt("c", &clk)
	if err := dstS.MergeBytes(sb); err != nil {
		t.Fatalf("MergeBytes(full): %v", err)
	}
	if got, err := dstS.Extents("/vol"); err != nil || got[0] != "c0" {
		t.Errorf("full snapshot should carry extents: (%v,%v)", got, err)
	}
}

func TestNamespace_CreateVolumeWriteReadExtents(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	if got, err := ns.ExtentSize("/vol"); err != nil || got != 4096 {
		t.Fatalf("ExtentSize = (%d,%v), want (4096,nil)", got, err)
	}

	clk++
	if _, err := ns.AcquireLease("/vol", "w"); err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	clk++
	if err := ns.WriteExtent("/vol", 0, "c0", "w"); err != nil {
		t.Fatalf("WriteExtent 0: %v", err)
	}
	clk++
	if err := ns.WriteExtent("/vol", 5, "c5", "w"); err != nil {
		t.Fatalf("WriteExtent 5: %v", err)
	}
	if got, err := ns.Extents("/vol"); err != nil || !reflect.DeepEqual(got, map[uint64]string{0: "c0", 5: "c5"}) {
		t.Fatalf("Extents = (%v,%v), want {0:c0 5:c5}", got, err)
	}

	// Per-extent lookup: a mapped extent and an unmapped one.
	if id, ok, err := ns.Extent("/vol", 5); err != nil || !ok || id != "c5" {
		t.Errorf("Extent(5) = (%q,%v,%v), want (c5,true,nil)", id, ok, err)
	}
	if id, ok, err := ns.Extent("/vol", 99); err != nil || ok || id != "" {
		t.Errorf("Extent(99) = (%q,%v,%v), want (\"\",false,nil)", id, ok, err)
	}
	if _, _, err := ns.Extent("/missing", 0); err == nil {
		t.Error("Extent on a missing volume should error")
	}

	// Overwriting a region under a newer HLC rebinds it (copy-on-write).
	clk++
	if err := ns.WriteExtent("/vol", 0, "c0-v2", "w"); err != nil {
		t.Fatalf("rewrite extent 0: %v", err)
	}
	if got, _ := ns.Extents("/vol"); got[0] != "c0-v2" {
		t.Errorf("extent 0 = %q, want c0-v2", got[0])
	}
}

func TestNamespace_VolumeSize(t *testing.T) {
	var clk int64 = 1
	ns := nsAt("a", &clk)

	// Without WithSize a volume's size is unset (zero).
	clk++
	if _, err := ns.CreateVolume("/nosize", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if got, _ := ns.Size("/nosize"); got != 0 {
		t.Errorf("unset size = %d, want 0", got)
	}

	// WithSize records the advertised device size and it survives the wire.
	clk++
	if _, err := ns.CreateVolume("/sized", 4096, namespace.WithSize(10<<20)); err != nil {
		t.Fatalf("CreateVolume sized: %v", err)
	}
	if got, _ := ns.Size("/sized"); got != 10<<20 {
		t.Errorf("size = %d, want %d", got, 10<<20)
	}
	if _, err := ns.Size("/missing"); err == nil {
		t.Error("Size of a missing volume should error")
	}

	state, _ := ns.Snapshot()
	clk++
	other := nsAt("b", &clk)
	if err := other.MergeBytes(state); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if got, _ := other.Size("/sized"); got != 10<<20 {
		t.Errorf("size after merge = %d, want %d", got, 10<<20)
	}
}

func TestNamespace_CreateVolumeDefaultsExtentSize(t *testing.T) {
	var clk int64 = 1
	ns := nsAt("a", &clk)
	for _, p := range []struct {
		path string
		size int64
	}{{"/zero", 0}, {"/neg", -5}} {
		clk++
		if _, err := ns.CreateVolume(p.path, p.size); err != nil {
			t.Fatalf("CreateVolume %s: %v", p.path, err)
		}
		if got, _ := ns.ExtentSize(p.path); got != namespace.DefaultExtentSize {
			t.Errorf("%s extent size = %d, want default %d", p.path, got, namespace.DefaultExtentSize)
		}
	}
}

func TestNamespace_CreateVolumeErrors(t *testing.T) {
	var clk int64 = 1
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.Mkdir("/dir"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	clk++
	if _, err := ns.CreateVolume("/dir", 4096); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("CreateVolume over an existing name: got %v, want exists", err)
	}
	if _, err := ns.CreateVolume("/", 4096); err == nil || !strings.Contains(err.Error(), "root") {
		t.Errorf("CreateVolume root: got %v", err)
	}
	if _, err := ns.CreateVolume("/a/../b", 4096); err == nil || !strings.Contains(err.Error(), `".."`) {
		t.Errorf("CreateVolume traversal: got %v", err)
	}
	if _, err := ns.CreateVolume("/missing/v", 4096); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("CreateVolume under missing parent: got %v", err)
	}
}

func TestNamespace_VolumeOpsRejectWrongTarget(t *testing.T) {
	var clk int64 = 1
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.Mkdir("/dir"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	clk++
	if _, err := ns.Touch("/file"); err != nil {
		t.Fatalf("Touch: %v", err)
	}

	cases := []struct {
		name string
		do   func() error
		want string
	}{
		{"write missing", func() error { return ns.WriteExtent("/nope", 0, "c", "w") }, "does not exist"},
		{"write under missing parent", func() error { return ns.WriteExtent("/gone/v", 0, "c", "w") }, "does not exist"},
		{"write a directory", func() error { return ns.WriteExtent("/dir", 0, "c", "w") }, "not a volume"},
		{"write a file", func() error { return ns.WriteExtent("/file", 0, "c", "w") }, "not a volume"},
		{"write root", func() error { return ns.WriteExtent("/", 0, "c", "w") }, "not a volume"},
		{"write traversal", func() error { return ns.WriteExtent("/a/../b", 0, "c", "w") }, `".."`},
		{"size of a file", func() error { _, err := ns.ExtentSize("/file"); return err }, "not a volume"},
		{"extents of a file", func() error { _, err := ns.Extents("/file"); return err }, "not a volume"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.do(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestNamespace_VolumePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ns.json")
	first, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := first.CreateVolume("/vol", 8192); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if _, err := first.AcquireLease("/vol", "w"); err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if err := first.WriteExtent("/vol", 2, "c2", "w"); err != nil {
		t.Fatalf("WriteExtent: %v", err)
	}

	second, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got, _ := second.ExtentSize("/vol"); got != 8192 {
		t.Errorf("persisted extent size = %d, want 8192", got)
	}
	if got, _ := second.Extents("/vol"); !reflect.DeepEqual(got, map[uint64]string{2: "c2"}) {
		t.Errorf("persisted extents = %v, want {2:c2}", got)
	}
}

func TestNamespace_VolumeConvergesAcrossReplicas(t *testing.T) {
	var clkA, clkB int64 = 10, 100 // b's clock runs ahead, so its writes win
	a := nsAt("a", &clkA)
	clkA++
	if _, err := a.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	clkA++
	if _, err := a.AcquireLease("/vol", "on-a"); err != nil {
		t.Fatalf("a acquire: %v", err)
	}
	clkA++
	if err := a.WriteExtent("/vol", 0, "a0", "on-a"); err != nil {
		t.Fatalf("a WriteExtent: %v", err)
	}

	state, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	b := nsAt("b", &clkB)
	if err := b.MergeBytes(state); err != nil {
		t.Fatalf("b merge: %v", err)
	}
	clkB++
	if _, err := b.AcquireLease("/vol", "on-b"); err != nil { // steal the lease
		t.Fatalf("b acquire: %v", err)
	}
	clkB++
	if err := b.WriteExtent("/vol", 0, "b0", "on-b"); err != nil { // overwrite, newer HLC
		t.Fatalf("b rewrite: %v", err)
	}
	clkB++
	if err := b.WriteExtent("/vol", 1, "b1", "on-b"); err != nil { // a new extent
		t.Fatalf("b WriteExtent: %v", err)
	}

	bState, err := b.Snapshot()
	if err != nil {
		t.Fatalf("b snapshot: %v", err)
	}
	if err := a.MergeBytes(bState); err != nil {
		t.Fatalf("a merge: %v", err)
	}
	if got, _ := a.Extents("/vol"); !reflect.DeepEqual(got, map[uint64]string{0: "b0", 1: "b1"}) {
		t.Errorf("converged extents = %v, want {0:b0 1:b1}", got)
	}
	if got, _ := a.ExtentSize("/vol"); got != 4096 {
		t.Errorf("converged extent size = %d, want 4096", got)
	}
}

func TestNamespace_SnapshotVolume(t *testing.T) {
	var clk int64 = 1
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.CreateVolume("/vol", 4096, namespace.WithSize(10<<20)); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	clk++
	if _, err := ns.AcquireLease("/vol", "w"); err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	clk++
	if err := ns.WriteExtent("/vol", 0, "c0", "w"); err != nil {
		t.Fatalf("WriteExtent 0: %v", err)
	}
	clk++
	if err := ns.WriteExtent("/vol", 3, "c3", "w"); err != nil {
		t.Fatalf("WriteExtent 3: %v", err)
	}

	// Snapshot freezes the current extent map and inherits size/extent size.
	clk++
	if _, err := ns.SnapshotVolume("/vol", "/snap"); err != nil {
		t.Fatalf("SnapshotVolume: %v", err)
	}
	if got, _ := ns.Extents("/snap"); !reflect.DeepEqual(got, map[uint64]string{0: "c0", 3: "c3"}) {
		t.Fatalf("snapshot extents = %v, want {0:c0 3:c3}", got)
	}
	if got, _ := ns.ExtentSize("/snap"); got != 4096 {
		t.Errorf("snapshot extent size = %d, want 4096", got)
	}
	if got, _ := ns.Size("/snap"); got != 10<<20 {
		t.Errorf("snapshot size = %d, want %d", got, 10<<20)
	}
	// The snapshot is created vacant — nobody holds its lease.
	if l, _ := ns.Lease("/snap"); l.Holder != "" {
		t.Errorf("snapshot lease holder = %q, want vacant", l.Holder)
	}

	// Copy-on-write divergence: writing the source after the snapshot rebinds
	// only the source's extent; the snapshot keeps the frozen chunk. And the
	// snapshot can be written independently (via its own lease) without
	// disturbing the source.
	clk++
	if err := ns.WriteExtent("/vol", 0, "c0-v2", "w"); err != nil {
		t.Fatalf("rewrite source extent 0: %v", err)
	}
	clk++
	if _, err := ns.AcquireLease("/snap", "s"); err != nil {
		t.Fatalf("AcquireLease snap: %v", err)
	}
	clk++
	if err := ns.WriteExtent("/snap", 3, "s3", "s"); err != nil {
		t.Fatalf("WriteExtent snap 3: %v", err)
	}
	if got, _ := ns.Extents("/vol"); !reflect.DeepEqual(got, map[uint64]string{0: "c0-v2", 3: "c3"}) {
		t.Errorf("source extents after divergence = %v, want {0:c0-v2 3:c3}", got)
	}
	if got, _ := ns.Extents("/snap"); !reflect.DeepEqual(got, map[uint64]string{0: "c0", 3: "s3"}) {
		t.Errorf("snapshot extents after divergence = %v, want {0:c0 3:s3}", got)
	}

	// The snapshot is a volume that lists and survives gossip merge.
	state, _ := ns.Snapshot()
	clk++
	other := nsAt("b", &clk)
	if err := other.MergeBytes(state); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if got, _ := other.Extents("/snap"); !reflect.DeepEqual(got, map[uint64]string{0: "c0", 3: "s3"}) {
		t.Errorf("snapshot extents after merge = %v, want {0:c0 3:s3}", got)
	}
}

func TestNamespace_SnapshotVolumeErrors(t *testing.T) {
	var clk int64 = 1
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.Touch("/file"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	clk++
	if _, err := ns.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	if _, err := ns.SnapshotVolume("/missing", "/snap"); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("snapshot of missing source: got %v, want not-exist", err)
	}
	if _, err := ns.SnapshotVolume("/file", "/snap"); err == nil || !strings.Contains(err.Error(), "not a volume") {
		t.Errorf("snapshot of a non-volume source: got %v, want not-a-volume", err)
	}
	if _, err := ns.SnapshotVolume("/vol", "/"); err == nil || !strings.Contains(err.Error(), "root") {
		t.Errorf("snapshot to root: got %v", err)
	}
	if _, err := ns.SnapshotVolume("/vol", "/a/../b"); err == nil || !strings.Contains(err.Error(), `".."`) {
		t.Errorf("snapshot to a traversal path: got %v", err)
	}
	if _, err := ns.SnapshotVolume("/vol", "/vol"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("snapshot over an existing name: got %v, want exists", err)
	}
	if _, err := ns.SnapshotVolume("/vol", "/missing/snap"); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("snapshot under a missing parent: got %v, want not-exist", err)
	}
}
