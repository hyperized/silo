package namespace

import (
	"testing"
	"time"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

// GC (and any merge) reaps an inode once its directory link is removed: Remove
// only tombstones the link, leaving the inode behind until the pruner reclaims
// it. A still-linked inode is never touched.
func TestPruneUnreachable_GCReapsOrphanedInodes(t *testing.T) {
	n := New(hlc.New("a"))
	if _, err := n.Mkdir("/vols"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	aID, err := n.CreateVolume("/vols/a", 4096)
	if err != nil {
		t.Fatalf("CreateVolume a: %v", err)
	}
	bID, err := n.CreateVolume("/vols/b", 4096)
	if err != nil {
		t.Fatalf("CreateVolume b: %v", err)
	}

	if err := n.Remove("/vols/a"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	// Right after Remove the inode still lingers — only the link is tombstoned.
	if _, ok := n.inodes[aID]; !ok {
		t.Fatal("the inode should still be present immediately after Remove")
	}

	n.GC(time.Hour)
	if _, ok := n.inodes[aID]; ok {
		t.Error("GC should reap the orphaned inode")
	}
	if _, ok := n.inodes[bID]; !ok {
		t.Error("a still-linked inode must not be reaped")
	}
	if got := n.inodesReaped.Load(); got != 1 {
		t.Errorf("inodesReaped = %d, want 1", got)
	}
}

// ReferencedInodeIDs must terminate and resolve each inode once even on a
// cyclic or dangling structure — shapes a corrupt merge could produce — so the
// reaper that consumes it can never loop forever. The seen-set guard handles a
// back-edge; the nil-inode guard handles a link to a missing inode.
func TestReferencedInodeIDs_CycleAndDanglingSafe(t *testing.T) {
	n := New(hlc.New("a"))
	tag := hlc.Timestamp{Wall: 100, Node: "x"}

	// A directory 'd' under root that links back to root (a cycle).
	d := &Inode{ID: "d", Type: Dir, children: crdt.NewORSet[Entry]()}
	n.inodes["d"] = d
	n.inodes[rootID].children.Add(Entry{Name: "d", Inode: "d"}, tag)
	d.children.Add(Entry{Name: "up", Inode: rootID}, tag) // back-edge: root already seen

	// A link to an inode that does not exist (dangling reference).
	n.inodes[rootID].children.Add(Entry{Name: "ghost", Inode: "ghost"}, tag)

	live := n.ReferencedInodeIDs()
	for _, id := range []string{rootID, "d", "ghost"} {
		if _, ok := live[id]; !ok {
			t.Errorf("%q should be in the referenced set", id)
		}
	}
	if len(live) != 3 {
		t.Errorf("a cycle/dangling structure must resolve each id once, got %d: %v", len(live), live)
	}
}

// LiveChunkRefs must survive the same corrupt shapes a bad merge can produce: a
// link to a missing inode (nil-inode guard) and File/Volume inodes whose CRDT
// payloads are nil (nil-manifest / nil-extents guards). It must collect what it
// can without panicking.
func TestLiveChunkRefs_CorruptShapesSafe(t *testing.T) {
	n := New(hlc.New("a"))
	tag := hlc.Timestamp{Wall: 100, Node: "x"}

	// A File with a normal manifest contributes its chunks.
	good := &Inode{ID: "good", Type: File, manifest: crdt.NewORSet[string]()}
	good.manifest.Add("c-good", tag)
	n.inodes["good"] = good
	n.inodes[rootID].children.Add(Entry{Name: "good", Inode: "good"}, tag)

	// A File with a nil manifest (corrupt) is skipped, not dereferenced.
	n.inodes["nilman"] = &Inode{ID: "nilman", Type: File}
	n.inodes[rootID].children.Add(Entry{Name: "nilman", Inode: "nilman"}, tag)

	// A Volume with nil extents (corrupt) still reports its id but adds no chunks.
	n.inodes["nilext"] = &Inode{ID: "nilext", Type: Volume}
	n.inodes[rootID].children.Add(Entry{Name: "nilext", Inode: "nilext"}, tag)

	// A dangling link to a missing inode is skipped by the nil-inode guard.
	n.inodes[rootID].children.Add(Entry{Name: "ghost", Inode: "ghost"}, tag)

	chunks, volumes := n.LiveChunkRefs()
	if _, ok := chunks["c-good"]; !ok || len(chunks) != 1 {
		t.Errorf("chunks = %v, want only {c-good}", chunks)
	}
	if _, ok := volumes["nilext"]; !ok || len(volumes) != 1 {
		t.Errorf("volumes = %v, want only {nilext}", volumes)
	}
}
