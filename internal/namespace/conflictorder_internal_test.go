package namespace

import (
	"testing"

	"github.com/hyperized/silo/internal/hlc"
)

// TestConflictOrder_DeterministicTieBreak verifies that when two sibling
// entries carry the same live HLC tag, the bare name and the lookup target are
// chosen deterministically by inode id — identically on every replica — rather
// than by map iteration order. Equal tags are unreachable under normal HLC
// monotonicity (the Node field disambiguates every event), so this exercises
// the defensive total-ordering tie-break that keeps convergence from depending
// on that global invariant.
func TestConflictOrder_DeterministicTieBreak(t *testing.T) {
	n := New(hlc.New("a"))
	root := n.inodes[rootID]
	tag := hlc.Timestamp{Wall: 100, Logical: 0, Node: "x"}
	// Same name, same tag, different inode ids.
	root.children.Add(Entry{Name: "f", Inode: "aaa"}, tag)
	root.children.Add(Entry{Name: "f", Inode: "zzz"}, tag)

	entries, err := n.List("/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var bare, conflict string
	for _, e := range entries {
		if e.Conflict {
			conflict = e.Inode
		} else {
			bare = e.Inode
		}
	}
	if bare != "zzz" {
		t.Errorf("bare-name inode = %q, want zzz (greater inode wins the tie)", bare)
	}
	if conflict != "aaa" {
		t.Errorf("conflict-sibling inode = %q, want aaa", conflict)
	}

	// primaryChildLocked must follow the same total order so a path lookup
	// resolves to whichever inode List left holding the bare name.
	n.mu.Lock()
	got, ok := n.primaryChildLocked(root, "f")
	n.mu.Unlock()
	if !ok || got != "zzz" {
		t.Errorf("primaryChildLocked = %q,%v, want zzz,true", got, ok)
	}
}

// TestManifest_DeterministicTieBreak verifies the same total-ordering property
// for a file's chunk manifest: two chunk ids appended at the same HLC tag read
// back in a deterministic (id-ascending) order on every replica.
func TestManifest_DeterministicTieBreak(t *testing.T) {
	n := New(hlc.New("a"))
	id, err := n.Touch("/file")
	if err != nil {
		t.Fatalf("Touch: %v", err)
	}
	tag := hlc.Timestamp{Wall: 100, Logical: 0, Node: "x"}
	file := n.inodes[id]
	file.manifest.Add("chunk-z", tag)
	file.manifest.Add("chunk-a", tag)

	ids, err := n.Manifest("/file")
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if len(ids) != 2 || ids[0] != "chunk-a" || ids[1] != "chunk-z" {
		t.Errorf("Manifest order = %v, want [chunk-a chunk-z] (id-ascending tie-break)", ids)
	}
}
