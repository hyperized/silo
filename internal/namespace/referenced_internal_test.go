package namespace

import (
	"testing"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

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
