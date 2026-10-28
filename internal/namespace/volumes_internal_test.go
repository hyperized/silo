package namespace

import (
	"testing"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

// A corrupt merge can leave a directory that links back to an ancestor, or an
// entry whose inode never arrived. Volumes must still terminate and skip both.
func TestVolumes_CycleAndDanglingSafe(t *testing.T) {
	n := New(hlc.New("a"))
	tag := hlc.Timestamp{Wall: 100, Node: "x"}

	d := &Inode{ID: "d", Type: Dir, children: crdt.NewORSet[Entry]()}
	n.inodes["d"] = d
	n.inodes["v"] = &Inode{ID: "v", Type: Volume, ExtentSize: 512}
	n.inodes[rootID].children.Add(Entry{Name: "d", Inode: "d"}, tag)
	n.inodes[rootID].children.Add(Entry{Name: "again", Inode: "d"}, tag)
	d.children.Add(Entry{Name: "up", Inode: rootID}, tag)
	d.children.Add(Entry{Name: "ghost", Inode: "missing"}, tag)
	d.children.Add(Entry{Name: "vol", Inode: "v"}, tag)

	got := n.Volumes()
	if len(got) != 1 || got[0].Inode != "v" || got[0].InlineExtents != 0 {
		t.Fatalf("Volumes = %+v, want exactly the one volume v with no inline extents", got)
	}
}
