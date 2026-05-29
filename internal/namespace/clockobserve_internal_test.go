package namespace

import (
	"testing"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

func ts(node string, wall int64) hlc.Timestamp { return hlc.Timestamp{Wall: wall, Node: node} }

func TestObservePeerClocks_PicksHighestForeignTimestamp(t *testing.T) {
	const self = "me"
	var gotNode string
	var gotWall int64
	calls := 0
	n := New(hlc.New(self), WithPeerClockObserver(func(node string, wall int64) {
		gotNode, gotWall = node, wall
		calls++
	}))

	// Every timestamp-bearing field carries a foreign timestamp; one Add tag
	// is from self with the largest wall of all, to prove self is skipped.
	w := wireNamespace{Inodes: []wireInode{
		{
			ID: "d", Type: Dir,
			ACLTS: ts("peer-b", 50),
			Adds: []crdt.ElementTags[Entry]{{
				Elem: Entry{Name: "f", Inode: "x"},
				Tags: []hlc.Timestamp{ts("peer-a", 100), ts(self, 9999)},
			}},
			Removes: []crdt.ElementTombstones[Entry]{{
				Elem:       Entry{Name: "g", Inode: "y"},
				Tombstones: []crdt.Tombstone{{Add: ts("peer-a", 120), At: ts("peer-c", 200)}},
			}},
		},
		{
			ID: "f", Type: File,
			ManifestAdds: []crdt.ElementTags[string]{{Elem: "c0", Tags: []hlc.Timestamp{ts("peer-a", 150)}}},
			ManifestRemoves: []crdt.ElementTombstones[string]{{
				Elem:       "c0",
				Tombstones: []crdt.Tombstone{{Add: ts("peer-a", 160), At: ts("peer-d", 175)}},
			}},
		},
		{
			ID: "v", Type: Volume, ExtentSize: 4096,
			Extents:     []crdt.MapEntry[uint64, string]{{Key: 0, Value: "c", TS: ts("peer-e", 250)}},
			LeaseHolder: "peer-f",
			LeaseTS:     ptr(ts("peer-f", 300)),
		},
	}}

	n.observePeerClocks(w)

	if calls != 1 {
		t.Fatalf("observer calls = %d, want 1", calls)
	}
	// The highest foreign wall is 300 (peer-f, the volume lease), beating the
	// self tag at 9999, the extent at 250, and the tombstone at 200.
	if gotNode != "peer-f" || gotWall != 300 {
		t.Errorf("observed (%s, %d), want (peer-f, 300)", gotNode, gotWall)
	}
}

func ptr(t hlc.Timestamp) *hlc.Timestamp { return &t }

func TestObservePeerClocks_NoForeignTimestampNoCall(t *testing.T) {
	const self = "me"
	called := false
	n := New(hlc.New(self), WithPeerClockObserver(func(string, int64) { called = true }))

	// Only this node's own tag and an unset (empty-node) tag — both skipped.
	w := wireNamespace{Inodes: []wireInode{{
		ID: "d", Type: Dir,
		Adds: []crdt.ElementTags[Entry]{{Elem: Entry{Name: "f"}, Tags: []hlc.Timestamp{ts(self, 1), {}}}},
	}}}

	n.observePeerClocks(w)
	if called {
		t.Error("observer fired with no foreign timestamps present")
	}
}

func TestObservePeerClocks_GuardsNilObserverAndNilClock(t *testing.T) {
	foreign := wireNamespace{Inodes: []wireInode{{ID: "d", Type: Dir, ACLTS: ts("peer", 1)}}}

	// No observer registered: nothing to do, must not panic.
	New(hlc.New("me")).observePeerClocks(foreign)

	// Observer set but the namespace has no clock (only Merge sources are
	// clock-less, and those never reach here, but the guard keeps it safe).
	called := false
	n := New(nil, WithPeerClockObserver(func(string, int64) { called = true }))
	n.observePeerClocks(foreign)
	if called {
		t.Error("observer fired despite a nil clock")
	}
}
