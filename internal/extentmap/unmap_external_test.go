package extentmap_test

import (
	"bytes"
	"testing"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/extentmap"
)

// An unmap is an entry, not a missing key: Get and Len treat it as unmapped,
// while Snapshot still carries it so peers receive it.
func TestStore_UnmappedEntry(t *testing.T) {
	s := extentmap.New(discard())
	if err := s.SetBatch("vol", []uint64{0, 1, 2}, []string{"c0", "c1", "c2"}, ts(10)); err != nil {
		t.Fatalf("SetBatch: %v", err)
	}
	if err := s.SetBatch("vol", []uint64{1, 2}, []string{extentmap.Unmapped, extentmap.Unmapped}, ts(20)); err != nil {
		t.Fatalf("unmap: %v", err)
	}
	if id, ok := s.Get("vol", 1); ok || id != "" {
		t.Errorf("Get(1) after unmap = (%q, %v), want (\"\", false)", id, ok)
	}
	if id, ok := s.Get("vol", 0); !ok || id != "c0" {
		t.Errorf("Get(0) = (%q, %v), want (c0, true)", id, ok)
	}
	if got := s.Len("vol"); got != 1 {
		t.Errorf("Len = %d, want 1 mapped extent", got)
	}
	snap := s.Snapshot("vol")
	if len(snap) != 3 || snap[1].Value != extentmap.Unmapped || snap[1].TS != ts(20) {
		t.Errorf("Snapshot = %+v, want three entries with extent 1 unmapped at ts 20", snap)
	}

	// Unmapping again, or unmapping an extent never written, does not count
	// below zero or double-count.
	if err := s.Set("vol", 1, extentmap.Unmapped, ts(30)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s.Set("vol", 9, extentmap.Unmapped, ts(30)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := s.Len("vol"); got != 1 {
		t.Errorf("Len after redundant unmaps = %d, want 1", got)
	}
	// Remapping a trimmed extent counts it again.
	if err := s.Set("vol", 1, "c1b", ts(40)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := s.Len("vol"); got != 2 {
		t.Errorf("Len after remap = %d, want 2", got)
	}
}

// The unmap carries its own HLC stamp, so it beats an older write whichever
// order the two reach a replica in, and loses to a newer one.
func TestStore_UnmapOrdersLikeAWrite(t *testing.T) {
	older := []crdt.MapEntry[uint64, string]{{Key: 4, Value: "old", TS: ts(10)}}
	unmap := []crdt.MapEntry[uint64, string]{{Key: 4, Value: extentmap.Unmapped, TS: ts(20)}}
	newer := []crdt.MapEntry[uint64, string]{{Key: 4, Value: "new", TS: ts(30)}}
	for _, tc := range []struct {
		name    string
		order   [][]crdt.MapEntry[uint64, string]
		wantID  string
		wantMap bool
		wantLen int
	}{
		{name: "unmap after older write", order: [][]crdt.MapEntry[uint64, string]{older, unmap}, wantLen: 0},
		{name: "older write arrives late", order: [][]crdt.MapEntry[uint64, string]{unmap, older}, wantLen: 0},
		{name: "newer write beats unmap", order: [][]crdt.MapEntry[uint64, string]{unmap, newer}, wantID: "new", wantMap: true, wantLen: 1},
		{name: "late unmap loses to newer write", order: [][]crdt.MapEntry[uint64, string]{newer, unmap}, wantID: "new", wantMap: true, wantLen: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := extentmap.New(discard())
			for _, batch := range tc.order {
				if err := s.Merge("vol", batch); err != nil {
					t.Fatalf("Merge: %v", err)
				}
			}
			if id, ok := s.Get("vol", 4); id != tc.wantID || ok != tc.wantMap {
				t.Errorf("Get = (%q, %v), want (%q, %v)", id, ok, tc.wantID, tc.wantMap)
			}
			if got := s.Len("vol"); got != tc.wantLen {
				t.Errorf("Len = %d, want %d", got, tc.wantLen)
			}
		})
	}
}

// A replica holding the binding and one holding the unmap must not look equal
// to the scrubber.
func TestStore_DigestSeesUnmap(t *testing.T) {
	a, b := extentmap.New(discard()), extentmap.New(discard())
	for _, s := range []*extentmap.Store{a, b} {
		if err := s.Set("vol", 0, "c0", ts(10)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := a.Set("vol", 0, extentmap.Unmapped, ts(20)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if bytes.Equal(a.Digest("vol"), b.Digest("vol")) {
		t.Fatal("a replica missing the unmap has the same digest as one holding it")
	}
	if err := b.Merge("vol", a.Snapshot("vol")); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if !bytes.Equal(a.Digest("vol"), b.Digest("vol")) {
		t.Error("digests still differ after the unmap was merged")
	}
}

// Unmaps survive a restart through both the log and a compacted snapshot, and
// the mapped count is rebuilt from them.
func TestStore_UnmapPersists(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []extentmap.Option
	}{
		{name: "log replay"},
		{name: "compacted snapshot", opts: []extentmap.Option{extentmap.WithCompactRecords(1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := extentmap.Open(dir, discard(), tc.opts...)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := s.SetBatch("vol", []uint64{0, 1}, []string{"c0", "c1"}, ts(10)); err != nil {
				t.Fatalf("SetBatch: %v", err)
			}
			if err := s.Set("vol", 1, extentmap.Unmapped, ts(20)); err != nil {
				t.Fatalf("unmap: %v", err)
			}
			want := s.Digest("vol")

			re, err := extentmap.Open(dir, discard())
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if _, ok := re.Get("vol", 1); ok {
				t.Error("the unmapped extent came back mapped after a restart")
			}
			if got := re.Len("vol"); got != 1 {
				t.Errorf("Len after restart = %d, want 1", got)
			}
			if !bytes.Equal(re.Digest("vol"), want) {
				t.Error("digest changed across the restart")
			}
		})
	}
}
