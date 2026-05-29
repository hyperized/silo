package crdt_test

import (
	"sort"
	"testing"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

func ts(wall int64, node string) hlc.Timestamp {
	return hlc.Timestamp{Wall: wall, Node: node}
}

func TestLWWRegister_Merge(t *testing.T) {
	early := crdt.Set("old", ts(1, "a"))
	late := crdt.Set("new", ts(2, "a"))

	if got := early.Merge(late); got.Value != "new" {
		t.Errorf("later write should win: got %q", got.Value)
	}
	if got := late.Merge(early); got.Value != "new" {
		t.Errorf("merge is order-independent: got %q", got.Value)
	}
	// A zero-timestamp register loses to any real write.
	var zero crdt.LWWRegister[string]
	if got := zero.Merge(early); got.Value != "old" {
		t.Errorf("real write should beat the zero register: got %q", got.Value)
	}
	// Equal timestamps keep the receiver (the node tiebreaker means equal
	// (wall,logical,node) only happens for the identical write).
	same := crdt.Set("keep", ts(5, "a"))
	if got := same.Merge(crdt.Set("other", ts(5, "a"))); got.Value != "keep" {
		t.Errorf("equal timestamps keep receiver: got %q", got.Value)
	}
}

func TestORSet_AddRemoveContains(t *testing.T) {
	s := crdt.NewORSet[string]()
	if s.Contains("x") {
		t.Error("empty set contains nothing")
	}
	s.Add("x", ts(1, "a"))
	if !s.Contains("x") {
		t.Error("x should be present after Add")
	}
	s.Remove("x", ts(100, "gc"))
	if s.Contains("x") {
		t.Error("x should be gone after Remove tombstones its tag")
	}
	// Removing an absent element is a no-op.
	s.Remove("never-added", ts(100, "gc"))

	// Re-adding with a fresh tag revives the element.
	s.Add("x", ts(2, "a"))
	if !s.Contains("x") {
		t.Error("x should be present again after re-Add with a new tag")
	}
}

func TestORSet_Elements(t *testing.T) {
	s := crdt.NewORSet[string]()
	s.Add("a", ts(1, "n"))
	s.Add("b", ts(2, "n"))
	s.Add("c", ts(3, "n"))
	s.Remove("b", ts(100, "gc"))

	got := s.Elements()
	sort.Strings(got)
	if len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Errorf("Elements = %v, want [a c]", got)
	}
}

func TestORSet_LiveTag(t *testing.T) {
	s := crdt.NewORSet[string]()
	if _, ok := s.LiveTag("absent"); ok {
		t.Error("absent element has no live tag")
	}
	s.Add("x", ts(1, "a"))
	s.Add("x", ts(3, "a")) // a later claim of the same element
	s.Add("x", ts(2, "a"))
	got, ok := s.LiveTag("x")
	if !ok || got.Compare(ts(3, "a")) != 0 {
		t.Errorf("LiveTag = %v (ok=%v), want the greatest tag ts(3,a)", got, ok)
	}
	s.Remove("x", ts(100, "gc"))
	if _, ok := s.LiveTag("x"); ok {
		t.Error("LiveTag should report absent once all tags are tombstoned")
	}
}

func TestORSet_AddWinsOverConcurrentRemove(t *testing.T) {
	// Replica A adds x@t1. B learns it, then removes x (tombstoning t1).
	// Concurrently A re-observes... model: A adds x again @t2 (a tag B never
	// saw). After merge, t2 is live and t1 is tombstoned -> x present.
	a := crdt.NewORSet[string]()
	a.Add("x", ts(1, "a"))

	b := a.Clone()
	b.Remove("x", ts(100, "gc")) // tombstones t1, the only tag B has seen

	a.Add("x", ts(2, "a")) // concurrent add with a tag B never observed

	a.Merge(b)
	if !a.Contains("x") {
		t.Error("an add concurrent with a remove must survive (add wins)")
	}
}

func TestORSet_GC(t *testing.T) {
	s := crdt.NewORSet[string]()
	s.Add("old", ts(1, "a"))
	s.Add("recent", ts(2, "a"))
	s.Add("live", ts(3, "a"))
	// "revived" is removed then re-added with a fresh tag, so it keeps a
	// live tag even after its old tombstone is reclaimed.
	s.Add("revived", ts(4, "a"))
	s.Remove("revived", ts(50, "a"))
	s.Add("revived", ts(60, "a"))

	s.Remove("old", ts(100, "a"))    // tombstoned at wall 100
	s.Remove("recent", ts(500, "a")) // tombstoned at wall 500

	// Cutoff wall 300 reclaims tombstones removed at or before it: "old"
	// (100) and "revived"'s old tag (50); "recent" (500) is kept.
	if got := s.GC(ts(300, "z")); got != 2 {
		t.Fatalf("GC reclaimed %d, want 2 (old + revived's stale tag)", got)
	}
	if s.Contains("old") {
		t.Error("old must stay absent after GC")
	}
	if s.Contains("recent") {
		t.Error("recent's tombstone is too recent to GC; it must stay removed")
	}
	if !s.Contains("live") {
		t.Error("an element with no tombstone must survive GC")
	}
	if !s.Contains("revived") {
		t.Error("a re-added element must survive GC of its stale tombstone")
	}

	// A later cutoff finally reclaims recent.
	if got := s.GC(ts(1000, "z")); got != 1 {
		t.Errorf("later GC reclaimed %d, want 1 (recent)", got)
	}
}

func TestORSet_ExportImportRoundTrip(t *testing.T) {
	src := crdt.NewORSet[string]()
	src.Add("keep", ts(1, "a"))
	src.Add("drop", ts(2, "a"))
	src.Remove("drop", ts(100, "gc"))

	adds, removes := src.Export()
	dst := crdt.NewORSet[string]()
	dst.Import(adds, removes)

	if !dst.Contains("keep") {
		t.Error("keep should survive an export/import round-trip")
	}
	if dst.Contains("drop") {
		t.Error("a removed element must stay removed after import (tombstone carried)")
	}
}

func TestORSet_MergeConverges(t *testing.T) {
	a := crdt.NewORSet[string]()
	a.Add("keep", ts(1, "a"))
	a.Add("drop", ts(2, "a"))
	a.Remove("drop", ts(100, "gc"))

	b := crdt.NewORSet[string]()
	b.Add("other", ts(3, "b"))

	ab := a.Clone()
	ab.Merge(b)
	ba := b.Clone()
	ba.Merge(a)

	for _, elem := range []string{"keep", "drop", "other"} {
		if ab.Contains(elem) != ba.Contains(elem) {
			t.Errorf("merge not commutative for %q: ab=%v ba=%v", elem, ab.Contains(elem), ba.Contains(elem))
		}
	}
	if !ab.Contains("keep") || ab.Contains("drop") || !ab.Contains("other") {
		t.Errorf("converged state wrong: keep=%v drop=%v other=%v",
			ab.Contains("keep"), ab.Contains("drop"), ab.Contains("other"))
	}

	// Idempotent: merging the same delta again changes nothing.
	ab.Merge(b)
	if !ab.Contains("keep") || ab.Contains("drop") || !ab.Contains("other") {
		t.Error("merge is not idempotent")
	}
}
