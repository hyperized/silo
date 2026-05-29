package crdt_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

func tsAt(node string, wall int64) hlc.Timestamp { return hlc.Timestamp{Wall: wall, Node: node} }

func TestLWWMap_SetGetNewerWins(t *testing.T) {
	m := crdt.NewLWWMap[uint64, string]()

	if _, ok := m.Get(7); ok {
		t.Error("unset key reported as set")
	}

	m.Set(7, "a", tsAt("n", 10))
	if v, ok := m.Get(7); !ok || v != "a" {
		t.Errorf("after first set = (%q,%v), want (a,true)", v, ok)
	}

	m.Set(7, "b", tsAt("n", 20)) // newer wins
	if v, _ := m.Get(7); v != "b" {
		t.Errorf("newer set = %q, want b", v)
	}

	m.Set(7, "stale", tsAt("n", 5)) // older is ignored
	if v, _ := m.Get(7); v != "b" {
		t.Errorf("older set overwrote newer: got %q, want b", v)
	}
}

func TestLWWMap_LenAndEntries(t *testing.T) {
	m := crdt.NewLWWMap[uint64, string]()
	m.Set(1, "x", tsAt("n", 1))
	m.Set(2, "y", tsAt("n", 2))
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m.Len())
	}
	got := map[uint64]string{}
	for _, e := range m.Entries() {
		got[e.Key] = e.Value
	}
	if !reflect.DeepEqual(got, map[uint64]string{1: "x", 2: "y"}) {
		t.Errorf("Entries = %v", got)
	}
}

func TestLWWMap_MergeConverges(t *testing.T) {
	a := crdt.NewLWWMap[uint64, string]()
	b := crdt.NewLWWMap[uint64, string]()

	a.Set(1, "a-old", tsAt("a", 10))
	b.Set(1, "b-new", tsAt("b", 20)) // newer claim on the shared key
	a.Set(2, "only-a", tsAt("a", 5))
	b.Set(3, "only-b", tsAt("b", 5))

	ab := a.Clone()
	ab.Merge(b)
	ba := b.Clone()
	ba.Merge(a)

	for _, key := range []uint64{1, 2, 3} {
		av, aok := ab.Get(key)
		bv, bok := ba.Get(key)
		if av != bv || aok != bok {
			t.Errorf("key %d diverged: ab=(%q,%v) ba=(%q,%v)", key, av, aok, bv, bok)
		}
	}
	if v, _ := ab.Get(1); v != "b-new" {
		t.Errorf("shared key resolved to %q, want b-new (higher HLC)", v)
	}
}

func TestLWWMap_CloneIsIndependent(t *testing.T) {
	m := crdt.NewLWWMap[uint64, string]()
	m.Set(1, "orig", tsAt("n", 10))
	clone := m.Clone()
	m.Set(1, "mutated", tsAt("n", 20))
	if v, _ := clone.Get(1); v != "orig" {
		t.Errorf("clone changed with the original: got %q, want orig", v)
	}
}

func TestLWWMap_ImportMatchesMerge(t *testing.T) {
	src := crdt.NewLWWMap[uint64, string]()
	src.Set(1, "a", tsAt("n", 1))
	src.Set(2, "b", tsAt("n", 2))

	imported := crdt.NewLWWMap[uint64, string]()
	imported.Import(src.Entries())

	want := src.Entries()
	got := imported.Entries()
	sort.Slice(want, func(i, j int) bool { return want[i].Key < want[j].Key })
	sort.Slice(got, func(i, j int) bool { return got[i].Key < got[j].Key })
	if !reflect.DeepEqual(got, want) {
		t.Errorf("import round-trip = %v, want %v", got, want)
	}
}
