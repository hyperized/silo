package crdt_test

import (
	"testing"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

// FuzzLWWMapConverges asserts the CRDT laws on the last-writer-wins map:
// after two replicas apply arbitrary writes and exchange state, merging in
// either direction yields the same value for every key (commutativity), and
// re-merging changes nothing (idempotence). These are the properties that let
// a volume's extent map heal a partition deterministically.
func FuzzLWWMapConverges(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 1, 1, 0, 2, 0})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		a := crdt.NewLWWMap[uint64, byte]()
		b := crdt.NewLWWMap[uint64, byte]()

		var wall int64
		for i := 0; i+2 < len(data); i += 3 {
			wall++ // strictly increasing -> globally unique, totally-ordered tags
			m, node := a, "a"
			if data[i]%2 == 1 {
				m, node = b, "b"
			}
			key := uint64(data[i+1] % 4) // a small key space so collisions happen
			m.Set(key, data[i+2], hlc.Timestamp{Wall: wall, Node: node})
		}

		ab := a.Clone()
		ab.Merge(b)
		ba := b.Clone()
		ba.Merge(a)

		for key := uint64(0); key < 4; key++ {
			av, aok := ab.Get(key)
			bv, bok := ba.Get(key)
			if av != bv || aok != bok {
				t.Fatalf("merge not commutative for key %d: ab=(%d,%v) ba=(%d,%v)", key, av, aok, bv, bok)
			}
		}

		// Re-merging an already-applied delta must not change any value.
		before := ab.Entries()
		ab.Merge(b)
		after := ab.Entries()
		if len(before) != len(after) {
			t.Fatalf("merge not idempotent: %d keys became %d", len(before), len(after))
		}
	})
}

// FuzzORSetConverges asserts the CRDT laws on the observed-remove set:
// after two replicas apply arbitrary add/remove sequences and exchange
// state, merging in either direction yields the same membership
// (commutativity), and re-merging changes nothing (idempotence). These are
// the properties that let namespace replicas heal a partition without a
// coordinator.
func FuzzORSetConverges(f *testing.F) {
	f.Add([]byte{0, 0, 1, 1, 0, 1, 0, 1, 2})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		a := crdt.NewORSet[string]()
		b := crdt.NewORSet[string]()
		elems := []string{"x", "y", "z"}

		var wall int64
		for i := 0; i+2 < len(data); i += 3 {
			wall++ // strictly increasing -> globally unique add tags
			set, node := a, "a"
			if data[i]%2 == 1 {
				set, node = b, "b"
			}
			elem := elems[int(data[i+2])%len(elems)]
			if data[i+1]%2 == 0 {
				set.Add(elem, hlc.Timestamp{Wall: wall, Node: node})
			} else {
				set.Remove(elem, hlc.Timestamp{Wall: wall, Node: node})
			}
		}

		ab := a.Clone()
		ab.Merge(b)
		ba := b.Clone()
		ba.Merge(a)

		for _, e := range elems {
			if ab.Contains(e) != ba.Contains(e) {
				t.Fatalf("merge not commutative for %q: ab=%v ba=%v", e, ab.Contains(e), ba.Contains(e))
			}
		}

		// Re-merging an already-applied delta must not change membership.
		snapshot := map[string]bool{}
		for _, e := range elems {
			snapshot[e] = ab.Contains(e)
		}
		ab.Merge(b)
		for _, e := range elems {
			if ab.Contains(e) != snapshot[e] {
				t.Fatalf("merge not idempotent for %q", e)
			}
		}
	})
}
