package crdt_test

import (
	"testing"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

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
				set.Remove(elem)
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
