package placement_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/hyperized/silo/internal/placement"
)

// FuzzReplicas checks the ring's placement invariants across arbitrary
// node counts, replica counts, and keys: the result is always distinct
// node ids drawn from the ring, its length is min(n, ring size), and two
// rings built from the same nodes in different order agree (the property
// that lets every silod compute placement independently).
func FuzzReplicas(f *testing.F) {
	f.Add(uint8(3), uint8(3), []byte("chunk-1"))
	f.Add(uint8(1), uint8(5), []byte(""))
	f.Add(uint8(0), uint8(0), []byte("x"))

	f.Fuzz(func(t *testing.T, nodeCount, want uint8, key []byte) {
		numNodes := int(nodeCount)%8 + 1
		nodes := make([]string, numNodes)
		valid := make(map[string]bool, numNodes)
		for i := range nodes {
			nodes[i] = fmt.Sprintf("node-%d", i)
			valid[nodes[i]] = true
		}
		n := int(want) % 10

		got := placement.New(nodes, 0).Replicas(string(key), n)

		seen := map[string]bool{}
		for _, id := range got {
			if seen[id] {
				t.Fatalf("Replicas returned a duplicate %q: %v", id, got)
			}
			seen[id] = true
			if !valid[id] {
				t.Fatalf("Replicas returned an unknown node %q", id)
			}
		}
		expected := n
		if expected > numNodes {
			expected = numNodes
		}
		if n <= 0 {
			expected = 0
		}
		if len(got) != expected {
			t.Fatalf("len(Replicas)=%d, want %d (n=%d, nodes=%d)", len(got), expected, n, numNodes)
		}

		// A ring built from the same nodes in a different order must agree.
		shuffled := append([]string(nil), nodes...)
		for i := range shuffled {
			j := (i*7 + 3) % len(shuffled)
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		}
		if other := placement.New(shuffled, 0).Replicas(string(key), n); !reflect.DeepEqual(got, other) {
			t.Fatalf("placement is not order-independent: %v vs %v", got, other)
		}
	})
}
