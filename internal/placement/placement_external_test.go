package placement_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/hyperized/silo/internal/placement"
)

func TestNew_DedupsAndDropsEmpty(t *testing.T) {
	r := placement.New([]string{"b", "a", "", "a", "b"}, 8)
	if got, want := r.Nodes(), []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Nodes() = %v, want %v", got, want)
	}
	if r.Len() != 2 {
		t.Errorf("Len() = %d, want 2", r.Len())
	}
}

func TestReplicas_EmptyAndDegenerate(t *testing.T) {
	empty := placement.New(nil, 8)
	if empty.Len() != 0 {
		t.Errorf("empty ring Len() = %d, want 0", empty.Len())
	}
	if got := empty.Replicas("k", 3); got != nil {
		t.Errorf("Replicas on empty ring = %v, want nil", got)
	}

	r := placement.New([]string{"a", "b", "c"}, 16)
	if got := r.Replicas("k", 0); got != nil {
		t.Errorf("Replicas n=0 = %v, want nil", got)
	}
	if got := r.Replicas("k", -1); got != nil {
		t.Errorf("Replicas n=-1 = %v, want nil", got)
	}
}

func TestReplicas_DistinctAndPriorityStable(t *testing.T) {
	r := placement.New([]string{"a", "b", "c", "d", "e"}, 64)
	for _, key := range []string{"chunk-1", "chunk-2", "writer/epoch/9001", ""} {
		got := r.Replicas(key, 3)
		if len(got) != 3 {
			t.Fatalf("Replicas(%q, 3) returned %d ids, want 3: %v", key, len(got), got)
		}
		if !allDistinct(got) {
			t.Errorf("Replicas(%q, 3) = %v has duplicates", key, got)
		}
		// The primary must not change as the caller asks for more replicas.
		if primary := r.Replicas(key, 1); len(primary) != 1 || primary[0] != got[0] {
			t.Errorf("primary unstable for %q: n=1 gave %v, n=3 head was %q", key, primary, got[0])
		}
	}
}

func TestReplicas_CapsAtRingSize(t *testing.T) {
	r := placement.New([]string{"a", "b", "c"}, 32)
	got := r.Replicas("anything", 10)
	if len(got) != 3 {
		t.Fatalf("Replicas asking for 10 on a 3-node ring returned %d: %v", len(got), got)
	}
	if !allDistinct(got) {
		t.Errorf("over-asked Replicas returned duplicates: %v", got)
	}
}

func TestReplicas_DeterministicAcrossRings(t *testing.T) {
	// Two rings built from the same nodes in different input order must
	// agree on placement — that is what lets every silod compute the same
	// answer independently.
	a := placement.New([]string{"node-1", "node-2", "node-3", "node-4"}, 50)
	b := placement.New([]string{"node-4", "node-1", "node-3", "node-2"}, 50)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("chunk-%d", i)
		if ra, rb := a.Replicas(key, 3), b.Replicas(key, 3); !reflect.DeepEqual(ra, rb) {
			t.Fatalf("rings disagree on %q: %v vs %v", key, ra, rb)
		}
	}
}

func TestReplicas_WrapAroundHead(t *testing.T) {
	// A single node with one virtual point means a key hashing past that
	// point must wrap back to the ring head. Probing many keys guarantees
	// both the wrap and non-wrap paths execute, and every key must still
	// resolve to the only node.
	r := placement.New([]string{"solo"}, 1)
	for i := 0; i < 2000; i++ {
		got := r.Replicas(fmt.Sprintf("k-%d", i), 1)
		if len(got) != 1 || got[0] != "solo" {
			t.Fatalf("single-node ring returned %v for key %d, want [solo]", got, i)
		}
	}
}

func TestReplicas_MinimalMovementOnGrowth(t *testing.T) {
	const keys = 4000
	before := placement.New([]string{"a", "b", "c"}, placement.DefaultVNodes)
	after := placement.New([]string{"a", "b", "c", "d"}, placement.DefaultVNodes)

	moved := 0
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("chunk-%d", i)
		if before.Replicas(key, 1)[0] != after.Replicas(key, 1)[0] {
			moved++
		}
	}
	// Adding a 4th node should reassign roughly 1/4 of primaries. Allow
	// generous slack for hash variance; the point is that it is nowhere
	// near "everything moved", which a non-consistent scheme would do.
	if frac := float64(moved) / keys; frac > 0.40 {
		t.Errorf("primary reassignment fraction %.3f too high; consistent hashing should move ~0.25", frac)
	}
}

func TestNodes_ReturnsCopy(t *testing.T) {
	r := placement.New([]string{"a", "b"}, 8)
	got := r.Nodes()
	got[0] = "mutated"
	if again := r.Nodes(); again[0] != "a" {
		t.Errorf("mutating the returned slice changed the ring: %v", again)
	}
}

func allDistinct(ids []string) bool {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			return false
		}
		seen[id] = struct{}{}
	}
	return true
}
