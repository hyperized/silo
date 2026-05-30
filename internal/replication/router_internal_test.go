package replication

import (
	"strconv"
	"testing"

	"github.com/hyperized/silo/internal/membership"
)

func node(id string, capacity int64) membership.Node {
	return membership.Node{ID: id, CapacityBytes: capacity, State: membership.StateAlive}
}

func TestCapacityWeights(t *testing.T) {
	// Fewer than two nodes with capacity -> nil (equal weights).
	if w := capacityWeights([]membership.Node{node("a", 0), node("b", 0)}); w != nil {
		t.Errorf("no capacity = %v, want nil", w)
	}
	if w := capacityWeights([]membership.Node{node("a", 1000), node("b", 0)}); w != nil {
		t.Errorf("one capacity = %v, want nil", w)
	}

	// Equal capacities -> nil (no reshuffle for a homogeneous cluster).
	if w := capacityWeights([]membership.Node{node("a", 1000), node("b", 1000)}); w != nil {
		t.Errorf("equal capacity = %v, want nil", w)
	}

	// Skewed capacities -> proportional weights, smallest = 1.
	w := capacityWeights([]membership.Node{node("small", 1000), node("big", 4000), node("mid", 2000)})
	if w["small"] != 1 || w["big"] != 4 || w["mid"] != 2 {
		t.Errorf("weights = %v, want small=1 big=4 mid=2", w)
	}

	// A node without advertised capacity is treated as the smallest (weight 1).
	w = capacityWeights([]membership.Node{node("a", 1000), node("b", 3000), node("c", 0)})
	if w["c"] != 1 || w["b"] != 3 {
		t.Errorf("weights = %v, want c=1 b=3", w)
	}

	// Huge ratios are capped.
	w = capacityWeights([]membership.Node{node("tiny", 1), node("huge", 1<<40)})
	if w["huge"] != maxCapacityWeight {
		t.Errorf("huge weight = %d, want cap %d", w["huge"], maxCapacityWeight)
	}
}

func TestRingSignatureChangesWithWeights(t *testing.T) {
	ids := []string{"a", "b"}
	base := ringSignature(ids, nil)
	weighted := ringSignature(ids, map[string]int{"a": 1, "b": 4})
	if base == weighted {
		t.Error("signature should differ when weights change")
	}
	// Stable for the same inputs.
	if ringSignature(ids, map[string]int{"b": 4}) != weighted {
		t.Error("signature should be stable for equal weights")
	}
}

func TestRouter_WeightsRingByCapacity(t *testing.T) {
	m, _ := membership.New("big", "big:7100", "big:7000")
	m.SetSelfCapacity(4000, 0, false)
	m.Apply(membership.Event{ID: "small", Address: "small:7100", DataAddress: "small:7000", State: membership.StateAlive, Incarnation: 1, CapacityBytes: 1000})
	r := NewRouter(m)

	counts := map[string]int{}
	for i := 0; i < 12000; i++ {
		counts[r.Replicas("k-"+strconv.Itoa(i), 1)[0]]++
	}
	ratio := float64(counts["big"]) / float64(counts["small"])
	if ratio < 3.0 || ratio > 5.0 {
		t.Errorf("big/small ratio = %.2f (big=%d small=%d), want ~4", ratio, counts["big"], counts["small"])
	}
}
