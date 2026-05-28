package placement

import "testing"

func TestLessPoint(t *testing.T) {
	cases := []struct {
		name string
		a, b point
		want bool
	}{
		{"lower hash sorts first", point{hash: 1, node: "z"}, point{hash: 2, node: "a"}, true},
		{"higher hash does not sort first", point{hash: 2, node: "a"}, point{hash: 1, node: "z"}, false},
		// Equal hashes are the collision case unreachable through real fnv
		// output; the node-id tiebreak keeps the ring order deterministic.
		{"equal hash breaks ties by node id", point{hash: 5, node: "a"}, point{hash: 5, node: "b"}, true},
		{"equal hash reverse tie", point{hash: 5, node: "b"}, point{hash: 5, node: "a"}, false},
		{"identical points are not less", point{hash: 5, node: "a"}, point{hash: 5, node: "a"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lessPoint(tc.a, tc.b); got != tc.want {
				t.Errorf("lessPoint(%+v, %+v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestHashFuncsAreDeterministic(t *testing.T) {
	// Build one input at runtime so the comparison is against an equal but
	// not syntactically identical expression — a real determinism check
	// rather than a tautology.
	key := "chunk-" + "42"
	if hashKey(key) != hashKey("chunk-42") {
		t.Error("hashKey is not deterministic for equal keys")
	}
	idx := 3 + 4
	if hashVNode("node-"+"a", idx) != hashVNode("node-a", 7) {
		t.Error("hashVNode is not deterministic for equal (node, index)")
	}
	if hashKey("a") == hashKey("b") {
		t.Error("distinct keys collided; fnv should separate these")
	}
	if hashVNode("node-a", 0) == hashVNode("node-a", 1) {
		t.Error("distinct virtual-node indices collided for the same node")
	}
}

func TestNewPopulatesPointsPerNode(t *testing.T) {
	r := New([]string{"a", "b"}, 4)
	if got := len(r.points); got != 2*4 {
		t.Errorf("points = %d, want %d", got, 2*4)
	}
	for i := 1; i < len(r.points); i++ {
		if lessPoint(r.points[i], r.points[i-1]) {
			t.Fatalf("points not sorted at index %d: %+v before %+v", i, r.points[i-1], r.points[i])
		}
	}
}

func TestNewDefaultsVNodes(t *testing.T) {
	for _, vnodes := range []int{0, -5} {
		r := New([]string{"a"}, vnodes)
		if got := len(r.points); got != DefaultVNodes {
			t.Errorf("New with vnodes=%d gave %d points, want DefaultVNodes=%d", vnodes, got, DefaultVNodes)
		}
	}
}
