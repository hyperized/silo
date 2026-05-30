package replication

import (
	"strconv"
	"testing"

	"github.com/hyperized/silo/internal/membership"
)

func pset(ids ...string) map[string]struct{} {
	s := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		s[id] = struct{}{}
	}
	return s
}

func contains(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func TestSteer_SkipsSinglePressuredPrimary(t *testing.T) {
	// natural top-3 = A,B,C (A pressured); D,E spill over.
	ordered := []string{"A", "B", "C", "D", "E"}
	got := steer(ordered, 3, pset("A"))
	if len(got) != 3 {
		t.Fatalf("want 3 replicas, got %v", got)
	}
	if contains(got, "A") {
		t.Errorf("pressured primary A should be skipped: %v", got)
	}
	// B and C (the two healthy naturals = quorum) are retained; D fills in.
	if !contains(got, "B") || !contains(got, "C") || !contains(got, "D") {
		t.Errorf("want {B,C,D}, got %v", got)
	}
}

func TestSteer_KeepsQuorumWhenMajorityPressured(t *testing.T) {
	// A,B pressured of natural {A,B,C}. quorum=2, only C is healthy, so one
	// pressured natural must be retained for the quorum overlap.
	ordered := []string{"A", "B", "C", "D", "E"}
	got := steer(ordered, 3, pset("A", "B"))
	if len(got) != 3 {
		t.Fatalf("want 3, got %v", got)
	}
	naturalsKept := 0
	for _, id := range []string{"A", "B", "C"} {
		if contains(got, id) {
			naturalsKept++
		}
	}
	if naturalsKept < 2 {
		t.Errorf("want >= quorum(2) naturals retained, got %d in %v", naturalsKept, got)
	}
	if !contains(got, "C") {
		t.Errorf("the one healthy natural C must be kept: %v", got)
	}
}

func TestSteer_AllPressuredFallsBack(t *testing.T) {
	// Whole cluster pressured: never under-replicate — return n nodes anyway.
	ordered := []string{"A", "B", "C", "D"}
	got := steer(ordered, 3, pset("A", "B", "C", "D"))
	if len(got) != 3 {
		t.Fatalf("a fully-pressured cluster must still place n replicas, got %v", got)
	}
}

func TestSteer_NoPressureIsNaturalOrder(t *testing.T) {
	ordered := []string{"A", "B", "C", "D"}
	got := steer(ordered, 3, pset())
	want := []string{"A", "B", "C"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("no pressure should preserve ring order: got %v want %v", got, want)
		}
	}
}

func TestSteer_ClampsAndEmpty(t *testing.T) {
	if got := steer([]string{"A", "B"}, 5, pset()); len(got) != 2 {
		t.Errorf("n > ring size should clamp to ring size, got %v", got)
	}
	if got := steer([]string{"A"}, 0, pset()); got != nil {
		t.Errorf("n=0 should return nil, got %v", got)
	}
	if got := steer(nil, 3, pset()); got != nil {
		t.Errorf("empty ring should return nil, got %v", got)
	}
}

// TestSteer_QuorumOverlapInvariant is the safety proof, checked exhaustively:
// for any key order and any pressure subset, the steered set shares a quorum
// with the natural set. That guarantees any two steered sets overlap in at
// least one node, so a read always finds a chunk its writer placed.
func TestSteer_QuorumOverlapInvariant(t *testing.T) {
	ordered := []string{"A", "B", "C", "D", "E", "F", "G"}
	for n := 1; n <= len(ordered); n++ {
		quorum := n/2 + 1
		// Enumerate every subset of the 7 nodes as the pressured set.
		for mask := 0; mask < (1 << len(ordered)); mask++ {
			pressured := map[string]struct{}{}
			for i, id := range ordered {
				if mask&(1<<i) != 0 {
					pressured[id] = struct{}{}
				}
			}
			got := steer(ordered, n, pressured)
			if len(got) != n {
				t.Fatalf("n=%d mask=%b: got %d replicas, want %d (%v)", n, mask, len(got), n, got)
			}
			if dups(got) {
				t.Fatalf("n=%d mask=%b: duplicate node in %v", n, mask, got)
			}
			natural := pset(ordered[:n]...)
			overlap := 0
			for _, id := range got {
				if _, ok := natural[id]; ok {
					overlap++
				}
			}
			if overlap < min(quorum, n) {
				t.Fatalf("n=%d mask=%b: steered %v overlaps natural by %d, want >= quorum %d", n, mask, got, overlap, quorum)
			}
		}
	}
}

func dups(ids []string) bool {
	seen := map[string]struct{}{}
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			return true
		}
		seen[id] = struct{}{}
	}
	return false
}

// TestRouter_SteersAwayFromPressuredNode is the integration check: with steering
// on, a pressured node's share of new placements collapses; with it off, the
// ring is unchanged.
func TestRouter_SteersAwayFromPressuredNode(t *testing.T) {
	build := func(steering bool) *Router {
		m, _ := membership.New("a", "a:7100", "a:7000")
		for _, id := range []string{"b", "c", "d"} {
			m.Apply(membership.Event{ID: id, Address: id + ":7100", DataAddress: id + ":7000", State: membership.StateAlive, Incarnation: 1, CapacityBytes: 1000})
		}
		// Mark node "b" as under disk pressure.
		m.Apply(membership.Event{ID: "b", State: membership.StateAlive, Incarnation: 2, CapacityBytes: 1000, UsedBytes: 950, Pressured: true})
		return NewRouter(m, WithPressureSteering(steering))
	}

	countPrimary := func(r *Router) int {
		onB := 0
		for i := 0; i < 6000; i++ {
			if r.Replicas("k-"+strconv.Itoa(i), 3)[0] == "b" {
				onB++
			}
		}
		return onB
	}

	steered := countPrimary(build(true))
	plain := countPrimary(build(false))
	if steered != 0 {
		t.Errorf("with steering, pressured node b should never be primary, got %d", steered)
	}
	if plain == 0 {
		t.Error("without steering, b should still take its ring share of primaries")
	}
}

func TestRouter_SteeringNoOpWhenNothingPressured(t *testing.T) {
	m, _ := membership.New("a", "a:7100", "a:7000")
	for _, id := range []string{"b", "c"} {
		m.Apply(membership.Event{ID: id, Address: id + ":7100", DataAddress: id + ":7000", State: membership.StateAlive, Incarnation: 1, CapacityBytes: 1000})
	}
	steered := NewRouter(m, WithPressureSteering(true))
	plain := NewRouter(m, WithPressureSteering(false))
	for i := 0; i < 500; i++ {
		k := "k-" + strconv.Itoa(i)
		if got, want := steered.Replicas(k, 2), plain.Replicas(k, 2); !equalIDs(got, want) {
			t.Fatalf("with nothing pressured, steering must match the plain ring: %v vs %v", got, want)
		}
	}
}

func equalIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
