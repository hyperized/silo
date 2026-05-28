package replication_test

import (
	"sort"
	"testing"

	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/replication"
)

func aliveEvent(id, dataAddr string, incarnation uint64) membership.Event {
	return membership.Event{
		ID:          id,
		Address:     id + ":7100",
		DataAddress: dataAddr,
		State:       membership.StateAlive,
		Incarnation: incarnation,
	}
}

func TestRouter_ResolvesSelfAndPeers(t *testing.T) {
	m, err := membership.New("self", "self:7100", "self:7000")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	m.Apply(aliveEvent("beta", "beta:7000", 1))

	r := replication.NewRouter(m)

	if r.SelfID() != "self" {
		t.Errorf("SelfID: got %q, want self", r.SelfID())
	}
	if addr, ok := r.DataAddr("beta"); !ok || addr != "beta:7000" {
		t.Errorf("DataAddr(beta): got %q, %v; want beta:7000, true", addr, ok)
	}
	if _, ok := r.DataAddr("ghost"); ok {
		t.Error("DataAddr for an unknown node should report ok=false")
	}

	// Self and the one peer are both placement candidates.
	got := r.Replicas("chunk-1", 5)
	sort.Strings(got)
	if len(got) != 2 || got[0] != "beta" || got[1] != "self" {
		t.Errorf("Replicas: got %v, want [beta self]", got)
	}
}

func TestRouter_SkipsNodeWithoutDataAddress(t *testing.T) {
	m, _ := membership.New("self", "self:7100", "self:7000")
	// Alive, but no data address advertised yet (e.g. just-learned peer).
	m.Apply(aliveEvent("beta", "", 1))

	r := replication.NewRouter(m)
	if _, ok := r.DataAddr("beta"); ok {
		t.Error("DataAddr should report ok=false until the peer advertises an address")
	}
}

func TestRouter_RebuildsWhenMembershipChanges(t *testing.T) {
	m, _ := membership.New("self", "self:7100", "self:7000")
	r := replication.NewRouter(m)

	// First call materialises the ring (self only).
	if got := r.Replicas("k", 10); len(got) != 1 {
		t.Fatalf("initial ring should hold only self, got %v", got)
	}
	// Second call with no change must reuse the cached ring.
	if got := r.Replicas("k", 10); len(got) != 1 {
		t.Fatalf("cached ring should still hold only self, got %v", got)
	}

	// Membership grows: the ring must rebuild and surface the new nodes.
	m.Apply(aliveEvent("beta", "beta:7000", 1))
	m.Apply(aliveEvent("gamma", "gamma:7000", 1))
	if got := r.Replicas("k", 10); len(got) != 3 {
		t.Fatalf("rebuilt ring should hold 3 nodes, got %v", got)
	}

	// A peer dying shrinks the live set and the ring with it.
	m.MarkDead("beta")
	if got := r.Replicas("k", 10); len(got) != 2 {
		t.Fatalf("ring should drop the dead node, got %v", got)
	}
}
