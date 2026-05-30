package membership_test

import (
	"sort"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/membership"
)

// TestMembership_PublicSurface exercises the package's contract from a
// caller's point of view: build a table, apply events from peers,
// observe convergence. White-box tests for the merge rule table live
// in membership_internal_test.go.
func TestMembership_PublicSurface(t *testing.T) {
	m, err := membership.New("alpha", "alpha:7100", "alpha:7000")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	self := m.Self()
	if self.ID != "alpha" || self.State != membership.StateAlive {
		t.Errorf("Self: got %+v", self)
	}
	if self.Address != "alpha:7100" {
		t.Errorf("Self address: got %q, want alpha:7100", self.Address)
	}

	// Insert two peers we learned about from a SyncResp.
	m.ApplyMany([]membership.Event{
		{ID: "beta", Address: "beta:7100", State: membership.StateAlive, Incarnation: 1},
		{ID: "gamma", Address: "gamma:7100", State: membership.StateAlive, Incarnation: 1},
	})

	got := m.Members()
	sort.Slice(got, func(i, j int) bool { return got[i].ID < got[j].ID })
	wantIDs := []string{"alpha", "beta", "gamma"}
	if len(got) != 3 {
		t.Fatalf("Members: got %d, want 3", len(got))
	}
	for i, n := range got {
		if n.ID != wantIDs[i] {
			t.Errorf("Members[%d].ID: got %q, want %q", i, n.ID, wantIDs[i])
		}
	}

	// MarkSuspect publishes an event the caller would broadcast; the
	// returned event must carry the peer's current incarnation so
	// concurrent observers can converge.
	ev, ok := m.MarkSuspect("beta")
	if !ok {
		t.Fatal("MarkSuspect should report a change for an Alive peer")
	}
	if ev.ID != "beta" || ev.State != membership.StateSuspect || ev.Incarnation != 1 {
		t.Errorf("MarkSuspect event: got %+v", ev)
	}

	// A peer that observed Beta as Dead at the same incarnation broadcasts
	// it back. Our table should merge to Dead via the equal-incarnation
	// ordering rule.
	if _, ok := m.Apply(membership.Event{ID: "beta", State: membership.StateDead, Incarnation: 1}); !ok {
		t.Fatal("Apply Dead should have changed the entry")
	}
	if n, _ := m.Lookup("beta"); n.State != membership.StateDead {
		t.Errorf("Beta: got %s, want dead", n.State)
	}

	// AlivePeers excludes self and beta (Dead).
	alive := m.AlivePeers()
	if len(alive) != 1 || alive[0].ID != "gamma" {
		t.Errorf("AlivePeers: got %+v, want [gamma]", alive)
	}
}

func TestMembership_DataAddressPropagates(t *testing.T) {
	m, err := membership.New("alpha", "alpha:7100", "alpha:7000")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := m.Self().DataAddress; got != "alpha:7000" {
		t.Errorf("Self data address: got %q, want alpha:7000", got)
	}

	// A peer's data address arrives on its first event and is retained.
	m.Apply(membership.Event{ID: "beta", Address: "beta:7100", DataAddress: "beta:7000", State: membership.StateAlive, Incarnation: 1})
	if n, _ := m.Lookup("beta"); n.DataAddress != "beta:7000" {
		t.Fatalf("beta data address after insert: got %q, want beta:7000", n.DataAddress)
	}

	// A newer event that omits the data address must not blank the one we
	// already learned — preferNonEmpty keeps it (e.g. a Suspect event
	// minted by a peer that never recorded beta's data plane).
	m.Apply(membership.Event{ID: "beta", State: membership.StateSuspect, Incarnation: 2})
	if n, _ := m.Lookup("beta"); n.DataAddress != "beta:7000" {
		t.Errorf("beta data address must survive a data-address-less merge: got %q", n.DataAddress)
	}

	// A newer event carrying a changed data address updates it.
	m.Apply(membership.Event{ID: "beta", DataAddress: "beta-moved:7000", State: membership.StateAlive, Incarnation: 3})
	if n, _ := m.Lookup("beta"); n.DataAddress != "beta-moved:7000" {
		t.Errorf("beta data address should update on a newer event: got %q", n.DataAddress)
	}

	// Mark* events must carry the data address so the broadcast that
	// announces a failure still teaches receivers where the node's data
	// plane was — the re-replication path needs it even for a dead node.
	ev, ok := m.MarkDead("beta")
	if !ok {
		t.Fatal("MarkDead should report a change")
	}
	if ev.DataAddress != "beta-moved:7000" {
		t.Errorf("MarkDead event data address: got %q, want beta-moved:7000", ev.DataAddress)
	}
}

func TestMembership_PruneRemovesDeadAfterRetention(t *testing.T) {
	// Drive the clock forward through Now so retention takes effect
	// without sleeping.
	prev := membership.Now
	t.Cleanup(func() { membership.Now = prev })
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	current := t0
	membership.Now = func() time.Time { return current }

	m, err := membership.New("self", "self:7100", "self:7000")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	m.Apply(membership.Event{ID: "dead-peer", State: membership.StateAlive, Incarnation: 1})
	if _, ok := m.MarkDead("dead-peer"); !ok {
		t.Fatal("MarkDead should succeed")
	}

	// 30 seconds is shorter than the 60-second retention default; pruning
	// should keep the entry. Two minutes later, it must be gone.
	current = t0.Add(30 * time.Second)
	if pruned := m.Prune(time.Minute); len(pruned) != 0 {
		t.Errorf("Prune retained entries too eagerly: %v", pruned)
	}
	current = t0.Add(2 * time.Minute)
	pruned := m.Prune(time.Minute)
	if len(pruned) != 1 || pruned[0] != "dead-peer" {
		t.Errorf("Prune: got %v, want [dead-peer]", pruned)
	}
	if _, ok := m.Lookup("dead-peer"); ok {
		t.Error("dead-peer should have been pruned")
	}
}

func TestMembership_SnapshotIsADeepCopy(t *testing.T) {
	m, _ := membership.New("self", "", "")
	m.Apply(membership.Event{ID: "p", Address: "p:1", State: membership.StateAlive, Incarnation: 1})
	snap := m.Snapshot()
	for i := range snap {
		snap[i].Address = "tampered"
	}
	got, _ := m.Lookup("p")
	if got.Address == "tampered" {
		t.Error("Snapshot returned a reference to the canonical entry")
	}
}

func TestSetSelfCapacityAndPropagation(t *testing.T) {
	m, err := membership.New("a", "a:7100", "a:7000")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// First advertisement changes; an identical one does not.
	if !m.SetSelfCapacity(1000, 250, false) {
		t.Fatal("first SetSelfCapacity should report a change")
	}
	if m.SetSelfCapacity(1000, 250, false) {
		t.Error("an unchanged SetSelfCapacity should report no change")
	}
	if self := m.Self(); self.CapacityBytes != 1000 || self.UsedBytes != 250 || self.Pressured {
		t.Errorf("self capacity = %+v, want 1000/250 unpressured", self)
	}
	// Flipping only the DiskPressure condition is itself a change.
	if !m.SetSelfCapacity(1000, 250, true) {
		t.Error("flipping the pressure flag should report a change")
	}
	if self := m.Self(); !self.Pressured {
		t.Errorf("self should now be pressured: %+v", self)
	}

	// A peer's advertisement (higher incarnation, carrying capacity + pressure) is adopted.
	m.Apply(membership.Event{ID: "b", Address: "b:7100", State: membership.StateAlive, Incarnation: 5, CapacityBytes: 2000, UsedBytes: 1900, Pressured: true})
	b, _ := m.Lookup("b")
	if b.CapacityBytes != 2000 || b.UsedBytes != 1900 || !b.Pressured {
		t.Errorf("peer b = %+v, want 2000/1900 pressured", b)
	}

	// A later plain state-change event (no capacity) must not wipe b's figures or condition.
	m.Apply(membership.Event{ID: "b", State: membership.StateSuspect, Incarnation: 5})
	b, _ = m.Lookup("b")
	if b.CapacityBytes != 2000 || b.UsedBytes != 1900 || !b.Pressured {
		t.Errorf("capacity-less event wiped b's figures: %+v", b)
	}
}
