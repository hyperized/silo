package membership

import (
	"sync"
	"testing"
	"time"
)

func TestState_String(t *testing.T) {
	cases := []struct {
		in   State
		want string
	}{
		{StateAlive, "alive"},
		{StateSuspect, "suspect"},
		{StateDead, "dead"},
		{StateLeft, "left"},
		{State(99), "state(99)"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.in.String(); got != tc.want {
				t.Errorf("State(%d).String(): got %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNew_RejectsEmptySelfID(t *testing.T) {
	_, err := New("", "127.0.0.1:7100")
	if err == nil {
		t.Fatal("expected error for empty selfID")
	}
}

func TestApply_MergeRules(t *testing.T) {
	// Pin the clock so LastChange comparisons are deterministic.
	prev := Now
	t.Cleanup(func() { Now = prev })
	base := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	step := time.Second
	tick := 0
	Now = func() time.Time {
		tick++
		return base.Add(time.Duration(tick) * step)
	}

	// applyTo is a fresh table seeded with one external entry, so each
	// case is independent.
	applyTo := func(t *testing.T, initial Node, ev Event) (Node, bool) {
		t.Helper()
		m, err := New("self", "addr:1")
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if initial.ID != "" {
			m.members[initial.ID] = initial
		}
		return m.Apply(ev)
	}

	cases := []struct {
		name       string
		initial    Node
		ev         Event
		wantChange bool
		wantState  State
		wantInc    uint64
	}{
		{
			name:       "insert unknown peer",
			ev:         Event{ID: "p1", Address: "h:1", State: StateAlive, Incarnation: 1},
			wantChange: true,
			wantState:  StateAlive,
			wantInc:    1,
		},
		{
			name:       "higher incarnation supersedes",
			initial:    Node{ID: "p1", Address: "h:1", State: StateSuspect, Incarnation: 5},
			ev:         Event{ID: "p1", Address: "h:1", State: StateAlive, Incarnation: 6},
			wantChange: true,
			wantState:  StateAlive,
			wantInc:    6,
		},
		{
			name:       "lower incarnation ignored",
			initial:    Node{ID: "p1", Address: "h:1", State: StateAlive, Incarnation: 5},
			ev:         Event{ID: "p1", Address: "h:1", State: StateDead, Incarnation: 4},
			wantChange: false,
			wantState:  StateAlive,
			wantInc:    5,
		},
		{
			name:       "equal incarnation, suspect supersedes alive",
			initial:    Node{ID: "p1", Address: "h:1", State: StateAlive, Incarnation: 7},
			ev:         Event{ID: "p1", Address: "h:1", State: StateSuspect, Incarnation: 7},
			wantChange: true,
			wantState:  StateSuspect,
			wantInc:    7,
		},
		{
			name:       "equal incarnation, dead supersedes suspect",
			initial:    Node{ID: "p1", Address: "h:1", State: StateSuspect, Incarnation: 7},
			ev:         Event{ID: "p1", Address: "h:1", State: StateDead, Incarnation: 7},
			wantChange: true,
			wantState:  StateDead,
			wantInc:    7,
		},
		{
			name:       "equal incarnation, left supersedes dead",
			initial:    Node{ID: "p1", State: StateDead, Incarnation: 1},
			ev:         Event{ID: "p1", State: StateLeft, Incarnation: 1},
			wantChange: true,
			wantState:  StateLeft,
			wantInc:    1,
		},
		{
			name:       "equal incarnation, no state regression",
			initial:    Node{ID: "p1", State: StateDead, Incarnation: 7},
			ev:         Event{ID: "p1", State: StateAlive, Incarnation: 7},
			wantChange: false,
			wantState:  StateDead,
			wantInc:    7,
		},
		{
			name:       "empty id ignored",
			ev:         Event{ID: "", State: StateAlive},
			wantChange: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, ok := applyTo(t, tc.initial, tc.ev)
			if ok != tc.wantChange {
				t.Errorf("change: got %v, want %v", ok, tc.wantChange)
			}
			if tc.wantChange {
				if n.State != tc.wantState {
					t.Errorf("state: got %s, want %s", n.State, tc.wantState)
				}
				if n.Incarnation != tc.wantInc {
					t.Errorf("incarnation: got %d, want %d", n.Incarnation, tc.wantInc)
				}
			}
		})
	}
}

func TestApply_PreservesKnownAddressWhenEventAddressEmpty(t *testing.T) {
	m, err := New("self", "self:1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	m.members["p1"] = Node{ID: "p1", Address: "known:7", State: StateAlive, Incarnation: 1}
	if _, ok := m.Apply(Event{ID: "p1", State: StateAlive, Incarnation: 2}); !ok {
		t.Fatal("Apply should have changed the entry")
	}
	if got, _ := m.Lookup("p1"); got.Address != "known:7" {
		t.Errorf("address erased on higher-incarnation merge: got %q", got.Address)
	}
}

func TestApply_SelfRefutation(t *testing.T) {
	m, err := New("self", "self:1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Peer thinks we're suspect at incarnation 3.
	n, ok := m.Apply(Event{ID: "self", State: StateSuspect, Incarnation: 3})
	if !ok {
		t.Fatal("self-refutation should report a change")
	}
	if n.State != StateAlive {
		t.Errorf("after refutation: got state %s, want alive", n.State)
	}
	if n.Incarnation != 4 {
		t.Errorf("after refutation: got incarnation %d, want 4", n.Incarnation)
	}
	// Repeated Alive claims about us at any incarnation should be no-ops.
	_, ok = m.Apply(Event{ID: "self", State: StateAlive, Incarnation: 99})
	if ok {
		t.Error("Alive self-claim should not be reported as a change")
	}
}

func TestApply_SelfRefutationWithLowerIncarnationStillRefutes(t *testing.T) {
	// If we're already at incarnation 10 and a stale Suspect event for
	// us arrives with incarnation 3, we still refute (state becomes
	// Alive) but we keep our existing higher incarnation.
	m, err := New("self", "self:1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cur := m.members["self"]
	cur.Incarnation = 10
	m.members["self"] = cur

	n, ok := m.Apply(Event{ID: "self", State: StateDead, Incarnation: 3})
	if !ok {
		t.Fatal("self-refutation should record a change (LastChange advanced)")
	}
	if n.Incarnation != 10 {
		t.Errorf("incarnation should stay at 10 when claim is stale, got %d", n.Incarnation)
	}
	if n.State != StateAlive {
		t.Errorf("state: got %s, want alive", n.State)
	}
}

func TestApplyMany_EmptyAndPartial(t *testing.T) {
	m, err := New("self", "self:1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := m.ApplyMany(nil); got != nil {
		t.Errorf("ApplyMany(nil): got %v, want nil", got)
	}
	changed := m.ApplyMany([]Event{
		{ID: "", State: StateAlive},                     // skipped
		{ID: "p1", State: StateAlive, Incarnation: 1},   // applied
		{ID: "p1", State: StateAlive, Incarnation: 1},   // no-op (same)
		{ID: "p2", State: StateSuspect, Incarnation: 1}, // applied
	})
	if len(changed) != 2 {
		t.Errorf("changed: got %d, want 2", len(changed))
	}
}

func TestMarkSuspect_Transitions(t *testing.T) {
	m, _ := New("self", "self:1")
	m.members["p1"] = Node{ID: "p1", State: StateAlive, Incarnation: 1}
	ev, ok := m.MarkSuspect("p1")
	if !ok || ev.State != StateSuspect {
		t.Errorf("MarkSuspect: got %v, %v", ev, ok)
	}
	if _, ok := m.MarkSuspect("p1"); ok {
		t.Error("re-suspecting an already-suspect node should be a no-op")
	}
	if _, ok := m.MarkSuspect(""); ok {
		t.Error("empty id should be rejected")
	}
	if _, ok := m.MarkSuspect("self"); ok {
		t.Error("self should not be suspectable from MarkSuspect")
	}
	if _, ok := m.MarkSuspect("unknown"); ok {
		t.Error("unknown id should not be suspectable")
	}
}

func TestMarkDead_Transitions(t *testing.T) {
	m, _ := New("self", "self:1")
	m.members["p1"] = Node{ID: "p1", State: StateSuspect, Incarnation: 2}
	ev, ok := m.MarkDead("p1")
	if !ok || ev.State != StateDead {
		t.Errorf("MarkDead: got %v, %v", ev, ok)
	}
	if _, ok := m.MarkDead("p1"); ok {
		t.Error("re-killing a dead node should be a no-op")
	}
	if _, ok := m.MarkDead(""); ok {
		t.Error("empty id should be rejected")
	}
	if _, ok := m.MarkDead("self"); ok {
		t.Error("self should not be deadable")
	}
	if _, ok := m.MarkDead("nope"); ok {
		t.Error("unknown id should not be deadable")
	}
}

func TestMarkLeft_Transitions(t *testing.T) {
	m, _ := New("self", "self:1")
	m.members["p1"] = Node{ID: "p1", State: StateAlive, Incarnation: 5}
	ev, ok := m.MarkLeft("p1")
	if !ok || ev.State != StateLeft {
		t.Errorf("MarkLeft: got %v, %v", ev, ok)
	}
	if _, ok := m.MarkLeft("p1"); ok {
		t.Error("re-leaving a left node should be a no-op")
	}
	if _, ok := m.MarkLeft(""); ok {
		t.Error("empty id should be rejected")
	}
	if _, ok := m.MarkLeft("nope"); ok {
		t.Error("unknown id should not be leavable")
	}
}

func TestPrune_DeadAndLeftAfterRetention(t *testing.T) {
	prev := Now
	t.Cleanup(func() { Now = prev })
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	Now = func() time.Time { return base }

	m, _ := New("self", "self:1")
	m.members["dead-old"] = Node{ID: "dead-old", State: StateDead, LastChange: base.Add(-time.Hour)}
	m.members["dead-new"] = Node{ID: "dead-new", State: StateDead, LastChange: base.Add(-time.Second)}
	m.members["left-old"] = Node{ID: "left-old", State: StateLeft, LastChange: base.Add(-time.Hour)}
	m.members["alive-old"] = Node{ID: "alive-old", State: StateAlive, LastChange: base.Add(-time.Hour)}

	if got := m.Prune(0); got != nil {
		t.Errorf("Prune(0): got %v, want nil", got)
	}
	pruned := m.Prune(time.Minute)
	want := map[string]bool{"dead-old": true, "left-old": true}
	if len(pruned) != len(want) {
		t.Errorf("Prune: got %d, want %d", len(pruned), len(want))
	}
	for _, id := range pruned {
		if !want[id] {
			t.Errorf("Prune returned unexpected id %q", id)
		}
	}
	if _, ok := m.Lookup("dead-old"); ok {
		t.Error("dead-old should have been pruned")
	}
	if _, ok := m.Lookup("dead-new"); !ok {
		t.Error("dead-new should still be present")
	}
	if _, ok := m.Lookup("self"); !ok {
		t.Error("self should never be pruned")
	}
}

func TestAlivePeers_ExcludesSelfAndNonAlive(t *testing.T) {
	m, _ := New("self", "self:1")
	m.members["p1"] = Node{ID: "p1", State: StateAlive, Address: "p1:1"}
	m.members["p2"] = Node{ID: "p2", State: StateSuspect}
	m.members["p3"] = Node{ID: "p3", State: StateDead}
	got := m.AlivePeers()
	if len(got) != 1 || got[0].ID != "p1" {
		t.Errorf("AlivePeers: got %+v, want one entry for p1", got)
	}
}

func TestSetSelfAddress_BumpsIncarnation(t *testing.T) {
	m, _ := New("self", "")
	before := m.Self()
	m.SetSelfAddress("self:7100")
	after := m.Self()
	if after.Address != "self:7100" {
		t.Errorf("Address: got %q, want self:7100", after.Address)
	}
	if after.Incarnation <= before.Incarnation {
		t.Errorf("incarnation should bump: %d -> %d", before.Incarnation, after.Incarnation)
	}
}

func TestSelfID_Returns(t *testing.T) {
	m, _ := New("self-x", "")
	if m.SelfID() != "self-x" {
		t.Errorf("SelfID: got %q, want self-x", m.SelfID())
	}
}

func TestMembership_ConcurrentApplyIsRaceFree(t *testing.T) {
	// Run lots of concurrent Apply/Lookup pairs under -race; failure is
	// only visible when go test is run with -race. The assertion side
	// just confirms we converged on something sensible.
	m, _ := New("self", "self:1")
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ev := Event{ID: "shared", State: StateAlive, Incarnation: uint64(id)}
			m.Apply(ev)
			_, _ = m.Lookup("shared")
			m.Members()
		}(i)
	}
	wg.Wait()
	n, ok := m.Lookup("shared")
	if !ok {
		t.Fatal("shared entry should exist")
	}
	if n.Incarnation == 0 {
		t.Errorf("expected highest-incarnation merge to win, got %d", n.Incarnation)
	}
}
