package hlc_test

import (
	"testing"
	"time"

	"github.com/hyperized/silo/internal/hlc"
)

func TestTimestamp_Compare(t *testing.T) {
	cases := []struct {
		name string
		a, b hlc.Timestamp
		want int
	}{
		{"lower wall", hlc.Timestamp{Wall: 1}, hlc.Timestamp{Wall: 2}, -1},
		{"higher wall", hlc.Timestamp{Wall: 2}, hlc.Timestamp{Wall: 1}, 1},
		{"lower logical", hlc.Timestamp{Wall: 1, Logical: 1}, hlc.Timestamp{Wall: 1, Logical: 2}, -1},
		{"higher logical", hlc.Timestamp{Wall: 1, Logical: 2}, hlc.Timestamp{Wall: 1, Logical: 1}, 1},
		{"node tiebreak lower", hlc.Timestamp{Wall: 1, Logical: 1, Node: "a"}, hlc.Timestamp{Wall: 1, Logical: 1, Node: "b"}, -1},
		{"node tiebreak higher", hlc.Timestamp{Wall: 1, Logical: 1, Node: "b"}, hlc.Timestamp{Wall: 1, Logical: 1, Node: "a"}, 1},
		{"equal", hlc.Timestamp{Wall: 1, Logical: 1, Node: "a"}, hlc.Timestamp{Wall: 1, Logical: 1, Node: "a"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Compare(tc.b); got != tc.want {
				t.Errorf("Compare = %d, want %d", got, tc.want)
			}
			if got := tc.a.Before(tc.b); got != (tc.want < 0) {
				t.Errorf("Before = %v, want %v", got, tc.want < 0)
			}
			if got := tc.a.After(tc.b); got != (tc.want > 0) {
				t.Errorf("After = %v, want %v", got, tc.want > 0)
			}
		})
	}
}

func TestTimestamp_IsZeroAndString(t *testing.T) {
	if !(hlc.Timestamp{}).IsZero() {
		t.Error("zero value should be IsZero")
	}
	if (hlc.Timestamp{Wall: 1}).IsZero() {
		t.Error("non-zero wall is not IsZero")
	}
	if got := (hlc.Timestamp{Wall: 42, Logical: 3, Node: "node-a"}).String(); got != "42.3.node-a" {
		t.Errorf("String = %q, want 42.3.node-a", got)
	}
}

// settableClock is a controllable physical clock for deterministic HLC tests.
type settableClock struct{ nanos int64 }

func (s *settableClock) now() time.Time { return time.Unix(0, s.nanos) }

func TestClock_NowIsMonotonicWithFrozenPhysicalTime(t *testing.T) {
	phys := &settableClock{nanos: 1000}
	c := hlc.New("self", hlc.WithNow(phys.now))

	first := c.Now()
	if first.Wall != 1000 || first.Logical != 0 || first.Node != "self" {
		t.Fatalf("first Now = %+v, want {1000,0,self}", first)
	}
	prev := first
	for i := 0; i < 5; i++ {
		got := c.Now()
		if !got.After(prev) {
			t.Fatalf("Now not strictly increasing: %s then %s", prev, got)
		}
		if got.Logical != uint32(i+1) {
			t.Errorf("logical = %d, want %d", got.Logical, i+1)
		}
		prev = got
	}
}

func TestClock_NowResetsLogicalWhenPhysicalAdvances(t *testing.T) {
	phys := &settableClock{nanos: 1000}
	c := hlc.New("self", hlc.WithNow(phys.now))
	c.Now() // wall=1000, logical=0
	c.Now() // wall=1000, logical=1

	phys.nanos = 2000
	got := c.Now()
	if got.Wall != 2000 || got.Logical != 0 {
		t.Errorf("after physical advance got %+v, want {2000,0}", got)
	}
}

func TestClock_NowStaysMonotonicWhenPhysicalGoesBackward(t *testing.T) {
	phys := &settableClock{nanos: 5000}
	c := hlc.New("self", hlc.WithNow(phys.now))
	high := c.Now() // wall=5000

	phys.nanos = 1000 // clock jumps backward
	got := c.Now()
	if got.Wall != 5000 {
		t.Errorf("wall regressed to %d; HLC must not move backward", got.Wall)
	}
	if !got.After(high) {
		t.Errorf("%s should be after %s despite the backward jump", got, high)
	}
}

func TestClock_UpdateAdoptsRemoteAhead(t *testing.T) {
	phys := &settableClock{nanos: 1000}
	c := hlc.New("self", hlc.WithNow(phys.now))
	c.Now() // local wall=1000

	remote := hlc.Timestamp{Wall: 9000, Logical: 5, Node: "peer"}
	got := c.Update(remote)
	if got.Wall != 9000 || got.Logical != 6 {
		t.Errorf("Update adopting remote-ahead = %+v, want {9000,6}", got)
	}
	if !got.After(remote) {
		t.Errorf("%s must happen-after the received %s", got, remote)
	}
}

func TestClock_UpdateRemoteBehind(t *testing.T) {
	phys := &settableClock{nanos: 5000}
	c := hlc.New("self", hlc.WithNow(phys.now))
	c.Now() // wall=5000, logical=0

	got := c.Update(hlc.Timestamp{Wall: 1000, Logical: 9, Node: "peer"})
	if got.Wall != 5000 || got.Logical != 1 {
		t.Errorf("Update with remote-behind = %+v, want {5000,1}", got)
	}
}

func TestClock_UpdateEqualWallsTakesMaxLogical(t *testing.T) {
	phys := &settableClock{nanos: 5000}
	c := hlc.New("self", hlc.WithNow(phys.now))
	c.Now() // wall=5000, logical=0

	got := c.Update(hlc.Timestamp{Wall: 5000, Logical: 3, Node: "peer"})
	if got.Wall != 5000 || got.Logical != 4 {
		t.Errorf("Update with equal walls = %+v, want {5000,4} (max(0,3)+1)", got)
	}
}

func TestClock_UpdatePhysicalAheadOfBoth(t *testing.T) {
	phys := &settableClock{nanos: 1000}
	c := hlc.New("self", hlc.WithNow(phys.now))
	c.Now() // wall=1000

	phys.nanos = 9000 // physical now beyond local and remote
	got := c.Update(hlc.Timestamp{Wall: 2000, Logical: 7, Node: "peer"})
	if got.Wall != 9000 || got.Logical != 0 {
		t.Errorf("Update with physical ahead = %+v, want {9000,0}", got)
	}
}

func TestNew_DefaultsToWallClock(t *testing.T) {
	c := hlc.New("self") // no WithNow: uses time.Now
	got := c.Now()
	if got.IsZero() || got.Node != "self" {
		t.Errorf("default-clock Now = %+v, want non-zero with node self", got)
	}
	if got.Wall < time.Now().Add(-time.Minute).UnixNano() {
		t.Errorf("wall %d is implausibly far from now", got.Wall)
	}
}
