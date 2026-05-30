package diskpressure_test

import (
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/diskpressure"
)

func TestDefaultThresholdsValid(t *testing.T) {
	if err := diskpressure.DefaultThresholds().Validate(); err != nil {
		t.Fatalf("the built-in defaults must be valid: %v", err)
	}
}

func TestThresholds_Validate(t *testing.T) {
	cases := []struct {
		name string
		t    diskpressure.Thresholds
		want string // "" = expect valid
	}{
		{"defaults", diskpressure.DefaultThresholds(), ""},
		{"high out of range", diskpressure.Thresholds{High: 1.5, Clear: 0.8, Hard: 0.95}, "high watermark must be in"},
		{"clear zero", diskpressure.Thresholds{High: 0.85, Clear: 0, Hard: 0.95}, "clear watermark must be in"},
		{"hard above one", diskpressure.Thresholds{High: 0.85, Clear: 0.8, Hard: 1.1}, "hard watermark must be in"},
		{"clear >= high", diskpressure.Thresholds{High: 0.80, Clear: 0.80, Hard: 0.95}, "must be below high"},
		{"high >= hard", diskpressure.Thresholds{High: 0.95, Clear: 0.80, Hard: 0.95}, "must be below hard"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.t.Validate()
			if tc.want == "" {
				if err != nil {
					t.Errorf("expected valid, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestThresholds_HardExceeded(t *testing.T) {
	th := diskpressure.DefaultThresholds() // hard 0.95
	if th.HardExceeded(0.94) {
		t.Error("0.94 is below the hard floor")
	}
	if !th.HardExceeded(0.95) {
		t.Error("0.95 is at the hard floor (inclusive)")
	}
	if !th.HardExceeded(0.99) {
		t.Error("0.99 is above the hard floor")
	}
}

func TestEvaluator_Hysteresis(t *testing.T) {
	e := diskpressure.NewEvaluator(diskpressure.Thresholds{High: 0.85, Clear: 0.80, Hard: 0.95})

	if e.Pressured() {
		t.Fatal("a fresh evaluator starts un-pressured")
	}
	// Below high: stays clear.
	if e.Update(0.84) {
		t.Error("0.84 < high should not enter pressure")
	}
	// At/above high: enters.
	if !e.Update(0.86) {
		t.Error("0.86 >= high should enter pressure")
	}
	// In the hysteresis band (between clear and high): stays pressured.
	if !e.Update(0.82) {
		t.Error("0.82 is in the band; pressure should persist")
	}
	if !e.Pressured() {
		t.Error("Pressured() should still report true in the band")
	}
	// At/below clear: leaves.
	if e.Update(0.80) {
		t.Error("0.80 <= clear should clear pressure")
	}
	// Back in the band from below: stays clear (no flap).
	if e.Update(0.82) {
		t.Error("0.82 from the clear state should not re-enter (hysteresis)")
	}
}
