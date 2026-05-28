package hlc_test

import (
	"testing"
	"time"

	"github.com/hyperized/silo/internal/hlc"
)

// FuzzClockMonotonic asserts the HLC's defining invariant: across any
// interleaving of local events and received remote timestamps — and with
// the physical clock wandering forward and backward — every timestamp the
// clock produces is strictly greater than the one before it. That strict
// monotonic total order is what lets the namespace order concurrent
// mutations deterministically.
func FuzzClockMonotonic(f *testing.F) {
	f.Add([]byte{0, 5, 1, 9, 0, 0, 1, 3})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		phys := int64(1)
		clk := hlc.New("self", hlc.WithNow(func() time.Time { return time.Unix(0, phys) }))

		var prev hlc.Timestamp
		started := false
		for i := 0; i+1 < len(data); i += 2 {
			phys = int64(data[i+1]) // wander physical time, including backward jumps

			var ts hlc.Timestamp
			if data[i]%2 == 0 {
				ts = clk.Now()
			} else {
				ts = clk.Update(hlc.Timestamp{Wall: int64(data[i+1]), Logical: uint32(data[i]), Node: "peer"})
			}

			if started && !ts.After(prev) {
				t.Fatalf("HLC not strictly monotonic: %s produced after %s", ts, prev)
			}
			prev, started = ts, true
		}
	})
}
