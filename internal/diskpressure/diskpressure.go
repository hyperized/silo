// Package diskpressure implements silo's disk high-watermark policy: the
// analog of a kubelet node's DiskPressure condition and eviction thresholds.
//
// Two tiers, both expressed as a used fraction of the filesystem backing the
// data directory:
//
//   - Soft (High/Clear, with hysteresis): the node raises a gossiped
//     DiskPressure condition. It is an early-warning signal for operators and
//     alerting — it does NOT relocate chunks. silo locates every chunk by
//     hashing it onto the placement ring (there is no per-chunk location
//     manifest), so removing a near-full node from the ring would reassign the
//     ownership of every chunk physically still on it, causing a re-replication
//     storm and transient read misses. The soft tier therefore signals; it does
//     not steer placement.
//
//   - Hard: the node refuses new local writes (chunkstore returns ErrNoSpace)
//     before the filesystem hits ENOSPC. In the replication coordinator a
//     refused replica simply fails its ack, so the write still completes on the
//     other replicas (quorum) and the scrubber heals the chunk onto the full
//     node once it has room. This is the enforcement tier, and it is safe
//     precisely because it never changes the ring.
package diskpressure

import "fmt"

// Default watermarks, as fractions of the data filesystem used. The soft pair
// mirrors kubelet's ~15%-available default with hysteresis; the hard floor
// leaves ~5% headroom before the filesystem itself would fail writes.
const (
	DefaultHigh  = 0.85
	DefaultClear = 0.80
	DefaultHard  = 0.95
)

// Thresholds is the watermark policy. High/Clear bound the soft DiskPressure
// condition with hysteresis; Hard is the absolute write-refusal floor.
type Thresholds struct {
	High  float64
	Clear float64
	Hard  float64
}

// DefaultThresholds returns the built-in policy.
func DefaultThresholds() Thresholds {
	return Thresholds{High: DefaultHigh, Clear: DefaultClear, Hard: DefaultHard}
}

// Validate rejects a nonsensical policy so a misconfiguration fails at startup
// with an actionable message rather than silently never (or always) firing.
func (t Thresholds) Validate() error {
	for _, f := range []struct {
		name string
		v    float64
	}{{"high", t.High}, {"clear", t.Clear}, {"hard", t.Hard}} {
		if f.v <= 0 || f.v > 1 {
			return fmt.Errorf("diskpressure: %s watermark must be in (0, 1], got %v; set SILO_DISK_PRESSURE_* to fractions like 0.85", f.name, f.v)
		}
	}
	if t.Clear >= t.High {
		return fmt.Errorf("diskpressure: clear watermark (%v) must be below high (%v) so the condition has hysteresis and does not flap", t.Clear, t.High)
	}
	if t.High >= t.Hard {
		return fmt.Errorf("diskpressure: high watermark (%v) must be below hard (%v) so writes are refused only after the soft condition has already fired", t.High, t.Hard)
	}
	return nil
}

// HardExceeded reports whether usedFraction is at or above the hard floor. It is
// stateless: the hard limit is an absolute physical reserve, not a hysteretic
// condition.
func (t Thresholds) HardExceeded(usedFraction float64) bool {
	return usedFraction >= t.Hard
}

// Evaluator tracks the soft DiskPressure condition for one node with hysteresis:
// it enters pressure at or above High and leaves it only at or below Clear, so a
// node hovering near High does not flap the condition (and, in turn, does not
// churn alerts or any drain automation watching it).
type Evaluator struct {
	t         Thresholds
	pressured bool
}

// NewEvaluator starts an evaluator in the un-pressured state.
func NewEvaluator(t Thresholds) *Evaluator {
	return &Evaluator{t: t}
}

// Update feeds the latest used fraction and returns the (possibly unchanged)
// soft-pressure state after applying hysteresis.
func (e *Evaluator) Update(usedFraction float64) bool {
	if e.pressured {
		if usedFraction <= e.t.Clear {
			e.pressured = false
		}
	} else if usedFraction >= e.t.High {
		e.pressured = true
	}
	return e.pressured
}

// Pressured returns the current soft-pressure state without feeding a new
// sample.
func (e *Evaluator) Pressured() bool {
	return e.pressured
}
