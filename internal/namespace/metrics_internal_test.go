package namespace

import (
	"testing"
	"time"

	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/metrics"
)

func nsMetric(t *testing.T, n *Namespace, name string) (metrics.Metric, bool) {
	t.Helper()
	for _, m := range n.CollectMetrics() {
		if m.Name == name {
			return m, true
		}
	}
	return metrics.Metric{}, false
}

func TestNamespace_AntiEntropyMetrics(t *testing.T) {
	a := New(hlc.New("a"))
	b := New(hlc.New("b"))
	if _, err := b.Mkdir("/fromB"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	state, err := b.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if a.MetricPrefix() != "silo_namespace" {
		t.Errorf("prefix = %q", a.MetricPrefix())
	}

	// Before any merge: merges=0, no lag gauge.
	if m, _ := nsMetric(t, a, "antientropy_merges_total"); m.Value != 0 || m.Kind != metrics.Counter {
		t.Errorf("merges before any = %v (kind %v), want 0 counter", m.Value, m.Kind)
	}
	if _, ok := nsMetric(t, a, "antientropy_last_merge_age_seconds"); ok {
		t.Error("no merge yet, so the lag gauge should be absent")
	}

	// Merge a peer's state with a pinned clock.
	prev := nsTimeNow
	t.Cleanup(func() { nsTimeNow = prev })
	base := time.Unix(2_000_000, 0)
	nsTimeNow = func() time.Time { return base }
	if err := a.MergeBytes(state); err != nil {
		t.Fatalf("MergeBytes: %v", err)
	}
	if m, _ := nsMetric(t, a, "antientropy_merges_total"); m.Value != 1 {
		t.Errorf("merges after one = %v, want 1", m.Value)
	}

	nsTimeNow = func() time.Time { return base.Add(3 * time.Second) }
	m, ok := nsMetric(t, a, "antientropy_last_merge_age_seconds")
	if !ok || m.Value < 2.9 || m.Value > 3.1 {
		t.Errorf("last_merge_age = %v (found %v), want ~3", m.Value, ok)
	}
}
