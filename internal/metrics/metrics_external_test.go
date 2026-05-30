package metrics_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/hyperized/silo/internal/metrics"
)

func TestStatic_ReportsConstantMetrics(t *testing.T) {
	ms := []metrics.Metric{
		{Name: "build_info", Kind: metrics.Gauge, Value: 1, Labels: [][2]string{{"node", "n1"}}},
		{Name: "ready", Kind: metrics.Gauge, Value: 1},
	}
	s := metrics.Static("silo", ms...)

	if got := s.MetricPrefix(); got != "silo" {
		t.Errorf("MetricPrefix = %q, want silo", got)
	}
	if got := s.CollectMetrics(); !reflect.DeepEqual(got, ms) {
		t.Errorf("CollectMetrics = %+v, want %+v", got, ms)
	}
}

// Static is a Source — guard the interface assertion at compile time.
var _ metrics.Source = metrics.Static("silo")

func TestHist_ObserveAndCollect(t *testing.T) {
	h := metrics.NewHist(0.01, 0.1, 1.0)
	for _, v := range []float64{0.005, 0.05, 0.05, 0.5, 5.0} { // last is past all bounds
		h.Observe(v)
	}
	m := h.Collect("op_latency_seconds", "help", [][2]string{{"op", "put"}})
	if m.Kind != metrics.Histogram || m.Count != 5 {
		t.Fatalf("collect = kind %v count %d, want histogram 5", m.Kind, m.Count)
	}
	if m.Sum < 5.605-0.0001 || m.Sum > 5.605+0.0001 {
		t.Errorf("sum = %v, want 5.605", m.Sum)
	}
	// Cumulative buckets: <=0.01 -> 1, <=0.1 -> 3, <=1.0 -> 4, +Inf -> 5.
	want := []uint64{1, 3, 4, 5}
	if len(m.Buckets) != 4 {
		t.Fatalf("buckets = %d, want 4", len(m.Buckets))
	}
	for i, b := range m.Buckets {
		if b.Count != want[i] {
			t.Errorf("bucket[%d] count = %d, want %d", i, b.Count, want[i])
		}
	}
	if !math.IsInf(m.Buckets[3].LE, 1) {
		t.Errorf("last bucket LE = %v, want +Inf", m.Buckets[3].LE)
	}
}

func TestNewHist_NoBounds(t *testing.T) {
	h := metrics.NewHist()
	h.Observe(2)
	h.Observe(3)
	m := h.Collect("x", "h", nil)
	if m.Count != 2 || m.Sum != 5 || len(m.Buckets) != 1 || !math.IsInf(m.Buckets[0].LE, 1) {
		t.Errorf("no-bounds collect = %+v", m)
	}
}
