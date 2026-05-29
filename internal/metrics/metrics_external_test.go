package metrics_test

import (
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
