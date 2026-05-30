package silod

import (
	"context"
	"errors"
	"testing"

	"github.com/hyperized/silo/internal/diskusage"
	"github.com/hyperized/silo/internal/metrics"
)

type fakeChunkLister struct {
	ids []string
	err error
}

func (f fakeChunkLister) List(context.Context) ([]string, error) { return f.ids, f.err }

func metricByName(ms []metrics.Metric, name string) (metrics.Metric, bool) {
	for _, m := range ms {
		if m.Name == name {
			return m, true
		}
	}
	return metrics.Metric{}, false
}

func TestStorageMetrics_CollectsAll(t *testing.T) {
	m := newStorageMetrics(fakeChunkLister{ids: []string{"a", "b", "c"}}, "/data", "node-1",
		withMeasure(func(string) (diskusage.Usage, error) {
			return diskusage.Usage{CapacityBytes: 1000, UsedBytes: 600, AvailableBytes: 400}, nil
		}))

	if m.MetricPrefix() != "silo_storage" {
		t.Errorf("prefix = %q", m.MetricPrefix())
	}
	got := m.CollectMetrics()
	if len(got) != 4 {
		t.Fatalf("metrics = %d, want 4: %+v", len(got), got)
	}
	for name, want := range map[string]float64{
		"capacity_bytes": 1000, "used_bytes": 600, "available_bytes": 400, "chunks": 3,
	} {
		mt, ok := metricByName(got, name)
		if !ok {
			t.Errorf("missing metric %q", name)
			continue
		}
		if mt.Value != want || mt.Kind != metrics.Gauge {
			t.Errorf("%s = %v (kind %v), want %v gauge", name, mt.Value, mt.Kind, want)
		}
		if len(mt.Labels) != 1 || mt.Labels[0] != [2]string{"node", "node-1"} {
			t.Errorf("%s labels = %v, want node=node-1", name, mt.Labels)
		}
	}
}

func TestStorageMetrics_OmitsOnError(t *testing.T) {
	// statfs fails -> only the chunk count is reported.
	measFail := newStorageMetrics(fakeChunkLister{ids: []string{"a"}}, "/data", "n",
		withMeasure(func(string) (diskusage.Usage, error) { return diskusage.Usage{}, errors.New("statfs") }))
	got := measFail.CollectMetrics()
	if len(got) != 1 {
		t.Fatalf("metrics = %d, want only chunks: %+v", len(got), got)
	}
	if _, ok := metricByName(got, "chunks"); !ok {
		t.Error("chunks metric should still be reported when statfs fails")
	}

	// list fails -> only the capacity gauges are reported.
	listFail := newStorageMetrics(fakeChunkLister{err: errors.New("list")}, "/data", "n",
		withMeasure(func(string) (diskusage.Usage, error) { return diskusage.Usage{CapacityBytes: 1}, nil }))
	got = listFail.CollectMetrics()
	if len(got) != 3 {
		t.Fatalf("metrics = %d, want the three capacity gauges: %+v", len(got), got)
	}
	if _, ok := metricByName(got, "chunks"); ok {
		t.Error("chunks metric should be omitted when list fails")
	}
}
