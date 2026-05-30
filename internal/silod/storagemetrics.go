package silod

import (
	"context"

	"github.com/hyperized/silo/internal/diskusage"
	"github.com/hyperized/silo/internal/metrics"
)

// chunkLister is the slice of the chunk store the storage metrics need.
type chunkLister interface {
	List(ctx context.Context) ([]string, error)
}

// storageMetrics exposes this node's backing-storage capacity to Prometheus: the
// filesystem behind the data directory and the number of chunks held locally.
// It implements metrics.Source, so the exporter pulls fresh readings on every
// scrape. A reading that cannot be taken (a transient statfs or list error) is
// simply omitted that scrape rather than reported as a misleading zero.
type storageMetrics struct {
	store   chunkLister
	dataDir string
	nodeID  string
	measure func(path string) (diskusage.Usage, error)
}

// storageMetricsOption configures a storageMetrics.
type storageMetricsOption func(*storageMetrics)

// withMeasure overrides the filesystem measurement (tests inject a fake).
func withMeasure(fn func(path string) (diskusage.Usage, error)) storageMetricsOption {
	return func(m *storageMetrics) { m.measure = fn }
}

// newStorageMetrics builds the storage metrics source for nodeID's data dir.
func newStorageMetrics(store chunkLister, dataDir, nodeID string, opts ...storageMetricsOption) *storageMetrics {
	m := &storageMetrics{store: store, dataDir: dataDir, nodeID: nodeID, measure: diskusage.Measure}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// MetricPrefix namespaces these readings.
func (m *storageMetrics) MetricPrefix() string { return "silo_storage" }

// CollectMetrics reports the data filesystem's capacity/used/available bytes and
// the local chunk count, each labelled with the node id.
func (m *storageMetrics) CollectMetrics() []metrics.Metric {
	labels := [][2]string{{"node", m.nodeID}}
	out := make([]metrics.Metric, 0, 4)
	if u, err := m.measure(m.dataDir); err == nil {
		out = append(out,
			metrics.Metric{Name: "capacity_bytes", Help: "Total bytes of the filesystem backing the data directory.", Kind: metrics.Gauge, Value: float64(u.CapacityBytes), Labels: labels},
			metrics.Metric{Name: "used_bytes", Help: "Used bytes of the filesystem backing the data directory.", Kind: metrics.Gauge, Value: float64(u.UsedBytes), Labels: labels},
			metrics.Metric{Name: "available_bytes", Help: "Available bytes of the filesystem backing the data directory.", Kind: metrics.Gauge, Value: float64(u.AvailableBytes), Labels: labels},
		)
	}
	if ids, err := m.store.List(context.Background()); err == nil {
		out = append(out, metrics.Metric{Name: "chunks", Help: "Number of chunks held on this node.", Kind: metrics.Gauge, Value: float64(len(ids)), Labels: labels})
	}
	return out
}
