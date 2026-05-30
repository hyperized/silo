package silod

import (
	"context"
	"time"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/metrics"
	"github.com/hyperized/silo/internal/transport"
)

// latencyBuckets are the histogram bounds (seconds) for chunk read/write
// latency — fine-grained around the sub-millisecond/few-millisecond range a
// healthy local store hits, with a long tail for replication stalls.
var latencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// meteredCoord wraps a replication Coordinator and times its writes and reads
// into Prometheus histograms. It satisfies both transport.Coordinator (so it
// drops in transparently) and metrics.Source.
type meteredCoord struct {
	inner  transport.Coordinator
	nodeID string
	now    func() time.Time

	writes *metrics.Hist
	reads  *metrics.Hist
}

// newMeteredCoord wraps inner with read/write latency instrumentation.
func newMeteredCoord(inner transport.Coordinator, nodeID string) *meteredCoord {
	return &meteredCoord{
		inner:  inner,
		nodeID: nodeID,
		now:    time.Now,
		writes: metrics.NewHist(latencyBuckets...),
		reads:  metrics.NewHist(latencyBuckets...),
	}
}

// Write times the underlying write into the write-latency histogram.
func (m *meteredCoord) Write(ctx context.Context, chunkID string, data []byte) (chunkstore.Info, error) {
	start := m.now()
	info, err := m.inner.Write(ctx, chunkID, data)
	m.writes.Observe(m.now().Sub(start).Seconds())
	return info, err
}

// Read times the underlying read into the read-latency histogram.
func (m *meteredCoord) Read(ctx context.Context, chunkID string) ([]byte, chunkstore.Info, error) {
	start := m.now()
	data, info, err := m.inner.Read(ctx, chunkID)
	m.reads.Observe(m.now().Sub(start).Seconds())
	return data, info, err
}

// Delete and Stat are passed through unmeasured (they are not on the data path
// the operator cares to chart).
func (m *meteredCoord) Delete(ctx context.Context, chunkID string) error {
	return m.inner.Delete(ctx, chunkID)
}

func (m *meteredCoord) Stat(ctx context.Context, chunkID string) (chunkstore.Info, error) {
	return m.inner.Stat(ctx, chunkID)
}

// MetricPrefix namespaces the chunk latency metrics.
func (m *meteredCoord) MetricPrefix() string { return "silo_chunk" }

// CollectMetrics reports the read and write latency distributions.
func (m *meteredCoord) CollectMetrics() []metrics.Metric {
	labels := [][2]string{{"node", m.nodeID}}
	return []metrics.Metric{
		m.writes.Collect("write_latency_seconds", "Chunk write (Put) latency through the replication coordinator.", labels),
		m.reads.Collect("read_latency_seconds", "Chunk read (Get) latency through the replication coordinator.", labels),
	}
}

// Compile-time checks.
var (
	_ transport.Coordinator = (*meteredCoord)(nil)
	_ metrics.Source        = (*meteredCoord)(nil)
)
