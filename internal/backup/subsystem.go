package backup

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/metrics"
)

// DefaultInterval is how often the backup subsystem exports when none is set.
const DefaultInterval = 6 * time.Hour

// Subsystem runs the exporter on a paced loop, writing the node's state to the
// configured target. It fits silod's subsystem interface (Name/Start/Shutdown)
// and exposes Prometheus metrics. The first export fires one interval in, not at
// startup, so a restart loop cannot trigger a backup storm.
type Subsystem struct {
	exporter *Exporter
	target   blobstore.Target
	interval time.Duration
	logger   *slog.Logger

	runs     atomic.Int64
	failures atomic.Int64
	chunks   atomic.Int64

	stop chan struct{}
	done chan struct{}
}

// NewSubsystem builds the backup subsystem. A non-positive interval falls back
// to DefaultInterval.
func NewSubsystem(exporter *Exporter, target blobstore.Target, interval time.Duration, logger *slog.Logger) *Subsystem {
	if interval <= 0 {
		interval = DefaultInterval
	}
	return &Subsystem{
		exporter: exporter,
		target:   target,
		interval: interval,
		logger:   logger,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Name identifies the subsystem in lifecycle logs.
func (s *Subsystem) Name() string { return "backup" }

// Start runs the export loop until Shutdown.
func (s *Subsystem) Start() error {
	defer close(s.done)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-s.stop
		cancel()
	}()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return nil
		case <-ticker.C:
			s.runOnce(ctx)
		}
	}
}

// Shutdown stops the loop.
func (s *Subsystem) Shutdown(ctx context.Context) error {
	close(s.stop)
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("backup subsystem did not stop within the shutdown deadline (%w); an upload may be in flight", ctx.Err())
	}
}

// runOnce performs one export, recording the outcome for the metrics scrape.
func (s *Subsystem) runOnce(ctx context.Context) {
	s.runs.Add(1)
	stats, err := s.exporter.Export(ctx, s.target)
	if err != nil {
		s.failures.Add(1)
		s.logger.Warn("backup failed; will retry next cycle", "target", s.target.Name(), "error", err)
		return
	}
	s.chunks.Store(int64(stats.Chunks))
	s.logger.Info("backup complete", "target", s.target.Name(), "chunks", stats.Chunks, "bytes", stats.Bytes)
}

// MetricPrefix namespaces the backup metrics.
func (s *Subsystem) MetricPrefix() string { return "silo_backup" }

// CollectMetrics reports backup activity: total runs, total failures, and the
// chunk count of the most recent successful export.
func (s *Subsystem) CollectMetrics() []metrics.Metric {
	return []metrics.Metric{
		{Name: "runs_total", Help: "Backup export runs attempted.", Kind: metrics.Counter, Value: float64(s.runs.Load())},
		{Name: "failures_total", Help: "Backup export runs that failed.", Kind: metrics.Counter, Value: float64(s.failures.Load())},
		{Name: "last_chunks", Help: "Chunks written by the most recent successful backup.", Kind: metrics.Gauge, Value: float64(s.chunks.Load())},
	}
}
