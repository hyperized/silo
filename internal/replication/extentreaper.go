package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/hyperized/silo/internal/metrics"
)

// DefaultExtentReapInterval is how often the reaper sweeps for orphaned extent
// maps when no interval is configured.
const DefaultExtentReapInterval = 15 * time.Minute

// DefaultExtentReapAfter is how long an extent map whose volume is gone from the
// namespace must have sat untouched before the reaper reclaims it, when none is
// configured. Generous so a freshly-created volume whose directory entry has not
// yet reached this node over gossip is never mistaken for a deleted one.
const DefaultExtentReapAfter = time.Hour

// ExtentReapStore is the local extent-map store the reaper prunes.
// *extentmap.Store satisfies it.
type ExtentReapStore interface {
	Reap(live map[string]struct{}, reapBefore time.Time) ([]string, error)
}

// LiveInodeSource yields the inode ids the namespace still refers to — the
// reaper's "is this volume still alive?" oracle. *namespace.Namespace satisfies
// it via ReferencedInodeIDs.
type LiveInodeSource interface {
	ReferencedInodeIDs() map[string]struct{}
}

// ExtentReaper reclaims the extent-map replicas of deleted volumes. On a paced
// loop it asks the namespace which inode ids are still referenced and tells the
// local store to drop every map whose volume is absent from that set and old
// enough that the removal has surely propagated. It is the GC backstop for the
// synchronous delete path (ExtentCoordinator.DeleteMap): it reclaims any copy a
// direct delete missed — a replica unreachable at delete time, or a serving
// node that warmed a map it does not place.
type ExtentReaper struct {
	live      LiveInodeSource
	store     ExtentReapStore
	nodeID    string
	reapAfter time.Duration
	interval  time.Duration
	logger    *slog.Logger
	now       func() time.Time

	reaped atomic.Int64
	last   atomic.Int64

	stop chan struct{}
	done chan struct{}
}

// NewExtentReaper builds a reaper. A non-positive interval or reapAfter falls
// back to the package default, so a misconfiguration paces conservatively
// rather than spinning or reaping too eagerly.
func NewExtentReaper(live LiveInodeSource, store ExtentReapStore, nodeID string, reapAfter, interval time.Duration, logger *slog.Logger) *ExtentReaper {
	if interval <= 0 {
		interval = DefaultExtentReapInterval
	}
	if reapAfter <= 0 {
		reapAfter = DefaultExtentReapAfter
	}
	return &ExtentReaper{
		live:      live,
		store:     store,
		nodeID:    nodeID,
		reapAfter: reapAfter,
		interval:  interval,
		logger:    logger,
		now:       time.Now,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Name identifies the subsystem in silod's lifecycle logs.
func (r *ExtentReaper) Name() string { return "extent-reaper" }

// Start runs the reap loop until Shutdown, returning nil on a clean stop so
// silod can treat it like every other subsystem.
func (r *ExtentReaper) Start() error {
	defer close(r.done)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return nil
		case <-ticker.C:
			r.runOnce()
		}
	}
}

// Shutdown stops the loop, waiting for an in-flight sweep to unwind or the
// context to expire.
func (r *ExtentReaper) Shutdown(ctx context.Context) error {
	close(r.stop)
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("extent-reaper did not stop within the shutdown deadline (%w); a reap sweep may be stuck on a slow data dir", ctx.Err())
	}
}

// runOnce reaps every locally-held extent map whose volume is no longer
// referenced by the namespace and whose file predates the reap-after window.
func (r *ExtentReaper) runOnce() {
	live := r.live.ReferencedInodeIDs()
	reaped, err := r.store.Reap(live, r.now().Add(-r.reapAfter))
	if err != nil {
		r.logger.Warn("extent-reaper could not reclaim some orphaned maps; will retry next cycle", "error", err)
	}
	r.last.Store(int64(len(reaped)))
	if len(reaped) > 0 {
		r.reaped.Add(int64(len(reaped)))
		r.logger.Info("extent-reaper reclaimed orphaned extent maps", "count", len(reaped), "volumes", reaped)
	}
}

// MetricPrefix namespaces the reaper's readings.
func (r *ExtentReaper) MetricPrefix() string { return "silo_extentmap" }

// CollectMetrics reports the cumulative orphaned maps this node has reclaimed
// and how many the last sweep reclaimed, labelled by node. A non-zero last_reap
// after a steady state means deletes are leaving orphans the reaper is cleaning.
func (r *ExtentReaper) CollectMetrics() []metrics.Metric {
	labels := [][2]string{{"node", r.nodeID}}
	return []metrics.Metric{
		{Name: "reaped_total", Help: "Orphaned extent maps this node has reclaimed.", Kind: metrics.Counter, Value: float64(r.reaped.Load()), Labels: labels},
		{Name: "last_reap_reclaimed", Help: "Orphaned extent maps reclaimed in the last reap sweep.", Kind: metrics.Gauge, Value: float64(r.last.Load()), Labels: labels},
	}
}
