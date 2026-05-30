package replication

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/hyperized/silo/internal/diskusage"
	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/metrics"
)

// DefaultRebalanceInterval is how often the rebalancer re-measures local disk
// and re-advertises capacity when none is configured.
const DefaultRebalanceInterval = 60 * time.Second

// capacityAdvertiser is the slice of the membership table the rebalancer drives.
// *membership.Membership satisfies it.
type capacityAdvertiser interface {
	SetSelfCapacity(capacityBytes, usedBytes int64) bool
	Members() []membership.Node
}

// Rebalancer keeps this node's advertised backing-store capacity current and
// surfaces cluster capacity skew. Capacity feeds the capacity-weighted placement
// ring: when a node advertises more (or a bigger node joins), the ring gives it
// proportionally more chunks and the scrubber moves data to match — that
// movement is the rebalance. The rebalancer itself moves nothing; it just keeps
// the inputs fresh and reports how balanced the cluster is.
type Rebalancer struct {
	members  capacityAdvertiser
	dataDir  string
	interval time.Duration
	measure  func(path string) (diskusage.Usage, error)
	logger   *slog.Logger

	skew    atomic.Uint64 // float64 bits of the last computed used-fraction skew
	adverts atomic.Int64

	stop chan struct{}
	done chan struct{}
}

// RebalancerOption configures a Rebalancer.
type RebalancerOption func(*Rebalancer)

// WithRebalanceMeasure overrides how the rebalancer measures the data directory
// (tests inject a fake).
func WithRebalanceMeasure(fn func(path string) (diskusage.Usage, error)) RebalancerOption {
	return func(r *Rebalancer) { r.measure = fn }
}

// NewRebalancer builds the rebalancer for nodeID's data dir. A non-positive
// interval falls back to DefaultRebalanceInterval.
func NewRebalancer(members capacityAdvertiser, dataDir string, interval time.Duration, logger *slog.Logger, opts ...RebalancerOption) *Rebalancer {
	if interval <= 0 {
		interval = DefaultRebalanceInterval
	}
	r := &Rebalancer{
		members:  members,
		dataDir:  dataDir,
		interval: interval,
		measure:  diskusage.Measure,
		logger:   logger,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Name identifies the subsystem in silod's lifecycle logs.
func (r *Rebalancer) Name() string { return "rebalancer" }

// Start advertises capacity once immediately (so peers learn it promptly) then
// refreshes it on a paced loop until Shutdown.
func (r *Rebalancer) Start() error {
	defer close(r.done)
	r.runOnce()
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

// Shutdown stops the loop.
func (r *Rebalancer) Shutdown(ctx context.Context) error {
	close(r.stop)
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("rebalancer did not stop within the shutdown deadline (%w)", ctx.Err())
	}
}

// runOnce measures local disk, advertises the figures if they changed, and
// recomputes the cluster's used-fraction skew for the metrics scrape.
func (r *Rebalancer) runOnce() {
	if u, err := r.measure(r.dataDir); err != nil {
		r.logger.Warn("rebalancer could not measure the data directory; will retry", "dir", r.dataDir, "error", err)
	} else if r.members.SetSelfCapacity(u.CapacityBytes, u.UsedBytes) {
		r.adverts.Add(1)
	}
	r.skew.Store(math.Float64bits(clusterSkew(r.members.Members())))
}

// MetricPrefix namespaces the rebalancer's readings.
func (r *Rebalancer) MetricPrefix() string { return "silo_rebalancer" }

// CollectMetrics reports the cluster's capacity skew (the spread in used
// fraction between the fullest and emptiest node — zero is perfectly balanced)
// and how many times this node has re-advertised its capacity.
func (r *Rebalancer) CollectMetrics() []metrics.Metric {
	return []metrics.Metric{
		{Name: "capacity_skew", Help: "Used-fraction spread between the fullest and emptiest node (0 = balanced).", Kind: metrics.Gauge, Value: math.Float64frombits(r.skew.Load())},
		{Name: "advertisements_total", Help: "Times this node has re-advertised its capacity over gossip.", Kind: metrics.Counter, Value: float64(r.adverts.Load())},
	}
}

// clusterSkew is the difference between the highest and lowest used fraction
// across nodes that have advertised a positive capacity. Zero when fewer than
// two such nodes exist.
func clusterSkew(nodes []membership.Node) float64 {
	minFrac, maxFrac := math.Inf(1), math.Inf(-1)
	seen := 0
	for _, n := range nodes {
		if n.CapacityBytes <= 0 {
			continue
		}
		seen++
		frac := float64(n.UsedBytes) / float64(n.CapacityBytes)
		minFrac = math.Min(minFrac, frac)
		maxFrac = math.Max(maxFrac, frac)
	}
	if seen < 2 {
		return 0
	}
	return maxFrac - minFrac
}
