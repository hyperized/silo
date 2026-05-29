// Package clockskew watches the gap between this node's physical clock and the
// clocks of its peers. silo orders concurrent writes by hybrid logical clock;
// if one node's wall clock runs far ahead, its timestamps dominate and can make
// other nodes' writes look stale. The monitor observes peer-issued timestamps
// as they arrive over anti-entropy, records the skew, and warns when a peer's
// clock runs ahead of this node beyond a configured threshold — the early
// signal that time sync (NTP/chrony) is misconfigured somewhere in the cluster.
package clockskew

import (
	"log/slog"
	"sync"
	"time"

	"github.com/hyperized/silo/internal/metrics"
)

// DefaultWarnInterval rate-limits the skew warning so a sustained skew logs
// once a minute rather than on every gossip round.
const DefaultWarnInterval = time.Minute

// Monitor records the skew between this node and its peers. It is safe for
// concurrent use; Observe is called from every anti-entropy merge.
type Monitor struct {
	threshold    time.Duration
	warnInterval time.Duration
	now          func() time.Time
	logger       *slog.Logger

	mu       sync.Mutex
	last     time.Duration // last observed signed skew; >0 means a peer is ahead of us
	alerts   uint64        // observations that exceeded the threshold
	lastWarn time.Time
}

// Option configures a Monitor.
type Option func(*Monitor)

// WithNow injects the physical clock source. Tests drive time through it;
// production leaves the default time.Now.
func WithNow(now func() time.Time) Option { return func(m *Monitor) { m.now = now } }

// WithWarnInterval overrides how often a sustained skew is allowed to log.
// Non-positive values are ignored.
func WithWarnInterval(d time.Duration) Option {
	return func(m *Monitor) {
		if d > 0 {
			m.warnInterval = d
		}
	}
}

// New builds a monitor that warns when a peer's clock runs more than threshold
// ahead of this node. A nil logger defaults to slog.Default.
func New(threshold time.Duration, logger *slog.Logger, opts ...Option) *Monitor {
	if logger == nil {
		logger = slog.Default()
	}
	m := &Monitor{
		threshold:    threshold,
		warnInterval: DefaultWarnInterval,
		now:          time.Now,
		logger:       logger,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Observe records the skew implied by a timestamp peerNode issued at peerWall
// (unix nanoseconds). A positive skew means the peer's clock is ahead of this
// node's. It warns, at most once per warn interval, when a peer runs ahead
// beyond the threshold.
func (m *Monitor) Observe(peerNode string, peerWall int64) {
	now := m.now()
	skew := time.Duration(peerWall - now.UnixNano())

	m.mu.Lock()
	m.last = skew
	warn := skew > m.threshold && now.Sub(m.lastWarn) >= m.warnInterval
	if warn {
		m.alerts++
		m.lastWarn = now
	}
	m.mu.Unlock()

	if warn {
		m.logger.Warn("a peer's clock is ahead of this node beyond the safe threshold; check time sync (NTP/chrony) on both hosts — large clock skew corrupts write ordering",
			"peer", peerNode,
			"skew", skew.String(),
			"threshold", m.threshold.String())
	}
}

// Last returns the most recently observed signed skew.
func (m *Monitor) Last() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last
}

// Alerts returns how many observations have exceeded the threshold.
func (m *Monitor) Alerts() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alerts
}

// MetricPrefix namespaces the monitor's metrics under the HLC subsystem.
func (m *Monitor) MetricPrefix() string { return "silo_hlc" }

// CollectMetrics reports the current skew and alert count for the exporter.
func (m *Monitor) CollectMetrics() []metrics.Metric {
	m.mu.Lock()
	last := m.last.Seconds()
	alerts := float64(m.alerts)
	m.mu.Unlock()
	return []metrics.Metric{
		{
			Name:  "peer_clock_skew_seconds",
			Help:  "Last observed clock skew to a peer; positive means the peer is ahead of this node.",
			Kind:  metrics.Gauge,
			Value: last,
		},
		{
			Name:  "clock_skew_alerts_total",
			Help:  "Times a peer's clock exceeded the configured skew threshold.",
			Kind:  metrics.Counter,
			Value: alerts,
		},
	}
}
