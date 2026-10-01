// Package clockskew watches the gap between this node's physical clock and the
// clocks of its peers. silo orders concurrent writes by hybrid logical clock;
// if one node's wall clock runs far ahead, its timestamps dominate and can make
// other nodes' writes look stale. Every gossip snapshot carries the sender's
// wall clock at send time; the monitor compares that against this node's clock
// on receipt, keeps the latest reading per peer, and warns when a peer's clock
// runs ahead beyond a configured threshold. That is the early signal that time
// sync (NTP/chrony) is misconfigured somewhere in the cluster.
package clockskew

import (
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hyperized/silo/internal/metrics"
)

// DefaultWarnInterval rate-limits the skew warning so a sustained skew logs
// once a minute rather than on every gossip round.
const DefaultWarnInterval = time.Minute

// DefaultStaleAfter is how long a peer's reading is kept without a fresh
// observation. Gossip refreshes every live peer within seconds, so a reading
// this old belongs to a node that left; dropping it keeps the per-peer gauge
// from reporting a frozen value forever.
const DefaultStaleAfter = 5 * time.Minute

type reading struct {
	skew time.Duration
	at   time.Time
}

// Monitor records the skew between this node and its peers. It is safe for
// concurrent use; Observe is called from every anti-entropy merge.
type Monitor struct {
	threshold    time.Duration
	warnInterval time.Duration
	staleAfter   time.Duration
	now          func() time.Time
	logger       *slog.Logger

	mu       sync.Mutex
	last     time.Duration      // last observed signed skew; >0 means a peer is ahead of us
	peers    map[string]reading // latest reading per peer, pruned after staleAfter
	alerts   uint64             // observations that exceeded the threshold
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

// WithStaleAfter overrides how long a peer's reading survives without a fresh
// observation. Non-positive values are ignored.
func WithStaleAfter(d time.Duration) Option {
	return func(m *Monitor) {
		if d > 0 {
			m.staleAfter = d
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
		staleAfter:   DefaultStaleAfter,
		now:          time.Now,
		logger:       logger,
		peers:        make(map[string]reading),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Observe records the skew implied by peerWall, the wall clock (unix
// nanoseconds) peerNode read just before sending. A positive skew means the
// peer's clock is ahead of this node's; transport latency pulls the reading
// slightly negative. It warns, at most once per warn interval, when a peer
// runs ahead beyond the threshold.
func (m *Monitor) Observe(peerNode string, peerWall int64) {
	now := m.now()
	skew := time.Duration(peerWall - now.UnixNano())

	m.mu.Lock()
	m.last = skew
	m.peers[peerNode] = reading{skew: skew, at: now}
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

// Last returns the most recently observed signed skew, from whichever peer
// was observed last.
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

// CollectMetrics reports the current skew to each recently seen peer and the
// alert count. Peers not observed within the stale window are dropped here,
// on the scrape, so Observe stays a constant-time map write. Series are sorted
// by peer so the exposition is deterministic.
func (m *Monitor) CollectMetrics() []metrics.Metric {
	now := m.now()
	m.mu.Lock()
	out := make([]metrics.Metric, 0, len(m.peers)+1)
	for peer, r := range m.peers {
		if now.Sub(r.at) > m.staleAfter {
			delete(m.peers, peer)
			continue
		}
		out = append(out, metrics.Metric{
			Name:   "peer_clock_skew_seconds",
			Help:   "Clock skew to a peer, measured from its gossip send time on receipt; positive means the peer is ahead of this node. Includes one-way gossip latency.",
			Kind:   metrics.Gauge,
			Value:  r.skew.Seconds(),
			Labels: [][2]string{{"peer", peer}},
		})
	}
	alerts := float64(m.alerts)
	m.mu.Unlock()
	slices.SortFunc(out, func(a, b metrics.Metric) int {
		return strings.Compare(a.Labels[0][1], b.Labels[0][1])
	})
	return append(out, metrics.Metric{
		Name:  "clock_skew_alerts_total",
		Help:  "Times a peer's clock exceeded the configured skew threshold.",
		Kind:  metrics.Counter,
		Value: alerts,
	})
}
