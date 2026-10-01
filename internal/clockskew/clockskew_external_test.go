package clockskew_test

import (
	"bytes"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/clockskew"
	"github.com/hyperized/silo/internal/metrics"
)

// A Monitor is a metric source the exporter can register.
var _ metrics.Source = (*clockskew.Monitor)(nil)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestMonitor_ObserveTracksSignedSkew(t *testing.T) {
	now := time.Unix(1_000, 0)
	m := clockskew.New(time.Second, discardLogger(), clockskew.WithNow(func() time.Time { return now }))

	m.Observe("peer", now.Add(2*time.Second).UnixNano()) // peer 2s ahead
	if got := m.Last(); got != 2*time.Second {
		t.Errorf("last skew = %v, want 2s", got)
	}
	m.Observe("peer", now.Add(-3*time.Second).UnixNano()) // peer 3s behind
	if got := m.Last(); got != -3*time.Second {
		t.Errorf("last skew = %v, want -3s", got)
	}
}

func TestMonitor_WarnsAndCountsOverThreshold(t *testing.T) {
	now := time.Unix(2_000, 0)
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	m := clockskew.New(500*time.Millisecond, logger,
		clockskew.WithNow(func() time.Time { return now }),
		clockskew.WithWarnInterval(time.Minute),
	)

	m.Observe("fast-node", now.Add(time.Second).UnixNano()) // 1s ahead > 500ms
	if m.Alerts() != 1 {
		t.Fatalf("alerts = %d, want 1", m.Alerts())
	}
	if !strings.Contains(buf.String(), "fast-node") || !strings.Contains(buf.String(), "ahead") {
		t.Errorf("warning did not name the peer/problem: %q", buf.String())
	}

	// A second observation within the warn interval is rate-limited.
	m.Observe("fast-node", now.Add(time.Second).UnixNano())
	if m.Alerts() != 1 {
		t.Errorf("alerts = %d after a rate-limited observation, want still 1", m.Alerts())
	}

	// Past the interval, it warns again.
	now = now.Add(2 * time.Minute)
	m.Observe("fast-node", now.Add(time.Second).UnixNano())
	if m.Alerts() != 2 {
		t.Errorf("alerts = %d after the interval elapsed, want 2", m.Alerts())
	}
}

func TestMonitor_QuietWithinThresholdOrBehind(t *testing.T) {
	now := time.Unix(3_000, 0)
	m := clockskew.New(500*time.Millisecond, discardLogger(), clockskew.WithNow(func() time.Time { return now }))

	m.Observe("p", now.Add(100*time.Millisecond).UnixNano()) // within threshold
	m.Observe("p", now.Add(-5*time.Second).UnixNano())       // behind us
	if m.Alerts() != 0 {
		t.Errorf("alerts = %d, want 0 for in-threshold and behind peers", m.Alerts())
	}
}

func TestMonitor_WarnIntervalZeroKeepsDefault(t *testing.T) {
	now := time.Unix(4_000, 0)
	// WithWarnInterval(0) is ignored, so the default minute still rate-limits.
	m := clockskew.New(time.Millisecond, discardLogger(),
		clockskew.WithNow(func() time.Time { return now }),
		clockskew.WithWarnInterval(0),
	)
	m.Observe("p", now.Add(time.Second).UnixNano())
	m.Observe("p", now.Add(time.Second).UnixNano())
	if m.Alerts() != 1 {
		t.Errorf("alerts = %d, want 1 — a zero warn interval should fall back to the default", m.Alerts())
	}
}

const skewHelp = "Clock skew to a peer, measured from its gossip send time on receipt; positive means the peer is ahead of this node. Includes one-way gossip latency."

func skewSeries(peer string, v float64) metrics.Metric {
	return metrics.Metric{Name: "peer_clock_skew_seconds", Help: skewHelp, Kind: metrics.Gauge, Value: v, Labels: [][2]string{{"peer", peer}}}
}

func alertsSeries(v float64) metrics.Metric {
	return metrics.Metric{Name: "clock_skew_alerts_total", Help: "Times a peer's clock exceeded the configured skew threshold.", Kind: metrics.Counter, Value: v}
}

func TestMonitor_CollectMetrics(t *testing.T) {
	now := time.Unix(6_000, 0)
	m := clockskew.New(500*time.Millisecond, discardLogger(), clockskew.WithNow(func() time.Time { return now }))
	m.Observe("p", now.Add(2*time.Second).UnixNano()) // 2s ahead -> one alert

	if m.MetricPrefix() != "silo_hlc" {
		t.Errorf("prefix = %q, want silo_hlc", m.MetricPrefix())
	}
	got := m.CollectMetrics()
	want := []metrics.Metric{skewSeries("p", 2), alertsSeries(1)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("CollectMetrics = %+v, want %+v", got, want)
	}
}

func TestMonitor_CollectMetricsPerPeerSorted(t *testing.T) {
	now := time.Unix(7_000, 0)
	m := clockskew.New(time.Second, discardLogger(), clockskew.WithNow(func() time.Time { return now }))
	m.Observe("node-c", now.Add(-30*time.Millisecond).UnixNano())
	m.Observe("node-a", now.Add(10*time.Millisecond).UnixNano())
	m.Observe("node-b", now.Add(-5*time.Millisecond).UnixNano())
	m.Observe("node-a", now.Add(20*time.Millisecond).UnixNano()) // replaces the earlier node-a reading

	got := m.CollectMetrics()
	want := []metrics.Metric{
		skewSeries("node-a", 0.02),
		skewSeries("node-b", -0.005),
		skewSeries("node-c", -0.03),
		alertsSeries(0),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("CollectMetrics = %+v, want %+v", got, want)
	}
}

func TestMonitor_DropsStalePeers(t *testing.T) {
	now := time.Unix(8_000, 0)
	m := clockskew.New(time.Second, discardLogger(),
		clockskew.WithNow(func() time.Time { return now }),
		clockskew.WithStaleAfter(time.Minute),
	)
	m.Observe("gone", now.UnixNano())
	now = now.Add(30 * time.Second)
	m.Observe("live", now.UnixNano())

	now = now.Add(45 * time.Second) // "gone" last seen 75s ago, "live" 45s ago
	got := m.CollectMetrics()
	want := []metrics.Metric{skewSeries("live", 0), alertsSeries(0)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("CollectMetrics = %+v, want %+v", got, want)
	}

	// Pruned for good: a later scrape inside "gone"'s old window does not revive it.
	if got := m.CollectMetrics(); len(got) != 2 {
		t.Errorf("second scrape returned %d metrics, want 2", len(got))
	}
}

func TestMonitor_StaleAfterNonPositiveKeepsDefault(t *testing.T) {
	now := time.Unix(9_000, 0)
	m := clockskew.New(time.Second, discardLogger(),
		clockskew.WithNow(func() time.Time { return now }),
		clockskew.WithStaleAfter(0),
	)
	m.Observe("p", now.UnixNano())
	now = now.Add(clockskew.DefaultStaleAfter) // exactly at the edge: still kept
	if got := m.CollectMetrics(); len(got) != 2 {
		t.Fatalf("at the default window edge got %d metrics, want 2", len(got))
	}
	now = now.Add(time.Nanosecond)
	if got := m.CollectMetrics(); len(got) != 1 {
		t.Errorf("past the default window got %d metrics, want 1", len(got))
	}
}

func TestMonitor_NilLoggerDoesNotPanic(t *testing.T) {
	now := time.Unix(5_000, 0)
	m := clockskew.New(time.Millisecond, nil, clockskew.WithNow(func() time.Time { return now }))
	m.Observe("p", now.Add(time.Second).UnixNano()) // would panic on a nil logger
	if m.Alerts() != 1 {
		t.Errorf("alerts = %d, want 1", m.Alerts())
	}
}
