package clockskew_test

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/clockskew"
)

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

func TestMonitor_NilLoggerDoesNotPanic(t *testing.T) {
	now := time.Unix(5_000, 0)
	m := clockskew.New(time.Millisecond, nil, clockskew.WithNow(func() time.Time { return now }))
	m.Observe("p", now.Add(time.Second).UnixNano()) // would panic on a nil logger
	if m.Alerts() != 1 {
		t.Errorf("alerts = %d, want 1", m.Alerts())
	}
}
