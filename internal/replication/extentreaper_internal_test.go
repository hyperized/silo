package replication

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeLive struct{ ids map[string]struct{} }

func (f fakeLive) ReferencedInodeIDs() map[string]struct{} { return f.ids }

type fakeReapStore struct {
	mu        sync.Mutex
	reaped    []string
	err       error
	calls     int
	gotLive   map[string]struct{}
	gotBefore time.Time
	ch        chan struct{}
}

func (s *fakeReapStore) Reap(live map[string]struct{}, before time.Time) ([]string, error) {
	s.mu.Lock()
	s.calls++
	s.gotLive = live
	s.gotBefore = before
	s.mu.Unlock()
	if s.ch != nil {
		select {
		case s.ch <- struct{}{}:
		default:
		}
	}
	return s.reaped, s.err
}

func reaperMetric(r *ExtentReaper, name string) float64 {
	for _, m := range r.CollectMetrics() {
		if m.Name == name {
			return m.Value
		}
	}
	return -1
}

func TestExtentReaper_NewClampsDefaults(t *testing.T) {
	r := NewExtentReaper(fakeLive{}, &fakeReapStore{}, "n", 0, 0, quietLog())
	if r.reapAfter != DefaultExtentReapAfter {
		t.Errorf("reapAfter = %v, want default %v", r.reapAfter, DefaultExtentReapAfter)
	}
	if r.interval != DefaultExtentReapInterval {
		t.Errorf("interval = %v, want default %v", r.interval, DefaultExtentReapInterval)
	}
}

func TestExtentReaper_RunOnceReaps(t *testing.T) {
	live := map[string]struct{}{"live": {}}
	store := &fakeReapStore{reaped: []string{"v1", "v2"}}
	r := NewExtentReaper(fakeLive{ids: live}, store, "node-a", time.Hour, time.Minute, quietLog())
	fixed := time.Unix(1_000_000, 0)
	r.now = func() time.Time { return fixed }

	r.runOnce()

	store.mu.Lock()
	gotBefore, gotLive := store.gotBefore, store.gotLive
	store.mu.Unlock()
	if !gotBefore.Equal(fixed.Add(-time.Hour)) {
		t.Errorf("reapBefore = %v, want now-reapAfter %v", gotBefore, fixed.Add(-time.Hour))
	}
	if _, ok := gotLive["live"]; !ok || len(gotLive) != 1 {
		t.Errorf("live set not forwarded: %v", gotLive)
	}
	if reaperMetric(r, "reaped_total") != 2 || reaperMetric(r, "last_reap_reclaimed") != 2 {
		t.Errorf("metrics wrong: total=%v last=%v", reaperMetric(r, "reaped_total"), reaperMetric(r, "last_reap_reclaimed"))
	}
	// A second sweep that reaps nothing resets last but not the cumulative total.
	store.reaped = nil
	r.runOnce()
	if reaperMetric(r, "reaped_total") != 2 || reaperMetric(r, "last_reap_reclaimed") != 0 {
		t.Errorf("after empty sweep: total=%v last=%v", reaperMetric(r, "reaped_total"), reaperMetric(r, "last_reap_reclaimed"))
	}
}

func TestExtentReaper_RunOnceErrorLogged(t *testing.T) {
	var buf bytes.Buffer
	store := &fakeReapStore{err: errors.New("read-only data dir")}
	r := NewExtentReaper(fakeLive{}, store, "n", time.Hour, time.Minute, slog.New(slog.NewTextHandler(&buf, nil)))
	r.runOnce()
	if !strings.Contains(buf.String(), "could not reclaim") {
		t.Errorf("a reap failure should be logged, got: %q", buf.String())
	}
}

func TestExtentReaper_StartShutdown(t *testing.T) {
	store := &fakeReapStore{ch: make(chan struct{}, 8)}
	r := NewExtentReaper(fakeLive{}, store, "n", time.Hour, 5*time.Millisecond, quietLog())
	if r.Name() != "extent-reaper" || r.MetricPrefix() != "silo_extentmap" {
		t.Errorf("identity = %q/%q", r.Name(), r.MetricPrefix())
	}
	go func() { _ = r.Start() }()
	select {
	case <-store.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("reaper did not run a sweep")
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestExtentReaper_ShutdownDeadline(t *testing.T) {
	// Shutdown without Start: done never closes, so an expired ctx hits the deadline.
	r := NewExtentReaper(fakeLive{}, &fakeReapStore{}, "n", time.Hour, time.Hour, quietLog())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Shutdown(ctx); err == nil {
		t.Error("Shutdown with an expired context should error")
	}
}
