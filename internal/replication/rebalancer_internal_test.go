package replication

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/diskusage"
	"github.com/hyperized/silo/internal/membership"
)

type fakeAdvertiser struct {
	setCalls   int
	lastCap    int64
	lastUsed   int64
	setChanged bool
	nodes      []membership.Node
}

func (f *fakeAdvertiser) SetSelfCapacity(capacityBytes, usedBytes int64) bool {
	f.setCalls++
	f.lastCap, f.lastUsed = capacityBytes, usedBytes
	return f.setChanged
}

func (f *fakeAdvertiser) Members() []membership.Node { return f.nodes }

func rebalLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func rebalMetric(t *testing.T, r *Rebalancer, name string) float64 {
	t.Helper()
	for _, m := range r.CollectMetrics() {
		if m.Name == name {
			return m.Value
		}
	}
	t.Fatalf("rebalancer did not report %q", name)
	return 0
}

func TestRebalancer_AdvertisesAndComputesSkew(t *testing.T) {
	adv := &fakeAdvertiser{
		setChanged: true,
		nodes: []membership.Node{
			{ID: "a", CapacityBytes: 1000, UsedBytes: 100}, // 10% full
			{ID: "b", CapacityBytes: 1000, UsedBytes: 900}, // 90% full
			{ID: "c"}, // no capacity advertised -> ignored
		},
	}
	r := NewRebalancer(adv, "/data", time.Hour, rebalLogger(),
		WithRebalanceMeasure(func(string) (diskusage.Usage, error) {
			return diskusage.Usage{CapacityBytes: 5000, UsedBytes: 2500}, nil
		}))

	if r.Name() != "rebalancer" || r.MetricPrefix() != "silo_rebalancer" {
		t.Errorf("identity = %q/%q", r.Name(), r.MetricPrefix())
	}

	r.runOnce()

	if adv.setCalls != 1 || adv.lastCap != 5000 || adv.lastUsed != 2500 {
		t.Errorf("advertise = (calls %d, %d/%d), want one 5000/2500", adv.setCalls, adv.lastCap, adv.lastUsed)
	}
	if got := rebalMetric(t, r, "advertisements_total"); got != 1 {
		t.Errorf("advertisements = %v, want 1", got)
	}
	// skew = 0.90 - 0.10 = 0.80 (c is ignored).
	if got := rebalMetric(t, r, "capacity_skew"); got < 0.79 || got > 0.81 {
		t.Errorf("capacity_skew = %v, want ~0.80", got)
	}
}

func TestRebalancer_NoAdvertiseWhenUnchanged(t *testing.T) {
	adv := &fakeAdvertiser{setChanged: false}
	r := NewRebalancer(adv, "/data", time.Hour, rebalLogger(),
		WithRebalanceMeasure(func(string) (diskusage.Usage, error) { return diskusage.Usage{CapacityBytes: 1}, nil }))
	r.runOnce()
	if got := rebalMetric(t, r, "advertisements_total"); got != 0 {
		t.Errorf("advertisements = %v, want 0 when capacity unchanged", got)
	}
}

func TestRebalancer_MeasureErrorIsNotFatal(t *testing.T) {
	adv := &fakeAdvertiser{}
	r := NewRebalancer(adv, "/data", time.Hour, rebalLogger(),
		WithRebalanceMeasure(func(string) (diskusage.Usage, error) { return diskusage.Usage{}, errors.New("statfs") }))
	r.runOnce() // must not panic
	if adv.setCalls != 0 {
		t.Error("a measure failure must not advertise")
	}
}

func TestRebalancer_DefaultIntervalAndShutdown(t *testing.T) {
	r := NewRebalancer(&fakeAdvertiser{}, "/data", 0, rebalLogger(),
		WithRebalanceMeasure(func(string) (diskusage.Usage, error) { return diskusage.Usage{CapacityBytes: 1}, nil }))
	if r.interval != DefaultRebalanceInterval {
		t.Errorf("interval = %v, want default %v", r.interval, DefaultRebalanceInterval)
	}

	go func() { _ = r.Start() }()
	time.Sleep(20 * time.Millisecond) // let Start's immediate runOnce fire
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestRebalancer_TickerFires(t *testing.T) {
	adv := &fakeAdvertiser{setChanged: true}
	r := NewRebalancer(adv, "/data", 5*time.Millisecond, rebalLogger(),
		WithRebalanceMeasure(func(string) (diskusage.Usage, error) { return diskusage.Usage{CapacityBytes: 1}, nil }))
	go func() { _ = r.Start() }()
	time.Sleep(40 * time.Millisecond) // immediate runOnce + several ticks
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if adv.setCalls < 2 {
		t.Errorf("advertise calls = %d, want the ticker to have fired at least once beyond the initial", adv.setCalls)
	}
}

func TestRebalancer_ShutdownDeadline(t *testing.T) {
	// Shutdown without a running Start: done never closes, so an already-expired
	// context drives the deadline branch.
	r := NewRebalancer(&fakeAdvertiser{}, "/data", time.Hour, rebalLogger())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Shutdown(ctx); err == nil {
		t.Error("Shutdown with an expired context should return a deadline error")
	}
}

func TestClusterSkew_FewerThanTwo(t *testing.T) {
	if s := clusterSkew([]membership.Node{{ID: "a", CapacityBytes: 1000, UsedBytes: 500}}); s != 0 {
		t.Errorf("skew with one node = %v, want 0", s)
	}
	if s := clusterSkew(nil); s != 0 {
		t.Errorf("skew with no nodes = %v, want 0", s)
	}
}
