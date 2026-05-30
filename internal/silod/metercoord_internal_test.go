package silod

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/metrics"
)

type meterFakeCoord struct {
	deleted string
	statted string
	err     error
}

func (f *meterFakeCoord) Write(context.Context, string, []byte) (chunkstore.Info, error) {
	return chunkstore.Info{}, f.err
}

func (f *meterFakeCoord) Read(context.Context, string) ([]byte, chunkstore.Info, error) {
	return []byte("x"), chunkstore.Info{}, f.err
}

func (f *meterFakeCoord) Delete(_ context.Context, id string) error { f.deleted = id; return f.err }

func (f *meterFakeCoord) Stat(_ context.Context, id string) (chunkstore.Info, error) {
	f.statted = id
	return chunkstore.Info{}, f.err
}

func TestMeteredCoord(t *testing.T) {
	fc := &meterFakeCoord{}
	m := newMeteredCoord(fc, "node-a")

	// A clock that advances 2ms on every read, so each timed op observes 2ms.
	base := time.Unix(0, 0)
	var i int
	m.now = func() time.Time {
		t := base.Add(time.Duration(i) * 2 * time.Millisecond)
		i++
		return t
	}

	ctx := context.Background()
	if _, err := m.Write(ctx, "c1", []byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, _, err := m.Read(ctx, "c1"); err != nil {
		t.Fatalf("Read: %v", err)
	}
	// Delete/Stat pass through.
	_ = m.Delete(ctx, "c1")
	_, _ = m.Stat(ctx, "c1")
	if fc.deleted != "c1" || fc.statted != "c1" {
		t.Errorf("passthrough = delete %q stat %q", fc.deleted, fc.statted)
	}

	if m.MetricPrefix() != "silo_chunk" {
		t.Errorf("prefix = %q", m.MetricPrefix())
	}
	got := m.CollectMetrics()
	for _, name := range []string{"write_latency_seconds", "read_latency_seconds"} {
		mt, ok := metricByName(got, name)
		if !ok {
			t.Fatalf("missing metric %q", name)
		}
		if mt.Kind != metrics.Histogram || mt.Count != 1 {
			t.Errorf("%s = kind %v count %d, want histogram 1", name, mt.Kind, mt.Count)
		}
		if mt.Sum < 0.0019 || mt.Sum > 0.0021 {
			t.Errorf("%s sum = %v, want ~0.002", name, mt.Sum)
		}
		if len(mt.Labels) != 1 || mt.Labels[0] != [2]string{"node", "node-a"} {
			t.Errorf("%s labels = %v", name, mt.Labels)
		}
	}
}

func TestMeteredCoord_PropagatesErrors(t *testing.T) {
	m := newMeteredCoord(&meterFakeCoord{err: errors.New("boom")}, "n")
	if _, err := m.Write(context.Background(), "c", nil); err == nil {
		t.Error("Write should propagate the inner error")
	}
	if _, _, err := m.Read(context.Background(), "c"); err == nil {
		t.Error("Read should propagate the inner error")
	}
	if err := m.Delete(context.Background(), "c"); err == nil {
		t.Error("Delete should propagate the inner error")
	}
	if _, err := m.Stat(context.Background(), "c"); err == nil {
		t.Error("Stat should propagate the inner error")
	}
}
