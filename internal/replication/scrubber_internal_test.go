package replication

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/chunkstore"
)

func nopLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// stubPlace is a fixed ring view: Replicas ignores the chunk id and returns
// the configured order, which is all the scrubber logic needs to exercise.
type stubPlace struct {
	self     string
	replicas []string
	addrs    map[string]string
}

func (p stubPlace) Replicas(_ string, n int) []string {
	if n > len(p.replicas) {
		n = len(p.replicas)
	}
	out := make([]string, n)
	copy(out, p.replicas[:n])
	return out
}

func (p stubPlace) DataAddr(id string) (string, bool) {
	a, ok := p.addrs[id]
	return a, ok
}

func (p stubPlace) SelfID() string { return p.self }

type fakeCatalog struct {
	ids     []string
	listErr error
	data    map[string][]byte
	getErr  map[string]error

	listOnce sync.Once
	listed   chan struct{}
}

func (c *fakeCatalog) List(context.Context) ([]string, error) {
	if c.listed != nil {
		c.listOnce.Do(func() { close(c.listed) })
	}
	if c.listErr != nil {
		return nil, c.listErr
	}
	return c.ids, nil
}

func (c *fakeCatalog) Get(_ context.Context, id string) ([]byte, chunkstore.Info, error) {
	if err := c.getErr[id]; err != nil {
		return nil, chunkstore.Info{}, err
	}
	return c.data[id], chunkstore.Info{ID: id}, nil
}

type fakeProbe struct {
	mu       sync.Mutex
	present  map[string]bool  // addr|id -> peer holds it
	storeErr map[string]error // addr -> error on Store
	stored   map[string]bool  // addr|id -> pushed by the scrubber
}

func key(addr, id string) string { return addr + "|" + id }

func (p *fakeProbe) Stat(_ context.Context, addr, id string) (chunkstore.Info, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.present[key(addr, id)] {
		return chunkstore.Info{ID: id}, nil
	}
	return chunkstore.Info{}, errors.New("not found")
}

func (p *fakeProbe) Store(_ context.Context, addr, id string, _ []byte) (chunkstore.Info, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.storeErr[addr]; err != nil {
		return chunkstore.Info{}, err
	}
	if p.stored == nil {
		p.stored = map[string]bool{}
	}
	p.stored[key(addr, id)] = true
	if p.present == nil {
		p.present = map[string]bool{}
	}
	p.present[key(addr, id)] = true
	return chunkstore.Info{ID: id}, nil
}

func (p *fakeProbe) pushed(addr, id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stored[key(addr, id)]
}

func (p *fakeProbe) pushCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.stored)
}

func TestNewScrubber_ClampsAndName(t *testing.T) {
	s := NewScrubber(stubPlace{self: "a"}, &fakeCatalog{}, &fakeProbe{}, 0, 0, nopLogger())
	if s.rf != 1 {
		t.Errorf("rf clamp: got %d, want 1", s.rf)
	}
	if s.interval != DefaultScrubInterval {
		t.Errorf("interval default: got %v, want %v", s.interval, DefaultScrubInterval)
	}
	if s.Name() != "scrubber" {
		t.Errorf("Name: got %q, want scrubber", s.Name())
	}
}

func TestScrubber_HealsMissingReplicaAsPrimary(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "b", "c"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000", "c": "c:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{key("c:7000", "c1"): true}} // c holds it, b does not
	s := NewScrubber(place, cat, probe, 3, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if !probe.pushed("b:7000", "c1") {
		t.Error("scrubber should re-replicate c1 to b (missing)")
	}
	if probe.pushed("c:7000", "c1") {
		t.Error("scrubber should not push c1 to c (already holds it)")
	}
}

func TestScrubber_DefersToHigherPriorityHolder(t *testing.T) {
	place := stubPlace{
		self:     "b",
		replicas: []string{"a", "b", "c"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000", "c": "c:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{key("a:7000", "c1"): true}} // a (rank 0) holds it
	s := NewScrubber(place, cat, probe, 3, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if probe.pushCount() != 0 {
		t.Errorf("b should defer to higher-priority holder a, made %d pushes", probe.pushCount())
	}
}

func TestScrubber_TakesOverWhenHigherPriorityMissing(t *testing.T) {
	place := stubPlace{
		self:     "b",
		replicas: []string{"a", "b", "c"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000", "c": "c:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{}} // a does not hold it -> b takes over
	s := NewScrubber(place, cat, probe, 3, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if !probe.pushed("a:7000", "c1") || !probe.pushed("c:7000", "c1") {
		t.Errorf("b should re-replicate to both a and c; stored=%d", probe.pushCount())
	}
}

func TestScrubber_SkipsChunkNotReplicatedHere(t *testing.T) {
	place := stubPlace{
		self:     "z",
		replicas: []string{"a", "b", "c"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000", "c": "c:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{}}
	s := NewScrubber(place, cat, probe, 3, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if probe.pushCount() != 0 {
		t.Errorf("a chunk this node is not a replica of must be skipped, made %d pushes", probe.pushCount())
	}
}

func TestScrubber_ListErrorIsLoggedNotFatal(t *testing.T) {
	cat := &fakeCatalog{listErr: errors.New("disk gone")}
	probe := &fakeProbe{}
	s := NewScrubber(stubPlace{self: "a"}, cat, probe, 1, time.Hour, nopLogger())
	s.runOnce(context.Background()) // must not panic
	if probe.pushCount() != 0 {
		t.Error("a list failure must do no work")
	}
}

func TestScrubber_GetErrorAbortsPush(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "b"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, getErr: map[string]error{"c1": errors.New("read failed")}}
	probe := &fakeProbe{present: map[string]bool{}}
	s := NewScrubber(place, cat, probe, 2, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if probe.pushCount() != 0 {
		t.Error("a failed local read must abort the push, not send empty data")
	}
}

func TestScrubber_SkipsTargetWithoutDataAddress(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "d"},
		addrs:    map[string]string{"a": "a:7000"}, // d has no advertised address
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{}}
	s := NewScrubber(place, cat, probe, 2, time.Hour, nopLogger())

	s.runOnce(context.Background())

	if probe.pushCount() != 0 {
		t.Error("a target without a data address must be skipped")
	}
}

func TestScrubber_StoreErrorIsLoggedNotFatal(t *testing.T) {
	place := stubPlace{
		self:     "a",
		replicas: []string{"a", "b"},
		addrs:    map[string]string{"a": "a:7000", "b": "b:7000"},
	}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{}, storeErr: map[string]error{"b:7000": errors.New("peer down")}}
	s := NewScrubber(place, cat, probe, 2, time.Hour, nopLogger())

	s.runOnce(context.Background()) // must not panic
	if probe.pushed("b:7000", "c1") {
		t.Error("a failed Store must not be recorded as pushed")
	}
}

func TestScrubber_StopsMidCycleOnCancel(t *testing.T) {
	place := stubPlace{self: "a", replicas: []string{"a", "b"}, addrs: map[string]string{"a": "a:7000", "b": "b:7000"}}
	cat := &fakeCatalog{ids: []string{"c1"}, data: map[string][]byte{"c1": []byte("payload")}}
	probe := &fakeProbe{present: map[string]bool{}}
	s := NewScrubber(place, cat, probe, 2, time.Hour, nopLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.runOnce(ctx)

	if probe.pushCount() != 0 {
		t.Error("a cancelled cycle must stop before doing work")
	}
}

func TestScrubber_StartTicksAndShutsDown(t *testing.T) {
	place := stubPlace{self: "a", replicas: []string{"a"}, addrs: map[string]string{"a": "a:7000"}}
	cat := &fakeCatalog{ids: nil, listed: make(chan struct{})}
	s := NewScrubber(place, cat, &fakeProbe{}, 1, time.Millisecond, nopLogger())

	go func() { _ = s.Start() }()

	select {
	case <-cat.listed:
	case <-time.After(2 * time.Second):
		t.Fatal("scrubber never ran a cycle")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func TestScrubber_ShutdownDeadlineExpires(t *testing.T) {
	s := NewScrubber(stubPlace{self: "a"}, &fakeCatalog{}, &fakeProbe{}, 1, time.Hour, nopLogger())
	// Never started, so done never closes; an already-expired context must
	// surface the deadline error rather than block forever.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := s.Shutdown(ctx); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want a deadline-exceeded error", err)
	}
}
