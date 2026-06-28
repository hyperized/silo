package replication

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func at(w int64) hlc.Timestamp { return hlc.Timestamp{Wall: w} }

// --- fakes ------------------------------------------------------------------

type fakeMetaPlace struct {
	replicas []string
	self     string
	noAddr   map[string]bool // node ids with no advertised data address
	lastN    int
}

func (f *fakeMetaPlace) MetaReplicas(_ string, n int) []string {
	f.lastN = n
	if n > len(f.replicas) {
		n = len(f.replicas)
	}
	return append([]string(nil), f.replicas[:n]...)
}

func (f *fakeMetaPlace) DataAddr(id string) (string, bool) {
	if f.noAddr[id] {
		return "", false
	}
	return id + ":7000", true
}

func (f *fakeMetaPlace) SelfID() string { return f.self }

type fakeStore struct {
	mu          sync.Mutex
	data        map[string]map[uint64]string
	has         map[string]bool
	setBatchErr error
	ensured     []string
	merges      map[string]int
}

func newFakeStore() *fakeStore {
	return &fakeStore{data: map[string]map[uint64]string{}, has: map[string]bool{}, merges: map[string]int{}}
}

func (s *fakeStore) ensureMap(vol string) map[uint64]string {
	if s.data[vol] == nil {
		s.data[vol] = map[uint64]string{}
	}
	s.has[vol] = true
	return s.data[vol]
}

func (s *fakeStore) SetBatch(vol string, idx []uint64, ids []string, _ hlc.Timestamp) error {
	if s.setBatchErr != nil {
		return s.setBatchErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.ensureMap(vol)
	for i := range idx {
		m[idx[i]] = ids[i]
	}
	return nil
}

func (s *fakeStore) Get(vol string, index uint64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[vol][index]
	return v, ok
}

func (s *fakeStore) Has(vol string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.has[vol]
}

func (s *fakeStore) Merge(vol string, entries []crdt.MapEntry[uint64, string]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.ensureMap(vol)
	for _, e := range entries {
		m[e.Key] = e.Value
	}
	s.merges[vol]++
}

func (s *fakeStore) Ensure(vol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureMap(vol)
	s.ensured = append(s.ensured, vol)
}

type appliedRec struct {
	addr, vol string
	entries   []crdt.MapEntry[uint64, string]
	ensure    bool
}

type fakeEpeers struct {
	mu       sync.Mutex
	applyErr map[string]error
	statHas  map[string]bool
	statErr  map[string]error
	fetchRes map[string][]crdt.MapEntry[uint64, string]
	fetchErr map[string]error
	applied  []appliedRec
	applyCh  chan struct{} // one signal per Apply call so tests can await fan-out
}

func newFakeEpeers() *fakeEpeers {
	return &fakeEpeers{applyErr: map[string]error{}, statHas: map[string]bool{}, statErr: map[string]error{}, fetchRes: map[string][]crdt.MapEntry[uint64, string]{}, fetchErr: map[string]error{}, applyCh: make(chan struct{}, 64)}
}

func (p *fakeEpeers) Apply(_ context.Context, addr, vol string, entries []crdt.MapEntry[uint64, string], ensure bool) error {
	p.mu.Lock()
	p.applied = append(p.applied, appliedRec{addr: addr, vol: vol, entries: entries, ensure: ensure})
	err := p.applyErr[addr]
	p.mu.Unlock()
	p.applyCh <- struct{}{}
	return err
}

// waitApplies blocks until n Apply calls (including background stragglers) have
// recorded, so a test reads p.applied without racing the fan-out goroutines.
func (p *fakeEpeers) waitApplies(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-p.applyCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for apply %d/%d", i+1, n)
		}
	}
}

func (p *fakeEpeers) snapApplied() []appliedRec {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]appliedRec(nil), p.applied...)
}

func (p *fakeEpeers) Fetch(_ context.Context, addr, _ string) ([]crdt.MapEntry[uint64, string], error) {
	if err := p.fetchErr[addr]; err != nil {
		return nil, err
	}
	return p.fetchRes[addr], nil
}

func (p *fakeEpeers) Stat(_ context.Context, addr, _ string) (bool, int64, error) {
	if err := p.statErr[addr]; err != nil {
		return false, 0, err
	}
	return p.statHas[addr], int64(len(p.fetchRes[addr])), nil
}

// --- tests ------------------------------------------------------------------

func TestExtentCoordinator_NewClampsRF(t *testing.T) {
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	c := NewExtentCoordinator(place, newFakeStore(), newFakeEpeers(), 0, quietLog())
	_ = c.ApplyDelta(context.Background(), "vol", []uint64{0}, []string{"c0"}, at(1))
	if place.lastN != 1 {
		t.Errorf("rf 0 should clamp to 1 (MetaReplicas n), got n=%d", place.lastN)
	}
}

func TestExtentCoordinator_ApplyDeltaValidation(t *testing.T) {
	c := NewExtentCoordinator(&fakeMetaPlace{replicas: []string{"a"}, self: "a"}, newFakeStore(), newFakeEpeers(), 3, quietLog())
	if err := c.ApplyDelta(context.Background(), "v", []uint64{0, 1}, []string{"c0"}, at(1)); err == nil {
		t.Error("mismatched slices should error")
	}
	if err := c.ApplyDelta(context.Background(), "v", nil, nil, at(1)); err != nil {
		t.Errorf("empty batch should be a no-op, got %v", err)
	}
}

func TestExtentCoordinator_ApplyDeltaQuorumWithSelfReplica(t *testing.T) {
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	store := newFakeStore()
	peers := newFakeEpeers() // both peers ack
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())

	if err := c.ApplyDelta(context.Background(), "vol", []uint64{0, 5}, []string{"c0", "c5"}, at(10)); err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	// Local working copy updated (self is a replica).
	if v, ok := store.Get("vol", 5); !ok || v != "c5" {
		t.Errorf("local extent 5 = (%q,%v), want c5", v, ok)
	}
	// Both peers received the delta as a replica apply (one synchronously to
	// reach quorum, one drained in the background).
	peers.waitApplies(t, 2)
	if got := peers.snapApplied(); len(got) != 2 {
		t.Errorf("want 2 peer applies, got %d", len(got))
	}
}

func TestExtentCoordinator_ApplyDeltaSelfNotReplica(t *testing.T) {
	place := &fakeMetaPlace{replicas: []string{"b", "c", "d"}, self: "a"} // self not in set
	store := newFakeStore()
	peers := newFakeEpeers()
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.ApplyDelta(context.Background(), "vol", []uint64{0}, []string{"c0"}, at(1)); err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	// Local cache still updated for read-after-write.
	if _, ok := store.Get("vol", 0); !ok {
		t.Error("local cache should be updated even when self is not a replica")
	}
}

func TestExtentCoordinator_ApplyDeltaNoReplicas(t *testing.T) {
	c := NewExtentCoordinator(&fakeMetaPlace{replicas: nil, self: "a"}, newFakeStore(), newFakeEpeers(), 3, quietLog())
	if err := c.ApplyDelta(context.Background(), "vol", []uint64{0}, []string{"c0"}, at(1)); err == nil {
		t.Error("no replicas should error")
	}
}

func TestExtentCoordinator_ApplyDeltaSetBatchError(t *testing.T) {
	store := newFakeStore()
	store.setBatchErr = errors.New("disk full")
	c := NewExtentCoordinator(&fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}, store, newFakeEpeers(), 3, quietLog())
	if err := c.ApplyDelta(context.Background(), "vol", []uint64{0}, []string{"c0"}, at(1)); err == nil {
		t.Error("a local SetBatch failure should propagate")
	}
}

func TestExtentCoordinator_ApplyDeltaQuorumNotReached(t *testing.T) {
	place := &fakeMetaPlace{replicas: []string{"b", "c", "d"}, self: "a"} // self not a replica: need 2 peer acks
	peers := newFakeEpeers()
	peers.applyErr["b:7000"] = errors.New("down")
	peers.applyErr["c:7000"] = errors.New("down")
	// d acks, but 1 < quorum 2.
	c := NewExtentCoordinator(place, newFakeStore(), peers, 3, quietLog())
	if err := c.ApplyDelta(context.Background(), "vol", []uint64{0}, []string{"c0"}, at(1)); err == nil {
		t.Error("a sub-quorum write should error")
	}
}

func TestExtentCoordinator_ApplyDeltaMissingDataAddr(t *testing.T) {
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a", noAddr: map[string]bool{"b": true}}
	// self acks (1) + c acks (1) = 2 == quorum; b has no addr (failure, drained).
	c := NewExtentCoordinator(place, newFakeStore(), newFakeEpeers(), 3, quietLog())
	if err := c.ApplyDelta(context.Background(), "vol", []uint64{0}, []string{"c0"}, at(1)); err != nil {
		t.Errorf("quorum should still be reached despite one missing addr: %v", err)
	}
}

func TestExtentCoordinator_EnsureMap(t *testing.T) {
	// self is a replica: local Ensure + peer ensures.
	store := newFakeStore()
	peers := newFakeEpeers()
	c := NewExtentCoordinator(&fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}, store, peers, 3, quietLog())
	if err := c.EnsureMap(context.Background(), "vol"); err != nil {
		t.Fatalf("EnsureMap: %v", err)
	}
	if len(store.ensured) != 1 || store.ensured[0] != "vol" {
		t.Errorf("local Ensure not called, got %v", store.ensured)
	}
	peers.waitApplies(t, 2)
	for _, rec := range peers.snapApplied() {
		if !rec.ensure {
			t.Error("peer apply should carry ensure=true")
		}
	}

	// self not a replica: only peer ensures.
	store2 := newFakeStore()
	peers2 := newFakeEpeers()
	c2 := NewExtentCoordinator(&fakeMetaPlace{replicas: []string{"b", "c", "d"}, self: "a"}, store2, peers2, 3, quietLog())
	if err := c2.EnsureMap(context.Background(), "vol"); err != nil {
		t.Fatalf("EnsureMap: %v", err)
	}
	peers2.waitApplies(t, 3)
	if len(store2.ensured) != 0 {
		t.Error("local Ensure must not be called when self is not a replica")
	}

	// no replicas → error.
	c3 := NewExtentCoordinator(&fakeMetaPlace{replicas: nil, self: "a"}, newFakeStore(), newFakeEpeers(), 3, quietLog())
	if err := c3.EnsureMap(context.Background(), "vol"); err == nil {
		t.Error("EnsureMap with no replicas should error")
	}
}

func TestExtentCoordinator_WarmAlreadyLocal(t *testing.T) {
	store := newFakeStore()
	store.Ensure("vol") // local already has it
	peers := newFakeEpeers()
	c := NewExtentCoordinator(&fakeMetaPlace{replicas: []string{"a", "b"}, self: "a"}, store, peers, 3, quietLog())
	if err := c.Warm(context.Background(), "vol"); err != nil {
		t.Fatalf("Warm: %v", err)
	}
	if got := peers.snapApplied(); len(got) != 0 {
		t.Error("Warm should not touch peers when the map is already local")
	}
}

func TestExtentCoordinator_WarmFetchesFromHolder(t *testing.T) {
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	store := newFakeStore()
	peers := newFakeEpeers()
	peers.statHas["b:7000"] = false // b lacks it
	peers.statHas["c:7000"] = true  // c has it
	peers.fetchRes["c:7000"] = []crdt.MapEntry[uint64, string]{{Key: 0, Value: "c0", TS: at(1)}}
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.Warm(context.Background(), "vol"); err != nil {
		t.Fatalf("Warm: %v", err)
	}
	if v, ok := store.Get("vol", 0); !ok || v != "c0" {
		t.Errorf("warmed map missing extent: (%q,%v)", v, ok)
	}
}

func TestExtentCoordinator_WarmErrors(t *testing.T) {
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c", "d"}, self: "a", noAddr: map[string]bool{"b": true}}
	peers := newFakeEpeers()
	peers.statErr["c:7000"] = errors.New("stat boom") // c stat errors
	peers.statHas["d:7000"] = true
	peers.fetchErr["d:7000"] = errors.New("fetch boom") // d fetch errors
	c := NewExtentCoordinator(place, newFakeStore(), peers, 4, quietLog())
	if err := c.Warm(context.Background(), "vol"); err == nil {
		t.Error("Warm should error when no replica yields the map")
	}

	// no replicas → error.
	c2 := NewExtentCoordinator(&fakeMetaPlace{replicas: nil, self: "a"}, newFakeStore(), newFakeEpeers(), 3, quietLog())
	if err := c2.Warm(context.Background(), "vol"); err == nil {
		t.Error("Warm with no replicas should error")
	}
}

func TestExtentCoordinator_WarmSkipsNonHolders(t *testing.T) {
	// Only peers that report has=true are fetched; a not-has peer is skipped
	// and the next holder is used.
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	peers := newFakeEpeers()
	peers.statHas["b:7000"] = false
	peers.statHas["c:7000"] = true
	peers.fetchRes["c:7000"] = nil // empty map (an Ensure'd volume)
	store := newFakeStore()
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.Warm(context.Background(), "vol"); err != nil {
		t.Fatalf("Warm: %v", err)
	}
	if !store.Has("vol") {
		t.Error("warming an empty map should still create the local map")
	}
}

func TestExtentCoordinator_Lookup(t *testing.T) {
	store := newFakeStore()
	_ = store.SetBatch("vol", []uint64{2}, []string{"c2"}, at(1))
	c := NewExtentCoordinator(&fakeMetaPlace{replicas: []string{"a"}, self: "a"}, store, newFakeEpeers(), 3, quietLog())
	if v, ok := c.Lookup("vol", 2); !ok || v != "c2" {
		t.Errorf("Lookup(2) = (%q,%v), want c2", v, ok)
	}
	if _, ok := c.Lookup("vol", 9); ok {
		t.Error("Lookup of an unmapped extent should be false")
	}
}

func TestExtentCoordinator_DrainLogsShortfall(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	c := NewExtentCoordinator(&fakeMetaPlace{self: "a"}, newFakeStore(), newFakeEpeers(), 3, logger)
	results := make(chan error, 2)
	results <- nil
	results <- errors.New("straggler failed")
	c.drain(results, 2, "vol") // consumes both: one success (no log), one failure (logged)
	if !strings.Contains(buf.String(), "fell short of full replication") {
		t.Errorf("a straggler failure should be logged, got: %q", buf.String())
	}
}
