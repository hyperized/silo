package replication

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
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
	deleteErr   error
	ensured     []string
	deleted     []string
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

func (s *fakeStore) Snapshot(vol string) []crdt.MapEntry[uint64, string] {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.data[vol]
	if m == nil {
		return nil
	}
	entries := make([]crdt.MapEntry[uint64, string], 0, len(m))
	for k, v := range m {
		entries = append(entries, crdt.MapEntry[uint64, string]{Key: k, Value: v})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries
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

func (s *fakeStore) Delete(vol string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, vol)
	delete(s.has, vol)
	s.deleted = append(s.deleted, vol)
	return nil
}

type appliedRec struct {
	addr, vol string
	entries   []crdt.MapEntry[uint64, string]
	ensure    bool
}

type fakeEpeers struct {
	mu        sync.Mutex
	applyErr  map[string]error
	statHas   map[string]bool
	statErr   map[string]error
	fetchRes  map[string][]crdt.MapEntry[uint64, string]
	fetchErr  map[string]error
	deleteErr map[string]error
	applied   []appliedRec
	deleted   []appliedRec
	applyCh   chan struct{} // one signal per Apply call so tests can await fan-out
}

func newFakeEpeers() *fakeEpeers {
	return &fakeEpeers{applyErr: map[string]error{}, statHas: map[string]bool{}, statErr: map[string]error{}, fetchRes: map[string][]crdt.MapEntry[uint64, string]{}, fetchErr: map[string]error{}, deleteErr: map[string]error{}, applyCh: make(chan struct{}, 64)}
}

func (p *fakeEpeers) Delete(_ context.Context, addr, vol string) error {
	p.mu.Lock()
	p.deleted = append(p.deleted, appliedRec{addr: addr, vol: vol})
	err := p.deleteErr[addr]
	p.mu.Unlock()
	return err
}

func (p *fakeEpeers) snapDeleted() []appliedRec {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]appliedRec(nil), p.deleted...)
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

func TestExtentCoordinator_WarmEmptyVolumeIsZeros(t *testing.T) {
	// All replicas reachable and none holds the map: a never-written volume.
	// Warm establishes an empty local map (reads as zeros) rather than erroring.
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	peers := newFakeEpeers() // statHas defaults false for b, c; no stat errors
	store := newFakeStore()
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.Warm(context.Background(), "vol"); err != nil {
		t.Fatalf("Warm of a never-written volume should succeed: %v", err)
	}
	if !store.Has("vol") {
		t.Error("Warm should establish an empty local map for a never-written volume")
	}
	if _, ok := store.Get("vol", 0); ok {
		t.Error("a never-written extent should read unmapped (zeros)")
	}
}

func TestExtentCoordinator_SnapshotMapClonesLocalSource(t *testing.T) {
	// Source map is already local and self is a dst replica: the source bindings
	// are cloned into the dst map locally and fanned out to peers with ensure=true.
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	store := newFakeStore()
	if err := store.SetBatch("src", []uint64{0, 1}, []string{"c0", "c1"}, at(1)); err != nil {
		t.Fatalf("seed src: %v", err)
	}
	peers := newFakeEpeers()
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.SnapshotMap(context.Background(), "src", "dst"); err != nil {
		t.Fatalf("SnapshotMap: %v", err)
	}
	if v, ok := store.Get("dst", 0); !ok || v != "c0" {
		t.Errorf("dst extent 0 = (%q,%v), want c0", v, ok)
	}
	if v, ok := store.Get("dst", 1); !ok || v != "c1" {
		t.Errorf("dst extent 1 = (%q,%v), want c1", v, ok)
	}
	peers.waitApplies(t, 2)
	for _, rec := range peers.snapApplied() {
		if rec.vol != "dst" || !rec.ensure || len(rec.entries) != 2 {
			t.Errorf("peer apply = %+v, want vol=dst ensure=true 2 entries", rec)
		}
	}
}

func TestExtentCoordinator_SnapshotMapWarmsRemoteSource(t *testing.T) {
	// Source map is not local: it is warmed from a holder before being cloned.
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	store := newFakeStore()
	peers := newFakeEpeers()
	peers.statHas["b:7000"] = true
	peers.fetchRes["b:7000"] = []crdt.MapEntry[uint64, string]{{Key: 7, Value: "c7", TS: at(1)}}
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.SnapshotMap(context.Background(), "src", "dst"); err != nil {
		t.Fatalf("SnapshotMap: %v", err)
	}
	if v, ok := store.Get("dst", 7); !ok || v != "c7" {
		t.Errorf("dst extent 7 = (%q,%v), want c7 (warmed then cloned)", v, ok)
	}
}

func TestExtentCoordinator_SnapshotMapEmptySourceEstablishesEmptyDst(t *testing.T) {
	// A never-written source (all replicas reachable, none holds a map) snapshots
	// to a valid empty dst: Warm establishes an empty src, the clone establishes
	// an empty dst, and peers receive an ensure=true apply with no entries.
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	store := newFakeStore()
	peers := newFakeEpeers() // statHas defaults false; no stat errors → never-written
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.SnapshotMap(context.Background(), "src", "dst"); err != nil {
		t.Fatalf("SnapshotMap: %v", err)
	}
	if !store.Has("dst") {
		t.Error("dst map should be established even when the source was empty")
	}
	peers.waitApplies(t, 2)
	for _, rec := range peers.snapApplied() {
		if rec.vol != "dst" || !rec.ensure || len(rec.entries) != 0 {
			t.Errorf("peer apply = %+v, want vol=dst ensure=true 0 entries", rec)
		}
	}
}

func TestExtentCoordinator_SnapshotMapSelfNotDstReplica(t *testing.T) {
	// Self is not a dst replica: no local dst map is written; the clone reaches
	// the dst replica set purely by peer fan-out. The source is pre-seeded local
	// so Warm is a no-op.
	place := &fakeMetaPlace{replicas: []string{"b", "c", "d"}, self: "a"}
	store := newFakeStore()
	if err := store.SetBatch("src", []uint64{0}, []string{"c0"}, at(1)); err != nil {
		t.Fatalf("seed src: %v", err)
	}
	peers := newFakeEpeers()
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.SnapshotMap(context.Background(), "src", "dst"); err != nil {
		t.Fatalf("SnapshotMap: %v", err)
	}
	if store.Has("dst") {
		t.Error("local dst map must not be written when self is not a dst replica")
	}
	peers.waitApplies(t, 3)
}

func TestExtentCoordinator_SnapshotMapWarmSourceError(t *testing.T) {
	// The source cannot be warmed (a replica unreachable): SnapshotMap fails
	// rather than cloning a partial or empty map.
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	peers := newFakeEpeers()
	peers.statErr["b:7000"] = errors.New("stat boom")
	peers.statErr["c:7000"] = errors.New("stat boom")
	c := NewExtentCoordinator(place, newFakeStore(), peers, 3, quietLog())
	if err := c.SnapshotMap(context.Background(), "src", "dst"); err == nil {
		t.Error("SnapshotMap should fail when the source map cannot be warmed")
	}
}

func TestExtentCoordinator_SnapshotMapNoDstReplicas(t *testing.T) {
	// Source warms trivially (already local), but the dst has no replicas: the
	// clone errors instead of silently dropping the snapshot's map.
	store := newFakeStore()
	store.Ensure("src") // local already holds src → Warm short-circuits
	c := NewExtentCoordinator(&fakeMetaPlace{replicas: nil, self: "a"}, store, newFakeEpeers(), 3, quietLog())
	if err := c.SnapshotMap(context.Background(), "src", "dst"); err == nil {
		t.Error("SnapshotMap with no dst replicas should error")
	}
}

func TestExtentCoordinator_SnapshotMapQuorumNotReached(t *testing.T) {
	// Self is not a dst replica and the peers fail: the clone cannot reach quorum
	// and errors, so the caller can roll the snapshot back.
	place := &fakeMetaPlace{replicas: []string{"b", "c", "d"}, self: "a"}
	store := newFakeStore()
	if err := store.SetBatch("src", []uint64{0}, []string{"c0"}, at(1)); err != nil {
		t.Fatalf("seed src: %v", err)
	}
	peers := newFakeEpeers()
	peers.applyErr["b:7000"] = errors.New("down")
	peers.applyErr["c:7000"] = errors.New("down")
	peers.applyErr["d:7000"] = errors.New("down")
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.SnapshotMap(context.Background(), "src", "dst"); err == nil {
		t.Error("a sub-quorum clone should error")
	}
}

func TestExtentCoordinator_DeleteMap(t *testing.T) {
	// self is a replica; both peers ack: local map gone, both peers told to delete.
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	store := newFakeStore()
	store.has["vol"] = true
	peers := newFakeEpeers()
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.DeleteMap(context.Background(), "vol"); err != nil {
		t.Fatalf("DeleteMap: %v", err)
	}
	if store.Has("vol") {
		t.Error("the local map should be deleted")
	}
	if del := peers.snapDeleted(); len(del) != 2 {
		t.Errorf("want 2 peer deletes, got %d (%v)", len(del), del)
	}
}

func TestExtentCoordinator_DeleteMapCollectsErrors(t *testing.T) {
	// Local delete fails, one replica has no advertised address, one peer errors:
	// all three are surfaced, joined, and none aborts the others.
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a", noAddr: map[string]bool{"b": true}}
	store := newFakeStore()
	store.deleteErr = errors.New("read-only fs")
	peers := newFakeEpeers()
	peers.deleteErr["c:7000"] = errors.New("peer down")
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())

	err := c.DeleteMap(context.Background(), "vol")
	if err == nil {
		t.Fatal("DeleteMap should report the joined failures")
	}
	for _, want := range []string{"read-only fs", "has not advertised a data address", "peer down"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("joined error %q is missing %q", err, want)
		}
	}
}

func TestExtentCoordinator_DeleteMapNoPeers(t *testing.T) {
	// Self is the only replica: a local delete, no fan-out, no error.
	place := &fakeMetaPlace{replicas: []string{"a"}, self: "a"}
	store := newFakeStore()
	store.has["vol"] = true
	peers := newFakeEpeers()
	c := NewExtentCoordinator(place, store, peers, 1, quietLog())
	if err := c.DeleteMap(context.Background(), "vol"); err != nil {
		t.Fatalf("DeleteMap: %v", err)
	}
	if store.Has("vol") {
		t.Error("the local map should be deleted")
	}
	if del := peers.snapDeleted(); len(del) != 0 {
		t.Errorf("no peer deletes expected when self is the only replica, got %v", del)
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
