package backup_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

const testNode = "node-a"

var (
	t0     = time.Date(2026, 10, 28, 12, 0, 0, 123_000_000, time.UTC)
	stamp0 = "2026-10-28T12:00:00.123Z"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeClock returns a time that moves forward by step on every call, plus
// whatever Advance added.
type fakeClock struct {
	mu   sync.Mutex
	t    time.Time
	step time.Duration
}

func newClock(start time.Time, step time.Duration) *fakeClock {
	return &fakeClock{t: start, step: step}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := c.t
	c.t = c.t.Add(c.step)
	return v
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type memObject struct {
	data []byte
	mod  time.Time
}

// memTarget is an in-memory blobstore.Target. Error maps are keyed by an exact
// object name (or list prefix); a key ending in "*" matches every name with
// that prefix.
type memTarget struct {
	mu        sync.Mutex
	objs      map[string]memObject
	clock     func() time.Time
	putErr    map[string]error
	getErr    map[string]error
	listErr   map[string]error
	deleteErr map[string]error
	// putHook runs before every Put and PutIfAbsent; an error fails the call.
	putHook func(ctx context.Context, name string) error
	// piaHook runs before every PutIfAbsent with the 1-based call number for
	// that name. It may call set to simulate another node writing first.
	piaHook func(m *memTarget, name string, call int) error
	// getHook runs before every Get, outside the lock.
	getHook func(name string)
	// listHook runs before every List with the 1-based call number for that
	// prefix, outside the lock.
	listHook func(m *memTarget, prefix string, call int)
	hidden   map[string]bool
	piaCalls map[string]int
	listCnt  map[string]int
	puts     []string
	gets     []string
	deletes  []string
}

func newMemTarget() *memTarget {
	return &memTarget{
		objs:      map[string]memObject{},
		clock:     func() time.Time { return t0 },
		putErr:    map[string]error{},
		getErr:    map[string]error{},
		listErr:   map[string]error{},
		deleteErr: map[string]error{},
		hidden:    map[string]bool{},
		piaCalls:  map[string]int{},
		listCnt:   map[string]int{},
	}
}

func errFor(m map[string]error, name string) error {
	if err, ok := m[name]; ok {
		return err
	}
	for k, err := range m {
		if p, ok := strings.CutSuffix(k, "*"); ok && strings.HasPrefix(name, p) {
			return err
		}
	}
	return nil
}

func (m *memTarget) failPut(key string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putErr[key] = err
}

func (m *memTarget) failGet(key string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getErr[key] = err
}

func (m *memTarget) failList(key string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listErr[key] = err
}

func (m *memTarget) failDelete(key string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteErr[key] = err
}

func (m *memTarget) setPutHook(h func(ctx context.Context, name string) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putHook = h
}

func (m *memTarget) setPutIfAbsentHook(h func(m *memTarget, name string, call int) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.piaHook = h
}

func (m *memTarget) setGetHook(h func(name string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getHook = h
}

func (m *memTarget) setListHook(h func(m *memTarget, prefix string, call int)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listHook = h
}

// hide keeps an object out of List results while it stays readable, like a
// listing that lags behind writes.
func (m *memTarget) hide(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hidden[name] = true
}

func (m *memTarget) Name() string { return "mem://test" }

func (m *memTarget) Put(ctx context.Context, name string, data []byte) error {
	if err := blobstore.ValidateName(name); err != nil {
		return err
	}
	m.mu.Lock()
	hook := m.putHook
	m.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, name); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := errFor(m.putErr, name); err != nil {
		return err
	}
	m.puts = append(m.puts, name)
	m.objs[name] = memObject{data: bytes.Clone(data), mod: m.clock()}
	return nil
}

// PutIfAbsent is write-once: an existing object, even an empty one, is left
// alone and created is false.
func (m *memTarget) PutIfAbsent(ctx context.Context, name string, data []byte) (bool, error) {
	if err := blobstore.ValidateName(name); err != nil {
		return false, err
	}
	m.mu.Lock()
	hook, pia := m.putHook, m.piaHook
	m.piaCalls[name]++
	call := m.piaCalls[name]
	m.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, name); err != nil {
			return false, err
		}
	}
	if pia != nil {
		if err := pia(m, name, call); err != nil {
			return false, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := errFor(m.putErr, name); err != nil {
		return false, err
	}
	if _, ok := m.objs[name]; ok {
		return false, nil
	}
	m.puts = append(m.puts, name)
	m.objs[name] = memObject{data: bytes.Clone(data), mod: m.clock()}
	return true, nil
}

func (m *memTarget) Get(_ context.Context, name string, maxBytes int64) ([]byte, error) {
	if err := blobstore.ValidateName(name); err != nil {
		return nil, err
	}
	m.mu.Lock()
	hook := m.getHook
	m.mu.Unlock()
	if hook != nil {
		hook(name)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets = append(m.gets, name)
	if err := errFor(m.getErr, name); err != nil {
		return nil, err
	}
	o, ok := m.objs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", blobstore.ErrNotFound, name)
	}
	if int64(len(o.data)) > maxBytes {
		return nil, fmt.Errorf("%w: %s", blobstore.ErrTooLarge, name)
	}
	return bytes.Clone(o.data), nil
}

func (m *memTarget) List(_ context.Context, prefix string) ([]blobstore.ObjectInfo, error) {
	if prefix != "" {
		if err := blobstore.ValidateName(strings.TrimSuffix(prefix, "/")); err != nil {
			return nil, err
		}
	}
	m.mu.Lock()
	hook := m.listHook
	m.listCnt[prefix]++
	call := m.listCnt[prefix]
	m.mu.Unlock()
	if hook != nil {
		hook(m, prefix, call)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := errFor(m.listErr, prefix); err != nil {
		return nil, err
	}
	var out []blobstore.ObjectInfo
	for n, o := range m.objs {
		if strings.HasPrefix(n, prefix) && !m.hidden[n] {
			out = append(out, blobstore.ObjectInfo{Name: n, Size: int64(len(o.data)), ModTime: o.mod})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *memTarget) Delete(_ context.Context, name string) error {
	if err := blobstore.ValidateName(name); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := errFor(m.deleteErr, name); err != nil {
		return err
	}
	m.deletes = append(m.deletes, name)
	delete(m.objs, name)
	return nil
}

// set stores an object directly, bypassing name validation and the hooks, so
// tests can plant foreign or damaged objects.
func (m *memTarget) set(name string, data []byte, mod time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[name] = memObject{data: bytes.Clone(data), mod: mod}
}

func (m *memTarget) remove(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, name)
}

func (m *memTarget) has(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objs[name]
	return ok
}

func (m *memTarget) bytesOf(name string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return bytes.Clone(m.objs[name].data)
}

func (m *memTarget) names(prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for n := range m.objs {
		if strings.HasPrefix(n, prefix) {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

func (m *memTarget) putOrder() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.puts...)
}

func (m *memTarget) resetPuts() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts = nil
}

func (m *memTarget) putCount(prefix string) int {
	n := 0
	for _, p := range m.putOrder() {
		if strings.HasPrefix(p, prefix) {
			n++
		}
	}
	return n
}

func (m *memTarget) getCount(prefix string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, g := range m.gets {
		if strings.HasPrefix(g, prefix) {
			n++
		}
	}
	return n
}

func (m *memTarget) resetGets() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets = nil
}

func (m *memTarget) deletedWith(prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, d := range m.deletes {
		if strings.HasPrefix(d, prefix) {
			out = append(out, d)
		}
	}
	return out
}

// fakeChunks is a ChunkSource over an in-memory map.
type fakeChunks struct {
	mu      sync.Mutex
	data    map[string][]byte
	ids     []string // overrides the listing when non-nil
	listErr error
	rawErr  map[string]error
}

func newChunks(kv map[string]string) *fakeChunks {
	f := &fakeChunks{data: map[string][]byte{}, rawErr: map[string]error{}}
	for k, v := range kv {
		f.data[k] = []byte(v)
	}
	return f
}

func (f *fakeChunks) List(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	if f.ids != nil {
		return append([]string(nil), f.ids...), nil
	}
	out := make([]string, 0, len(f.data))
	for id := range f.data {
		out = append(out, id)
	}
	return out, nil
}

func (f *fakeChunks) RawChunk(_ context.Context, id string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.rawErr[id]; err != nil {
		return nil, err
	}
	return f.data[id], nil
}

func (f *fakeChunks) drop(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, id)
}

type fakeNS struct {
	snap []byte
	err  error
}

func (f fakeNS) Snapshot() ([]byte, error) { return f.snap, f.err }

type fakeExtents struct {
	vols map[string][]crdt.MapEntry[uint64, string]
}

func (f fakeExtents) Volumes() []string {
	out := make([]string, 0, len(f.vols))
	for v := range f.vols {
		out = append(out, v)
	}
	return out
}

func (f fakeExtents) Snapshot(vol string) []crdt.MapEntry[uint64, string] { return f.vols[vol] }

func entry(key uint64, chunk string) crdt.MapEntry[uint64, string] {
	return crdt.MapEntry[uint64, string]{Key: key, Value: chunk, TS: hlc.Timestamp{Wall: int64(key) + 1, Node: testNode}}
}

func twoVolumes() fakeExtents {
	return fakeExtents{vols: map[string][]crdt.MapEntry[uint64, string]{
		"vol/1": {entry(0, "c1"), entry(1, "c2")},
		"vol-2": {entry(0, "c3")},
	}}
}

func threeChunks() *fakeChunks {
	return newChunks(map[string]string{"c1": "aaa", "c2": "bb", "c3": "c"})
}

// export runs one export and fails the test on error.
func export(t *testing.T, e *backup.Exporter, tgt blobstore.Target) backup.Result {
	t.Helper()
	res, err := e.Export(context.Background(), tgt)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	return res
}

func sha256Of(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// chunkRefFor describes the object putChunk stores for id.
func chunkRefFor(id string) backup.ChunkRef {
	data := []byte("data-" + id)
	return backup.ChunkRef{ID: id, Size: int64(len(data)), SHA256: sha256Of(data)}
}

// putManifest stores a valid manifest for node/stamp referencing chunks (as
// putChunk would store them), with namespace and extent objects stored
// alongside, all with modification time mod. The chunk objects themselves are
// not stored.
func putManifest(t *testing.T, tgt *memTarget, node, stamp string, chunks []string, mod time.Time) *backup.Manifest {
	t.Helper()
	started, err := time.Parse("2006-01-02T15:04:05.000Z", stamp)
	if err != nil {
		t.Fatal(err)
	}
	refs := make([]backup.ChunkRef, len(chunks))
	var chunkBytes int64
	for i, id := range chunks {
		refs[i] = chunkRefFor(id)
		chunkBytes += refs[i].Size
	}
	empty := backup.ObjectRef{Size: 2, SHA256: sha256Of([]byte("{}"))}
	ns, ext := empty, empty
	ns.Key = "namespace/" + node + "/" + stamp + ".json"
	ext.Key = "extents/" + node + "/" + stamp + "/dm9s.json"
	m := &backup.Manifest{
		Format:      backup.ManifestFormat,
		SiloVersion: "test",
		NodeID:      node,
		StartedAt:   started,
		FinishedAt:  started.Add(time.Second),
		Namespace:   ns,
		Extents:     []backup.ObjectRef{ext},
		Chunks:      refs,
		ChunkCount:  len(chunks),
		ChunkBytes:  chunkBytes,
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	tgt.set(backup.ManifestKey(node, stamp), raw, mod)
	tgt.set(ns.Key, []byte("{}"), mod)
	tgt.set(ext.Key, []byte("{}"), mod)
	return m
}

func putChunk(tgt *memTarget, id string, mod time.Time) {
	tgt.set("chunks/"+id, []byte("data-"+id), mod)
}

func decodeManifest(t *testing.T, raw []byte) backup.Manifest {
	t.Helper()
	var m backup.Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	return m
}
