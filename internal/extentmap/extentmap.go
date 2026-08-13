// Package extentmap is a node's local replica of the volume extent maps it
// holds: the per-volume offset-index -> chunk-id bindings that back a block
// device, keyed by volume inode id. Each map is a last-writer-wins CRDT keyed
// by extent index, so the lease holder's writes and repair pushes from peers
// converge by HLC regardless of arrival order.
//
// Extent maps used to ride in the gossiped namespace snapshot, but a single
// large volume's map exceeds the gossip per-message cap and silently stranded a
// node's whole namespace. They now travel to a volume's replica set out of band
// (the way chunks do) and are persisted here; the namespace keeps only the
// small, gossiped directory tree plus each volume's size, extent size, and
// lease.
package extentmap

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

// fileSuffix tags this store's per-volume files in the data directory.
const fileSuffix = ".emap.json"

// marshal is the serialization seam; tests override it to exercise the
// persistence error path.
var marshal = json.Marshal

// Store holds the extent maps for the volumes whose replica set includes this
// node, keyed by volume inode id. It is safe for concurrent use. When opened
// with a directory it persists each volume's map to <dir>/<id>.emap.json
// atomically and reloads them on the next Open; an empty directory means
// in-memory only (the maps are still recoverable from peer replicas).
type Store struct {
	dir    string
	logger *slog.Logger

	mu   sync.Mutex
	maps map[string]*crdt.LWWMap[uint64, string]
}

// New returns an in-memory store. A nil logger defaults to slog.Default.
func New(logger *slog.Logger) *Store {
	if logger == nil {
		logger = slog.Default()
	}
	return &Store{logger: logger, maps: map[string]*crdt.LWWMap[uint64, string]{}}
}

// Open returns a store backed by dir: it creates dir if missing, loads every
// extent map previously persisted there, and persists local mutations going
// forward. A corrupt per-volume file is logged and skipped (the map re-converges
// from peers). A nil logger defaults to slog.Default.
func Open(dir string, logger *slog.Logger) (*Store, error) {
	s := New(logger)
	s.dir = dir
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("extentmap: could not create the data directory at %s (%w); check it is writable", dir, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

// wireMap is the on-disk JSON shape: the volume id (so the filename need not be
// decoded) plus the map's entries.
type wireMap struct {
	VolumeID string                          `json:"volume_id"`
	Entries  []crdt.MapEntry[uint64, string] `json:"entries"`
}

// Set binds the extent at index of volume to chunkID as of ts, creating the
// volume's map if absent. The LWW timestamp means a replayed or out-of-order
// delta never moves an extent backward.
func (s *Store) Set(volumeID string, index uint64, chunkID string, ts hlc.Timestamp) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getOrCreateLocked(volumeID).Set(index, chunkID, ts)
	s.persistLocked(volumeID)
}

// SetBatch binds many extents of volume in one acquisition + one persist. All
// updates share ts. indexes and chunkIDs are positionally paired; their lengths
// must match. An empty batch is a successful no-op (no persist).
func (s *Store) SetBatch(volumeID string, indexes []uint64, chunkIDs []string, ts hlc.Timestamp) error {
	if len(indexes) != len(chunkIDs) {
		return fmt.Errorf("extentmap: SetBatch needs paired slices, got %d indexes and %d chunk ids", len(indexes), len(chunkIDs))
	}
	if len(indexes) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.getOrCreateLocked(volumeID)
	for i, idx := range indexes {
		m.Set(idx, chunkIDs[i], ts)
	}
	s.persistLocked(volumeID)
	return nil
}

// Get returns the chunk id backing the extent at index of volume and whether it
// is mapped. An unknown volume or unmapped extent returns ("", false).
func (s *Store) Get(volumeID string, index uint64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.maps[volumeID]
	if !ok {
		return "", false
	}
	return m.Get(index)
}

// Has reports whether this node holds a map for volume (even an empty one).
func (s *Store) Has(volumeID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.maps[volumeID]
	return ok
}

// Len returns the number of mapped extents for volume, or 0 if unknown.
func (s *Store) Len(volumeID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.maps[volumeID]; ok {
		return m.Len()
	}
	return 0
}

// emptyMapSalt keeps an empty map's digest away from zero, so a peer that
// returns an all-zero digest by accident does not read as agreeing with one.
const emptyMapSalt = 0x5110000000000001

// Digest returns a fingerprint of volume's bindings, or nil if the volume is
// unknown. Two replicas holding the same map produce the same digest; any
// difference in the set of (index, chunk, timestamp) triples changes it.
//
// It exists so replicas can discover that their copies disagree without
// shipping the copies. A map is small next to the data it addresses but not
// small in absolute terms — a 40 GiB volume's map runs to six figures of
// entries — so comparing contents outright on every scrub cycle would cost far
// more than the divergence it is looking for.
//
// The fold is XOR over per-entry hashes, which makes the result independent of
// iteration order. That is the one property this needs, since two replicas have
// no reason to enumerate a map in the same order.
func (s *Store) Digest(volumeID string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.maps[volumeID]
	if !ok {
		return nil
	}
	var acc uint64
	var buf [8]byte
	for _, e := range m.Entries() {
		h := fnv.New64a()
		binary.BigEndian.PutUint64(buf[:], e.Key)
		_, _ = h.Write(buf[:])
		_, _ = h.Write([]byte(e.Value))
		// Reinterpreting the wall clock's bits is the point: the digest only has
		// to be stable and collision-resistant, not arithmetically meaningful.
		binary.BigEndian.PutUint64(buf[:], uint64(e.TS.Wall)) // #nosec G115
		_, _ = h.Write(buf[:])
		binary.BigEndian.PutUint64(buf[:], uint64(e.TS.Logical)) // #nosec G115
		_, _ = h.Write(buf[:])
		_, _ = h.Write([]byte(e.TS.Node))
		acc ^= h.Sum64()
	}
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, acc^emptyMapSalt)
	return out
}

// Snapshot returns volume's extent bindings, sorted by index for a stable wire
// form, or nil if the volume is unknown. It is the payload the serve/repair
// path ships to a peer that needs the whole map.
func (s *Store) Snapshot(volumeID string) []crdt.MapEntry[uint64, string] {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.maps[volumeID]
	if !ok {
		return nil
	}
	entries := m.Entries()
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries
}

// Merge folds a peer's entries into volume's map (creating it if absent) with
// LWW-by-HLC semantics, then persists. Used to warm a serving node's map from a
// replica and to repair a lagging replica.
func (s *Store) Merge(volumeID string, entries []crdt.MapEntry[uint64, string]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.getOrCreateLocked(volumeID)
	m.Import(entries)
	s.persistLocked(volumeID)
}

// Ensure creates an empty map for volume if none exists and persists it, so a
// freshly-created volume is established on its replica set before any write.
func (s *Store) Ensure(volumeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.maps[volumeID]; ok {
		return
	}
	s.getOrCreateLocked(volumeID)
	s.persistLocked(volumeID)
}

// Delete drops volume's map from memory and removes its persisted file, so a
// removed volume's extent map does not outlive it on this node. It is
// idempotent: deleting an unknown volume, or one with no file on disk, is a
// no-op. The delete path calls it directly on a volume's replica set; the GC
// reaper calls it via Reap for any copy the direct delete missed.
func (s *Store) Delete(volumeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteLocked(volumeID)
}

// Reap deletes every map this node holds whose volume id is NOT in live and
// whose persisted file was last modified before reapBefore, returning the ids
// it reaped (sorted). It is the GC backstop for the delete path: a volume
// removed from the namespace is absent from live, so its orphaned extent-map
// copies are reclaimed here once they are old enough that the removal has surely
// propagated. The age guard is what keeps a freshly-created volume whose
// directory entry has not yet gossiped to this node — present on disk but not
// yet in live — from being mistaken for a deleted one. An in-memory store (no
// data dir) has no file age to judge by and reaps nothing.
func (s *Store) Reap(live map[string]struct{}, reapBefore time.Time) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dir == "" {
		return nil, nil
	}
	var (
		reaped []string
		errs   []error
	)
	for id := range s.maps {
		if _, ok := live[id]; ok {
			continue
		}
		fi, err := os.Stat(s.filename(id))
		if errors.Is(err, os.ErrNotExist) {
			// The file is already gone; drop the stale in-memory entry too.
			delete(s.maps, id)
			reaped = append(reaped, id)
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("extentmap: could not stat the map of volume %q to judge its age (%w)", id, err))
			continue
		}
		if !fi.ModTime().Before(reapBefore) {
			continue // too young: the removal may not have propagated to us yet
		}
		if err := s.deleteLocked(id); err != nil {
			errs = append(errs, fmt.Errorf("extentmap: could not reap the map of volume %q (%w)", id, err))
			continue
		}
		reaped = append(reaped, id)
	}
	sort.Strings(reaped)
	return reaped, errors.Join(errs...)
}

// deleteLocked removes volume from the in-memory set and, for a disk-backed
// store, deletes its persisted file and any stale temp file left by an
// interrupted write. A missing file is not an error, so the operation is
// idempotent.
func (s *Store) deleteLocked(volumeID string) error {
	delete(s.maps, volumeID)
	if s.dir == "" {
		return nil
	}
	if err := removeIfExists(s.filename(volumeID)); err != nil {
		return err
	}
	return removeIfExists(s.filename(volumeID) + ".tmp")
}

// removeIfExists deletes path, treating an already-absent path as success.
func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// Volumes returns the ids of the volumes this node holds a map for, sorted.
func (s *Store) Volumes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.maps))
	for id := range s.maps {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// getOrCreateLocked returns volume's map, creating an empty one if absent.
func (s *Store) getOrCreateLocked(volumeID string) *crdt.LWWMap[uint64, string] {
	m, ok := s.maps[volumeID]
	if !ok {
		m = crdt.NewLWWMap[uint64, string]()
		s.maps[volumeID] = m
	}
	return m
}

// filename is the per-volume file under dir. The id is base64url-encoded so any
// node id is filename-safe; the id is also stored inside the file, so loading
// never decodes the name.
func (s *Store) filename(volumeID string) string {
	return filepath.Join(s.dir, base64.RawURLEncoding.EncodeToString([]byte(volumeID))+fileSuffix)
}

// persistLocked writes volume's map to disk for disk-backed stores. It is
// best-effort: a failure is logged, not returned, because the map already
// applied in memory and is replicated to peers — the on-disk copy is a recovery
// cache, not the source of truth.
func (s *Store) persistLocked(volumeID string) {
	if s.dir == "" {
		return
	}
	m := s.maps[volumeID]
	b, err := marshal(wireMap{VolumeID: volumeID, Entries: m.Entries()})
	if err != nil {
		s.logger.Warn("extentmap: could not serialise a map to persist it", "volume", volumeID, "error", err)
		return
	}
	if err := writeFileAtomic(s.filename(volumeID), b); err != nil {
		s.logger.Warn("extentmap: could not persist a map to disk; it will be recovered from peers", "volume", volumeID, "error", err)
	}
}

// loadLocked reads every persisted map under s.dir into memory. A corrupt file
// is logged and skipped so one bad file cannot stop the node booting.
func (s *Store) loadLocked() error {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("extentmap: could not read the data directory at %s (%w)", s.dir, err)
	}
	for _, e := range ents {
		if e.IsDir() || filepath.Ext(e.Name()) == "" || len(e.Name()) <= len(fileSuffix) || e.Name()[len(e.Name())-len(fileSuffix):] != fileSuffix {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.dir, e.Name())) // #nosec G304 -- name comes from ReadDir of our own data dir
		if err != nil {
			s.logger.Warn("extentmap: could not read a map file; skipping it", "file", e.Name(), "error", err)
			continue
		}
		var wm wireMap
		if err := json.Unmarshal(raw, &wm); err != nil || wm.VolumeID == "" {
			s.logger.Warn("extentmap: a map file is corrupt; ignoring it and recovering from peers", "file", e.Name(), "error", err)
			continue
		}
		m := crdt.NewLWWMap[uint64, string]()
		m.Import(wm.Entries)
		s.maps[wm.VolumeID] = m
	}
	return nil
}

// writeFileAtomic writes data via a temp file + rename so a crash leaves either
// the old map or the new, never a torn file.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
