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
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"

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
