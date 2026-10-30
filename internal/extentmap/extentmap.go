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
	"strings"
	"sync"
	"time"

	"github.com/hyperized/silo/internal/atomicfile"
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

// fileSuffix tags this store's per-volume files in the data directory.
const fileSuffix = ".emap.json"

// corruptSuffix is appended to a map file that failed to load. The file keeps
// its place in the data directory for inspection, and the extra suffix keeps
// the next Open from trying to load it again. Its presence without a map beside
// it is what keeps the volume flagged lost across restarts.
const corruptSuffix = ".corrupt"

// marshal is the serialization seam; tests override it to exercise the
// persistence error path.
var marshal = json.Marshal

// Store holds the extent maps for the volumes whose replica set includes this
// node, keyed by volume inode id. It is safe for concurrent use. An empty
// directory means in-memory only (the maps are still recoverable from peer
// replicas).
//
// When opened with a directory, each volume has a snapshot, <dir>/<id>.emap.json,
// and an append-only log beside it, <dir>/<id>.emap.log. A write appends one
// small record to the log and fsyncs it, so the cost of a write follows the
// size of the write, not the size of the volume. Once the log grows past a
// threshold the next write rewrites the snapshot and empties the log instead.
// Open loads each snapshot and replays its log on top.
type Store struct {
	dir            string
	logger         *slog.Logger
	compactBytes   int64
	compactRecords int

	mu   sync.Mutex
	maps map[string]*volMap
	// logs tracks the on-disk state of each volume whose snapshot exists.
	logs map[string]*logState
	// recBuf is reused to encode log records; s.mu guards it.
	recBuf []byte
	// lost holds the volumes whose persisted map could not be loaded at Open.
	// Such a volume may hold newer bindings than any single peer, so the serve
	// path must consult every replica before trusting what it has.
	lost map[string]struct{}
}

// New returns an in-memory store. A nil logger defaults to slog.Default.
func New(logger *slog.Logger) *Store {
	if logger == nil {
		logger = slog.Default()
	}
	return &Store{
		logger:         logger,
		compactBytes:   defaultCompactBytes,
		compactRecords: defaultCompactRecords,
		maps:           map[string]*volMap{},
		logs:           map[string]*logState{},
		lost:           map[string]struct{}{},
	}
}

// Open returns a store backed by dir: it creates dir if missing, loads every
// extent map previously persisted there, and persists local mutations going
// forward. A per-volume file that cannot be loaded does not stop the node: a
// damaged one is renamed to <file>.corrupt, its volume is reported by Lost, and
// the map is rebuilt from the volume's replicas. A .corrupt file with no map
// beside it flags its volume lost again on the next Open, so a restart before
// the rebuild does not forget the damage. Temp files left by an interrupted
// write are removed. A nil logger defaults to slog.Default.
//
// Each volume's log is replayed on top of its snapshot and, when it held
// anything, folded into a fresh snapshot, so a restart leaves every log empty.
// A log that ends in a record cut short by a crash is replayed up to that
// record, which was never acknowledged. A log damaged anywhere else is moved
// aside with its snapshot and the volume is rebuilt from its replicas, as for a
// damaged snapshot.
func Open(dir string, logger *slog.Logger, opts ...Option) (*Store, error) {
	s := New(logger)
	s.dir = dir
	for _, opt := range opts {
		opt(s)
	}
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
// delta never moves an extent backward. A chunkID of Unmapped records the
// extent as unmapped. A non-nil error means the binding is in memory but not on
// disk; see SetBatch.
func (s *Store) Set(volumeID string, index uint64, chunkID string, ts hlc.Timestamp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getOrCreateLocked(volumeID).set(index, chunkID, ts)
	return s.persistLocked(volumeID, []crdt.MapEntry[uint64, string]{{Key: index, Value: chunkID, TS: ts}})
}

// SetBatch binds many extents of volume in one acquisition + one fsynced log
// append. All updates share ts. indexes and chunkIDs are positionally paired;
// their lengths must match, and an Unmapped id unmaps its extent. An empty
// batch is a successful no-op (no persist).
//
// When the map cannot be saved to disk the bindings still apply in memory and
// the persist error is returned, so the caller does not count this node's copy
// as durable.
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
	delta := make([]crdt.MapEntry[uint64, string], len(indexes))
	for i, idx := range indexes {
		m.set(idx, chunkIDs[i], ts)
		delta[i] = crdt.MapEntry[uint64, string]{Key: idx, Value: chunkIDs[i], TS: ts}
	}
	return s.persistLocked(volumeID, delta)
}

// Get returns the chunk id backing the extent at index of volume and whether it
// is mapped. An unknown volume, an extent never written and an extent recorded
// as Unmapped all return ("", false).
func (s *Store) Get(volumeID string, index uint64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.maps[volumeID]
	if !ok {
		return "", false
	}
	return m.get(index)
}

// Has reports whether this node holds a map for volume (even an empty one). A
// volume can be both held and Lost when a peer pushed its copy in after Open.
func (s *Store) Has(volumeID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.maps[volumeID]
	return ok
}

// Len returns the number of mapped extents for volume, or 0 if unknown. An
// extent recorded as Unmapped is not counted, so a trimmed volume's count goes
// down even though its map keeps an entry for the extent.
func (s *Store) Len(volumeID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.maps[volumeID]; ok {
		return m.mapped
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
	for _, e := range m.lww.Entries() {
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
// path ships to a peer that needs the whole map, so it includes entries whose
// chunk id is Unmapped: a peer must receive those to stop serving the binding
// they replaced. Callers after live chunk references must skip them.
func (s *Store) Snapshot(volumeID string) []crdt.MapEntry[uint64, string] {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.maps[volumeID]
	if !ok {
		return nil
	}
	entries := m.lww.Entries()
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries
}

// Merge folds a peer's entries into volume's map (creating it if absent) with
// LWW-by-HLC semantics, then persists. Used to warm a serving node's map from a
// replica and to repair a lagging replica. As with SetBatch, a persist failure
// leaves the merge applied in memory and is returned.
func (s *Store) Merge(volumeID string, entries []crdt.MapEntry[uint64, string]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.getOrCreateLocked(volumeID)
	m.importEntries(entries)
	// The log takes entries as given, including any that lost to a newer local
	// binding. Replay applies them with the same LWW rule, so they lose there
	// too, and filtering them here would need the timestamps LWWMap keeps to
	// itself.
	return s.persistLocked(volumeID, entries)
}

// Ensure creates an empty map for volume if none exists and persists it, so a
// freshly-created volume is established on its replica set before any write.
// An existing map is left alone and nil is returned. As with SetBatch, a
// persist failure leaves the empty map in memory and is returned.
func (s *Store) Ensure(volumeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.maps[volumeID]; ok {
		return nil
	}
	s.getOrCreateLocked(volumeID)
	return s.persistLocked(volumeID, nil)
}

// Lost reports whether volume's persisted map was found damaged or unreadable
// at Open and has not been rebuilt since. Whatever this node holds for such a
// volume may be older than what it last wrote, so it must not be served until
// every replica has been merged in and ClearLost called.
func (s *Store) Lost(volumeID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.lost[volumeID]
	return ok
}

// ClearLost records that volume's map has been rebuilt from every replica. It
// does not remove the .corrupt file; that stays for an operator to inspect.
func (s *Store) ClearLost(volumeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.lost, volumeID)
}

// Delete drops volume's map from memory and removes its snapshot, its log, its
// lost flag and any .corrupt copies, so a removed volume's extent map does not
// outlive it on this node. It is idempotent: deleting an unknown volume, or one
// with no files on disk, is a no-op. The delete path calls it directly on a volume's replica set; the GC
// reaper calls it via Reap for any copy the direct delete missed.
func (s *Store) Delete(volumeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteLocked(volumeID)
}

// Reap deletes every map this node holds whose volume id is NOT in live and
// whose snapshot and log were both last modified before reapBefore, returning
// the ids it reaped (sorted). It is the GC backstop for the delete path: a volume
// removed from the namespace is absent from live, so its orphaned extent-map
// copies are reclaimed here once they are old enough that the removal has surely
// propagated. The age guard is what keeps a freshly-created volume whose
// directory entry has not yet gossiped to this node — present on disk but not
// yet in live — from being mistaken for a deleted one. An in-memory store (no
// data dir) has no file age to judge by and reaps nothing.
//
// A volume flagged Lost but never rebuilt has no map in memory; it is judged by
// the age of its .corrupt file, so a deleted volume's damaged copy does not
// linger forever.
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
	for _, id := range s.knownLocked() {
		if _, ok := live[id]; ok {
			continue
		}
		modTime, err := s.modTimeForAgeLocked(id)
		if errors.Is(err, os.ErrNotExist) {
			// The files are already gone; drop the stale in-memory entry too.
			delete(s.maps, id)
			delete(s.logs, id)
			delete(s.lost, id)
			reaped = append(reaped, id)
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("extentmap: could not stat the map of volume %q to judge its age (%w)", id, err))
			continue
		}
		if !modTime.Before(reapBefore) {
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

// knownLocked returns every volume id the store has state for: a map in memory,
// a lost flag, or both.
func (s *Store) knownLocked() []string {
	ids := make([]string, 0, len(s.maps)+len(s.lost))
	for id := range s.maps {
		ids = append(ids, id)
	}
	for id := range s.lost {
		if _, ok := s.maps[id]; !ok {
			ids = append(ids, id)
		}
	}
	return ids
}

// modTimeForAgeLocked returns when volume's map last changed on disk: the newer
// of its snapshot and its log, since a write now touches only the log. When
// neither exists it falls back to the .corrupt copies left after damage, and it
// returns os.ErrNotExist when there is nothing at all.
func (s *Store) modTimeForAgeLocked(volumeID string) (time.Time, error) {
	snap, log := s.filename(volumeID), s.logPath(volumeID)
	t, err := newestModTime(snap, log)
	if errors.Is(err, os.ErrNotExist) {
		return newestModTime(snap+corruptSuffix, log+corruptSuffix)
	}
	return t, err
}

// newestModTime returns the latest modification time among the paths that
// exist, or os.ErrNotExist when none does.
func newestModTime(paths ...string) (time.Time, error) {
	var (
		newest time.Time
		found  bool
	)
	for _, p := range paths {
		fi, err := os.Stat(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return time.Time{}, err
		}
		found = true
		if fi.ModTime().After(newest) {
			newest = fi.ModTime()
		}
	}
	if !found {
		return time.Time{}, os.ErrNotExist
	}
	return newest, nil
}

// deleteLocked removes volume from the in-memory set and its lost flag and, for
// a disk-backed store, deletes its log, snapshot, any stale temp files left by
// an interrupted write, and any .corrupt copies. A missing file is not an
// error, so the operation is idempotent.
//
// The log goes first. A crash part way through then leaves at worst the
// snapshot alone, a whole map that the reaper judges by age as before, and never
// a log without its snapshot, which would read as damage.
func (s *Store) deleteLocked(volumeID string) error {
	delete(s.maps, volumeID)
	delete(s.logs, volumeID)
	delete(s.lost, volumeID)
	if s.dir == "" {
		return nil
	}
	log, snap := s.logPath(volumeID), s.filename(volumeID)
	for _, path := range []string{
		log, log + atomicfile.TempSuffix, log + corruptSuffix,
		snap, snap + atomicfile.TempSuffix, snap + corruptSuffix,
	} {
		if err := removeIfExists(path); err != nil {
			return err
		}
	}
	return nil
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
func (s *Store) getOrCreateLocked(volumeID string) *volMap {
	m, ok := s.maps[volumeID]
	if !ok {
		m = newVolMap()
		s.maps[volumeID] = m
	}
	return m
}

// filename is the per-volume file under dir. The id is base64url-encoded so any
// node id is filename-safe. Loading reads the id from inside the file and only
// decodes the name when the content is too damaged to say which volume it was.
func (s *Store) filename(volumeID string) string {
	return filepath.Join(s.dir, encodeVolumeID(volumeID)+fileSuffix)
}

func encodeVolumeID(volumeID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(volumeID))
}

// loadLocked reads every persisted map under s.dir into memory. A file that
// cannot be loaded is set aside and its volume flagged lost, so one bad file
// cannot stop the node booting and cannot quietly turn into an older map.
func (s *Store) loadLocked() error {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("extentmap: could not read the data directory at %s (%w)", s.dir, err)
	}
	corrupt := map[string]string{} // volume id -> path of its .corrupt file
	var logs []string
	for _, e := range ents {
		name := e.Name()
		switch {
		case e.IsDir():
		case hasStoreSuffix(name, atomicfile.TempSuffix):
			s.removeStaleTemp(name)
		case hasStoreSuffix(name, corruptSuffix):
			s.noteCorrupt(name, corrupt)
		case strings.HasSuffix(name, fileSuffix) && len(name) > len(fileSuffix):
			s.loadFileLocked(name)
		case strings.HasSuffix(name, logSuffix) && len(name) > len(logSuffix):
			logs = append(logs, name)
		}
	}
	// Resolved after the loop because a rebuilt map may sort after its .corrupt
	// file, and only its absence means the volume is still unrebuilt.
	for volumeID, path := range corrupt {
		s.flagLeftoverCorruptLocked(volumeID, path)
	}
	// Logs go last: replay needs every snapshot loaded and every lost flag set.
	for _, name := range logs {
		s.replayLogLocked(name)
	}
	return nil
}

// hasStoreSuffix reports whether name is a snapshot or log file name followed by
// extra, such as a temp or .corrupt suffix.
func hasStoreSuffix(name, extra string) bool {
	return strings.HasSuffix(name, fileSuffix+extra) || strings.HasSuffix(name, logSuffix+extra)
}

// noteCorrupt records the volume a .corrupt file from an earlier boot belongs to.
func (s *Store) noteCorrupt(name string, corrupt map[string]string) {
	path := filepath.Join(s.dir, name)
	volumeID, ok := volumeFromFilename(strings.TrimSuffix(name, corruptSuffix))
	if !ok {
		s.logger.Warn("extentmap: a file that looks like a damaged map file has a name that does not decode to a volume; delete it or report it", "file", path)
		return
	}
	corrupt[volumeID] = path
}

// flagLeftoverCorruptLocked decides what a .corrupt file from an earlier boot
// means. With a map beside it the volume was rebuilt and the file is only kept
// for inspection. Without one the rebuild never finished, and forgetting that
// would let Warm serve the volume from a single, possibly older, replica.
func (s *Store) flagLeftoverCorruptLocked(volumeID, path string) {
	if _, ok := s.maps[volumeID]; ok {
		s.logger.Info("extentmap: a damaged copy of an extent map that has since been rebuilt is still on disk; it is safe to delete", "volume", volumeID, "file", path)
		return
	}
	if _, ok := s.lost[volumeID]; ok {
		return // damaged again this boot; quarantineLocked already said so
	}
	s.lost[volumeID] = struct{}{}
	s.logger.Warn("extentmap: the damaged copy of a volume's extent map from an earlier unclean shutdown is still on disk and no rebuilt map exists yet; the volume will be rebuilt from every replica before it is served. Delete the .corrupt file once the volume serves again, or now if the volume never held data",
		"volume", volumeID, "file", path)
}

// removeStaleTemp deletes a temp file left by a write that a crash cut short.
// The map it was meant to replace is still intact under its real name.
func (s *Store) removeStaleTemp(name string) {
	path := filepath.Join(s.dir, name)
	if err := os.Remove(path); err != nil {
		s.logger.Warn("extentmap: could not remove a temp file left by an interrupted write; it is harmless, but delete it by hand to reclaim the space", "file", path, "error", err)
		return
	}
	s.logger.Info("extentmap: removed a temp file left by an interrupted write", "file", path)
}

// loadFileLocked loads one map file, or flags its volume lost when it cannot.
func (s *Store) loadFileLocked(name string) {
	path := filepath.Join(s.dir, name)
	raw, err := os.ReadFile(path) // #nosec G304 -- name comes from ReadDir of our own data dir
	if err != nil {
		s.markUnreadableLocked(name, err)
		return
	}
	var wm wireMap
	if err := json.Unmarshal(raw, &wm); err != nil || wm.VolumeID == "" {
		if err == nil {
			err = errors.New("the file names no volume")
		}
		s.quarantineLocked(name, err)
		return
	}
	m := newVolMap()
	m.importEntries(wm.Entries)
	s.maps[wm.VolumeID] = m
	// A snapshot under some other name (copied in by hand) is not where the log
	// expects it. Leaving the volume untracked makes the first save write one
	// that is, so its log never ends up without a snapshot beside it.
	if name == encodeVolumeID(wm.VolumeID)+fileSuffix {
		s.logs[wm.VolumeID] = &logState{}
	}
}

// markUnreadableLocked flags the volume of a map file that exists but could not
// be read. The file is left in place: its content may be fine once the read
// problem (usually permissions) is fixed.
func (s *Store) markUnreadableLocked(name string, readErr error) {
	path := filepath.Join(s.dir, name)
	volumeID, ok := volumeFromFilename(name)
	if !ok {
		s.logger.Warn("extentmap: could not read a file that looks like a map file, and its name does not decode to a volume; delete it or report it", "file", path, "error", readErr)
		return
	}
	s.lost[volumeID] = struct{}{}
	s.logger.Warn("extentmap: could not read the local copy of a volume's extent map; fix the file's permissions and restart, otherwise the volume will be rebuilt from every replica before it is served",
		"volume", volumeID, "file", path, "error", readErr)
}

// quarantineLocked moves a damaged map file aside and flags its volume lost.
// Dropping the file instead would let the next peer recovery hand back an older
// map as if it were current, which is how a volume silently rolls back.
func (s *Store) quarantineLocked(name string, parseErr error) {
	path := filepath.Join(s.dir, name)
	volumeID, ok := volumeFromFilename(name)
	if !ok {
		s.logger.Warn("extentmap: a file that looks like a map file is damaged and its name does not decode to a volume; delete it or report it", "file", path, "error", parseErr)
		return
	}
	s.lost[volumeID] = struct{}{}
	dst := path + corruptSuffix
	if err := os.Rename(path, dst); err != nil {
		s.logger.Warn("extentmap: the local copy of a volume's extent map is damaged (most likely by an unclean shutdown) and could not be moved aside; the volume will be rebuilt from every replica before it is served. Check permissions on the data directory so the file can be renamed",
			"volume", volumeID, "file", path, "error", parseErr, "rename_error", err)
		return
	}
	s.logger.Warn("extentmap: the local copy of a volume's extent map was damaged (most likely by an unclean shutdown); it has been moved aside for inspection and the volume will be rebuilt from every replica before it is served. Delete the .corrupt file once the volume serves again",
		"volume", volumeID, "file", dst, "error", parseErr)
}

// volumeFromFilename recovers the volume id encoded in a snapshot or log file's
// name.
func volumeFromFilename(name string) (string, bool) {
	name = strings.TrimSuffix(strings.TrimSuffix(name, fileSuffix), logSuffix)
	raw, err := base64.RawURLEncoding.DecodeString(name)
	if err != nil || len(raw) == 0 {
		return "", false
	}
	return string(raw), true
}
