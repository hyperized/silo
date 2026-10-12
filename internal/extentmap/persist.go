package extentmap

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hyperized/silo/internal/atomicfile"
	"github.com/hyperized/silo/internal/crdt"
)

// logSuffix tags a volume's append-only log, which sits beside its snapshot.
const logSuffix = ".emap.log"

const (
	// defaultCompactBytes bounds how much log a volume carries before its
	// snapshot is rewritten. Rewriting a 40 GiB volume's snapshot costs tens of
	// milliseconds; 4 MiB of log is a few thousand 16-extent writes, so that
	// cost is spread thin while a restart replays at most a few megabytes.
	defaultCompactBytes int64 = 4 << 20
	// maxCompactBytes caps WithCompactBytes. It keeps every record's length
	// inside the u32 field, with plenty of room to spare.
	maxCompactBytes int64 = 1 << 30
	// defaultCompactRecords bounds the record count as well, for the case of
	// many tiny single-extent records, where the byte bound alone would let
	// replay walk tens of thousands of them.
	defaultCompactRecords = 16384
	// maxRetainedRecordBuf is the largest encode buffer the store keeps between
	// appends. A big Merge should not pin its buffer for the life of the node.
	maxRetainedRecordBuf = 64 << 10
)

// Option configures a Store opened with Open.
type Option func(*Store)

// WithCompactBytes sets how many bytes of log a volume may carry before the
// write that would cross it rewrites the snapshot instead of appending. A value
// of zero or less keeps the default (4 MiB); values above 1 GiB are capped.
func WithCompactBytes(n int64) Option {
	return func(s *Store) {
		if n > 0 {
			s.compactBytes = min(n, maxCompactBytes)
		}
	}
}

// WithCompactRecords sets how many records a volume's log may hold before the
// next write rewrites the snapshot instead of appending. A value of zero or less
// keeps the default (16384).
func WithCompactRecords(n int) Option {
	return func(s *Store) {
		if n > 0 {
			s.compactRecords = n
		}
	}
}

// logState is what the store knows about one volume's files. A volume has an
// entry only once its snapshot is on disk, so a missing entry means the next
// save has to write the snapshot.
type logState struct {
	bytes   int64
	records int
	// created means the log file exists and its directory entry is durable, so
	// an append needs no directory fsync.
	created bool
	// stale means the log may end in bytes the store did not account for (an
	// append that failed part way, or a torn tail compaction at Open could not
	// clear), or that memory holds bindings neither file has. Either way the
	// next save must rewrite the snapshot rather than append.
	stale bool
}

// logFile is the part of *os.File the log code uses.
type logFile interface {
	io.Writer
	Sync() error
	Close() error
}

// openLog is the file-system seam for the log: appends, truncation and
// directory fsyncs all open through it, so tests can fail each step.
var openLog = func(path string, flag int) (logFile, error) {
	return os.OpenFile(path, flag, 0o600) // #nosec G304 -- path is built from our own data dir
}

func (s *Store) logPath(volumeID string) string {
	return filepath.Join(s.dir, encodeVolumeID(volumeID)+logSuffix)
}

// persistLocked makes volume's latest change durable for disk-backed stores
// and returns any failure. delta is the set of entries the change applied; it
// becomes one log record. The map has already changed in memory by the time
// this runs, so the caller decides what a failure means; replication must not
// count this node's copy as durable.
func (s *Store) persistLocked(volumeID string, delta []crdt.MapEntry[uint64, string]) error {
	if s.dir == "" {
		return nil
	}
	st, ok := s.logs[volumeID]
	if !ok || st.stale {
		return s.compactLocked(volumeID)
	}
	if len(delta) == 0 {
		return nil
	}
	size, fits := recordSize(delta)
	if !fits || st.bytes+int64(size) > s.compactBytes || st.records >= s.compactRecords {
		return s.compactLocked(volumeID)
	}
	return s.appendLocked(volumeID, st, delta)
}

// appendLocked writes delta as one record at the end of volume's log and
// fsyncs it.
func (s *Store) appendLocked(volumeID string, st *logState, delta []crdt.MapEntry[uint64, string]) error {
	s.recBuf = appendRecord(s.recBuf[:0], delta)
	rec := s.recBuf
	if cap(s.recBuf) > maxRetainedRecordBuf {
		s.recBuf = nil
	}
	if err := writeLogRecord(s.logPath(volumeID), rec, !st.created); err != nil {
		// Some of the record may have reached the file. The next save rewrites
		// the snapshot and empties the log, so those bytes never sit between
		// two good records where replay would take them for damage.
		st.stale = true
		return s.saveError(volumeID, err)
	}
	st.created = true
	st.bytes += int64(len(rec))
	st.records++
	return nil
}

// writeLogRecord appends rec to the log at path and fsyncs it. A log that may
// have just been created also gets its directory fsynced, or a crash could
// lose the file along with every record in it.
func writeLogRecord(path string, rec []byte, syncParent bool) error {
	f, err := openLog(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE)
	if err != nil {
		return fmt.Errorf("could not open the log %s (%w)", path, err)
	}
	if _, err := f.Write(rec); err != nil {
		_ = f.Close()
		return fmt.Errorf("could not append to the log %s (%w)", path, err)
	}
	if err := syncClose(f); err != nil {
		return fmt.Errorf("could not flush the log %s to disk (%w)", path, err)
	}
	if syncParent {
		return syncDir(filepath.Dir(path))
	}
	return nil
}

// compactLocked writes volume's whole map as its snapshot and then empties its
// log. The order is what makes it crash-safe. Until atomicfile.Write returns,
// the disk holds the old snapshot and the full log, which replay turns into the
// current map. After it returns, the disk holds the new snapshot and either the
// full log or an empty one; replaying the full log on top of the new snapshot
// changes nothing, because every record in it is already in the snapshot and an
// LWW import of an entry with the same timestamp is a no-op. No order of crash
// leaves less than the map as of the last successful save.
//
// A failure to empty the log is logged, not returned: the snapshot already holds
// everything, so the change is durable, and the next save tries again.
func (s *Store) compactLocked(volumeID string) error {
	m := s.maps[volumeID]
	b, err := marshal(wireMap{VolumeID: volumeID, Entries: m.Entries()})
	if err == nil {
		err = atomicfile.Write(s.filename(volumeID), b, 0o600)
	}
	st, ok := s.logs[volumeID]
	if err != nil {
		if ok {
			st.stale = true
		}
		return s.saveError(volumeID, err)
	}
	if !ok {
		st = &logState{}
		s.logs[volumeID] = st
	}
	existed, err := truncateLog(s.logPath(volumeID))
	if err != nil {
		st.stale = true
		s.logger.Warn("extentmap: saved a volume's extent map, but could not empty the log beside it; the next write will try again. Check permissions and free space on the data directory",
			"volume", volumeID, "dir", s.dir, "error", err)
		return nil
	}
	*st = logState{created: existed}
	return nil
}

// truncateLog empties the log at path and fsyncs it, reporting whether the file
// exists. It truncates rather than deletes so the file keeps its directory
// entry, and the next append needs no directory fsync. A missing log is fine:
// the first append creates it.
func truncateLog(path string) (bool, error) {
	f, err := openLog(path, os.O_WRONLY|os.O_TRUNC)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return true, fmt.Errorf("could not open the log %s to empty it (%w)", path, err)
	}
	if err := syncClose(f); err != nil {
		return true, fmt.Errorf("could not flush the emptied log %s to disk (%w)", path, err)
	}
	return true, nil
}

// syncDir makes a new directory entry under dir durable.
func syncDir(dir string) error {
	d, err := openLog(dir, os.O_RDONLY)
	if err != nil {
		return fmt.Errorf("could not open the directory %s to flush it (%w)", dir, err)
	}
	if err := syncClose(d); err != nil {
		return fmt.Errorf("could not flush the directory %s to disk (%w)", dir, err)
	}
	return nil
}

// syncClose fsyncs and closes f, closing it even when the fsync fails.
func syncClose(f logFile) error {
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (s *Store) saveError(volumeID string, err error) error {
	return fmt.Errorf("extentmap: the extent map of volume %q changed in memory but could not be saved under %s (%w); check free space and permissions on that directory", volumeID, s.dir, err)
}

// replayLogLocked replays one volume's log on top of the snapshot loaded for it
// and, when the log held anything, compacts so the restart leaves an empty log.
func (s *Store) replayLogLocked(name string) {
	path := filepath.Join(s.dir, name)
	volumeID, ok := volumeFromFilename(name)
	if !ok {
		s.logger.Warn("extentmap: a file that looks like an extent-map log has a name that does not decode to a volume; delete it or report it", "file", path)
		return
	}
	m, held := s.maps[volumeID]
	if !held {
		s.orphanLogLocked(volumeID, path)
		return
	}
	raw, err := os.ReadFile(path) // #nosec G304 -- name comes from ReadDir of our own data dir
	if err != nil {
		// Serving the snapshot without the log would serve an older map.
		delete(s.maps, volumeID)
		delete(s.logs, volumeID)
		s.lost[volumeID] = struct{}{}
		s.logger.Warn("extentmap: could not read a volume's extent-map log; fix the file's permissions and restart, otherwise the volume will be rebuilt from every replica before it is served",
			"volume", volumeID, "file", path, "error", err)
		return
	}
	if len(raw) == 0 {
		s.logs[volumeID].created = true
		return
	}
	records, torn, err := replayLog(raw, m.Import)
	if err != nil {
		s.quarantineLogLocked(volumeID, path, err)
		return
	}
	if torn {
		s.logger.Warn("extentmap: a volume's extent-map log ends in a record cut short by a crash; it was never acknowledged, so the map was restored up to the record before it",
			"volume", volumeID, "file", path, "records_replayed", records)
	}
	if err := s.compactLocked(volumeID); err != nil {
		s.logger.Warn("extentmap: replayed a volume's extent-map log but could not fold it into a fresh snapshot; the map is intact and the next write will try again. Check free space and permissions on the data directory",
			"volume", volumeID, "error", err)
	}
}

// orphanLogLocked handles a log with no snapshot loaded for its volume. When the
// snapshot was damaged or unreadable the volume is already lost, and the log
// stays where it is: the rebuild merges every replica, which covers every
// binding the log could hold, and the first save afterwards empties it. A log
// with no snapshot at all is not a state this store leaves behind, since the
// snapshot is written before the first record and deleted after the log, so
// something removed the snapshot and the log alone is a partial map.
func (s *Store) orphanLogLocked(volumeID, path string) {
	if _, lost := s.lost[volumeID]; lost {
		return
	}
	s.quarantineLogLocked(volumeID, path, errors.New("the log has no snapshot beside it"))
}

// quarantineLogLocked moves a damaged log aside, together with its snapshot, and
// flags the volume lost. The snapshot goes too because a snapshot left beside a
// .corrupt log reads, on the next boot, as a volume that was rebuilt, and the
// lost flag would not survive a restart.
func (s *Store) quarantineLogLocked(volumeID, path string, cause error) {
	delete(s.maps, volumeID)
	delete(s.logs, volumeID)
	s.lost[volumeID] = struct{}{}
	snap := s.filename(volumeID)
	// The snapshot moves first. If the log's rename then fails, the next boot
	// finds a log with no snapshot and lands here again.
	for _, p := range []string{snap, path} {
		if err := os.Rename(p, p+corruptSuffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.logger.Warn("extentmap: could not move a damaged extent-map file aside; the volume will still be rebuilt from every replica before it is served. Check permissions on the data directory so the file can be renamed",
				"volume", volumeID, "file", p, "error", err)
			return
		}
	}
	s.logger.Warn("extentmap: a volume's extent-map log cannot be trusted, and a crash does not explain it; the log and its snapshot were moved aside as .corrupt and the volume will be rebuilt from every replica before it is served. Check the disk's health and that nothing else writes to the data directory, then delete the .corrupt files once the volume serves again",
		"volume", volumeID, "file", path, "error", cause)
}
