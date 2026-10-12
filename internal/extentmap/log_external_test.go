package extentmap_test

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/extentmap"
	"github.com/hyperized/silo/internal/hlc"
)

// logFile is the on-disk path of a volume's log under dir.
func logFile(dir, volumeID string) string {
	return emapFile(dir, volumeID)[:len(emapFile(dir, volumeID))-len(".emap.json")] + ".emap.log"
}

func size(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.Size()
}

func mustOpen(t *testing.T, dir string, opts ...extentmap.Option) *extentmap.Store {
	t.Helper()
	s, err := extentmap.Open(dir, discard(), opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

// writeThree leaves vol with a snapshot from Ensure and three appended records,
// returning the log's size after each record.
func writeThree(t *testing.T, dir string) []int64 {
	t.Helper()
	s := mustOpen(t, dir)
	if err := s.Ensure("vol"); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	var sizes []int64
	for i := range uint64(3) {
		if err := s.SetBatch("vol", []uint64{i, i + 10}, []string{"a", "b"}, ts(int64(i)+1)); err != nil {
			t.Fatalf("SetBatch: %v", err)
		}
		sizes = append(sizes, size(t, logFile(dir, "vol")))
	}
	return sizes
}

// A write appends to the log instead of rewriting the snapshot.
func TestLog_WritesAppendAndSnapshotStaysPut(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	if err := s.Ensure("vol"); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	snap, err := os.ReadFile(emapFile(dir, "vol"))
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	var last int64
	for i := range uint64(5) {
		if err := s.SetBatch("vol", []uint64{i}, []string{"c"}, ts(1)); err != nil {
			t.Fatalf("SetBatch: %v", err)
		}
		if n := size(t, logFile(dir, "vol")); n <= last {
			t.Fatalf("log did not grow: %d after %d", n, last)
		} else {
			last = n
		}
	}
	if now, _ := os.ReadFile(emapFile(dir, "vol")); !bytes.Equal(now, snap) {
		t.Error("an append must not rewrite the snapshot")
	}
}

func TestLog_CompactionThresholds(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  extentmap.Option
	}{
		{"by records", extentmap.WithCompactRecords(3)},
		{"by bytes", extentmap.WithCompactBytes(200)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := mustOpen(t, dir, tc.opt)
			_ = s.Ensure("vol")
			emptied := 0
			for i := range uint64(12) {
				if err := s.SetBatch("vol", []uint64{i}, []string{"chunk-id"}, ts(int64(i)+1)); err != nil {
					t.Fatalf("SetBatch: %v", err)
				}
				if size(t, logFile(dir, "vol")) == 0 {
					emptied++
				}
			}
			if emptied == 0 {
				t.Fatal("the log never compacted")
			}
			re := mustOpen(t, dir)
			if re.Len("vol") != 12 {
				t.Errorf("after compactions and a reopen Len = %d, want 12", re.Len("vol"))
			}
		})
	}
}

// A chunk id too long for the record format is saved by compaction instead.
func TestLog_OversizedIDCompacts(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	_ = s.Ensure("vol")
	long := string(bytes.Repeat([]byte("x"), 70000))
	if err := s.Set("vol", 0, long, ts(1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if n := size(t, emapFile(dir, "vol")); n < 70000 {
		t.Errorf("the snapshot should hold the long id, size %d", n)
	}
	if id, _ := mustOpen(t, dir).Get("vol", 0); id != long {
		t.Error("the long id did not survive a reopen")
	}
}

// The crash states a volume's files can be found in, written out by hand. Each
// must open to the newest acknowledged map, not lost, with an empty log.
func TestLog_CrashStates(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mangle  func(t *testing.T, dir string, sizes []int64)
		wantLen int
	}{
		{"snapshot plus log", func(*testing.T, string, []int64) {}, 6},
		{"snapshot plus log cut inside the last record", func(t *testing.T, dir string, sizes []int64) {
			if err := os.Truncate(logFile(dir, "vol"), sizes[2]-3); err != nil {
				t.Fatal(err)
			}
		}, 4},
		{"snapshot plus log cut inside the last header", func(t *testing.T, dir string, sizes []int64) {
			if err := os.Truncate(logFile(dir, "vol"), sizes[1]+4); err != nil {
				t.Fatal(err)
			}
		}, 4},
		{"snapshot plus log with a zero-filled tail", func(t *testing.T, dir string, _ []int64) {
			f, err := os.OpenFile(logFile(dir, "vol"), os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			_, _ = f.Write(make([]byte, 4096))
		}, 6},
		{"snapshot without log", func(t *testing.T, dir string, _ []int64) {
			// A log emptied and then lost entirely, e.g. by an operator.
			_ = mustOpen(t, dir) // folds the log into the snapshot
			if err := os.Remove(logFile(dir, "vol")); err != nil {
				t.Fatal(err)
			}
		}, 6},
		{"new snapshot plus the full log it already holds", func(t *testing.T, dir string, _ []int64) {
			// A crash between writing the snapshot and emptying the log.
			full, _ := os.ReadFile(logFile(dir, "vol"))
			_ = mustOpen(t, dir)
			mustWrite(t, logFile(dir, "vol"), string(full))
		}, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			sizes := writeThree(t, dir)
			tc.mangle(t, dir, sizes)
			s := mustOpen(t, dir)
			if s.Lost("vol") || s.Len("vol") != tc.wantLen {
				t.Fatalf("Lost=%v Len=%d, want a healthy map of %d", s.Lost("vol"), s.Len("vol"), tc.wantLen)
			}
			if fi, err := os.Stat(logFile(dir, "vol")); err == nil && fi.Size() != 0 {
				t.Errorf("Open should leave the log empty, size %d", fi.Size())
			}
			// And the store keeps working on top of the recovered state.
			if err := s.Set("vol", 99, "after", ts(100)); err != nil {
				t.Fatalf("Set after recovery: %v", err)
			}
			if mustOpen(t, dir).Len("vol") != tc.wantLen+1 {
				t.Error("a write after recovery did not survive a reopen")
			}
		})
	}
}

// A v0.14.0 data directory holds only snapshots; it loads as is and takes appends.
func TestLog_UpgradeFromSnapshotOnlyDirectory(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, emapFile(dir, "vol"), `{"volume_id":"vol","entries":[{"key":0,"value":"c0","ts":{"wall":5,"logical":0,"node":"n"}}]}`)
	s := mustOpen(t, dir)
	if id, ok := s.Get("vol", 0); !ok || id != "c0" || s.Lost("vol") {
		t.Fatalf("Get = (%q,%v) lost=%v", id, ok, s.Lost("vol"))
	}
	if err := s.Set("vol", 1, "c1", ts(6)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if size(t, logFile(dir, "vol")) == 0 {
		t.Error("a write to an upgraded volume should append to a new log")
	}
	if mustOpen(t, dir).Len("vol") != 2 {
		t.Error("the appended write did not survive a reopen")
	}
}

// Replay is LWW by HLC on top of the snapshot and idempotent: the log's newer
// binding wins, its older one loses, and replaying twice changes nothing.
func TestLog_ReplayIsLWWAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, emapFile(dir, "vol"), `{"volume_id":"vol","entries":[`+
		`{"key":0,"value":"snap0","ts":{"wall":10,"logical":0,"node":"n"}},`+
		`{"key":1,"value":"snap1","ts":{"wall":10,"logical":0,"node":"n"}}]}`)
	// Produce a log with a newer binding for 0 and an older one for 1.
	other := t.TempDir()
	w := mustOpen(t, other)
	_ = w.Ensure("vol")
	_ = w.Merge("vol", []crdt.MapEntry[uint64, string]{
		{Key: 0, Value: "log0", TS: hlc.Timestamp{Wall: 20, Node: "n"}},
		{Key: 1, Value: "log1", TS: hlc.Timestamp{Wall: 5, Node: "n"}},
	})
	rec, err := os.ReadFile(logFile(other, "vol"))
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, logFile(dir, "vol"), string(rec))
	once := mustOpen(t, dir)
	if id, _ := once.Get("vol", 0); id != "log0" {
		t.Errorf("extent 0 = %q, want the log's newer log0", id)
	}
	if id, _ := once.Get("vol", 1); id != "snap1" {
		t.Errorf("extent 1 = %q, want the snapshot's newer snap1", id)
	}
	digest := once.Digest("vol")
	mustWrite(t, logFile(dir, "vol"), string(rec)+string(rec))
	twice := mustOpen(t, dir)
	if !bytes.Equal(twice.Digest("vol"), digest) {
		t.Error("replaying the same records again changed the map")
	}
}

// Damage that a crash cannot explain sets the volume aside like a damaged
// snapshot: both files become .corrupt, the volume is lost, and it stays lost
// across a restart until it is rebuilt.
func TestLog_DamageMarksVolumeLost(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mangle func(t *testing.T, dir string)
	}{
		{"checksum mismatch in the middle", func(t *testing.T, dir string) {
			b, _ := os.ReadFile(logFile(dir, "vol"))
			b[10] ^= 0xff
			mustWrite(t, logFile(dir, "vol"), string(b))
		}},
		{"log without its snapshot", func(t *testing.T, dir string) {
			if err := os.Remove(emapFile(dir, "vol")); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeThree(t, dir)
			tc.mangle(t, dir)
			s := mustOpen(t, dir)
			if !s.Lost("vol") || s.Has("vol") {
				t.Fatalf("Lost=%v Has=%v, want a lost volume with no map", s.Lost("vol"), s.Has("vol"))
			}
			if _, err := os.Stat(logFile(dir, "vol") + ".corrupt"); err != nil {
				t.Errorf("the log should be kept as .corrupt: %v", err)
			}
			for _, p := range []string{logFile(dir, "vol"), emapFile(dir, "vol")} {
				if _, err := os.Stat(p); !os.IsNotExist(err) {
					t.Errorf("%s should have been moved aside, stat err = %v", filepath.Base(p), err)
				}
			}
			if !mustOpen(t, dir).Lost("vol") {
				t.Error("a restart before the rebuild must keep the volume lost")
			}
			// A rebuild writes a fresh snapshot, after which the leftovers no
			// longer hold the volume hostage.
			if err := s.Merge("vol", []crdt.MapEntry[uint64, string]{{Key: 0, Value: "peer", TS: ts(50)}}); err != nil {
				t.Fatalf("Merge: %v", err)
			}
			if re := mustOpen(t, dir); re.Lost("vol") || re.Len("vol") != 1 {
				t.Errorf("after a rebuild Lost=%v Len=%d", re.Lost("vol"), re.Len("vol"))
			}
		})
	}
}

// Damage that cannot be moved aside still flags the volume lost.
func TestLog_DamageRenameFailureStillFlags(t *testing.T) {
	dir := t.TempDir()
	writeThree(t, dir)
	b, _ := os.ReadFile(logFile(dir, "vol"))
	b[10] ^= 0xff
	mustWrite(t, logFile(dir, "vol"), string(b))
	if err := os.MkdirAll(filepath.Join(emapFile(dir, "vol")+".corrupt", "occupied"), 0o700); err != nil {
		t.Fatal(err)
	}
	if s := mustOpen(t, dir); !s.Lost("vol") {
		t.Error("the volume must be lost even when its files cannot be moved aside")
	}
}

func TestLog_OpenEdgeCases(t *testing.T) {
	dir := t.TempDir()
	_ = mustOpen(t, dir).Ensure("vol")
	mustWrite(t, logFile(dir, "vol"), "")                     // an empty log is a clean one
	mustWrite(t, filepath.Join(dir, "a.emap.log"), "x")       // name does not decode
	mustWrite(t, filepath.Join(dir, ".emap.log"), "x")        // no name at all
	mustWrite(t, logFile(dir, "vol")+".tmp", "half")          // never produced, but cleaned
	mustWrite(t, logFile(dir, "vol")+".corrupt", "inspected") // left from a rebuilt volume
	s := mustOpen(t, dir)
	if !s.Has("vol") || s.Lost("vol") {
		t.Errorf("Has=%v Lost=%v, want a healthy volume", s.Has("vol"), s.Lost("vol"))
	}
	if got := s.Volumes(); !reflect.DeepEqual(got, []string{"vol"}) {
		t.Errorf("Volumes = %v", got)
	}
	if _, err := os.Stat(logFile(dir, "vol") + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("a stale log temp file should be removed, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.emap.log")); err != nil {
		t.Errorf("an undecodable log name should be left alone: %v", err)
	}
}

func TestLog_UnreadableLogFlagsVolumeLost(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test is meaningless as root")
	}
	dir := t.TempDir()
	writeThree(t, dir)
	if err := os.Chmod(logFile(dir, "vol"), 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(logFile(dir, "vol"), 0o600) }()
	s := mustOpen(t, dir)
	if !s.Lost("vol") || s.Has("vol") {
		t.Errorf("Lost=%v Has=%v: serving the snapshot without its log would serve an older map", s.Lost("vol"), s.Has("vol"))
	}
	if _, err := os.Stat(logFile(dir, "vol")); err != nil {
		t.Errorf("an unreadable log should stay where it is: %v", err)
	}
}

// When Open cannot fold the log into a fresh snapshot, the replayed map still
// serves and the first write afterwards compacts.
func TestLog_CompactionAtOpenFailureIsRetried(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test is meaningless as root")
	}
	dir := t.TempDir()
	sizes := writeThree(t, dir)
	if err := os.Truncate(logFile(dir, "vol"), sizes[2]-1); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	s := mustOpen(t, dir)
	_ = os.Chmod(dir, 0o700)
	if s.Lost("vol") || s.Len("vol") != 4 {
		t.Fatalf("Lost=%v Len=%d, want the replayed map", s.Lost("vol"), s.Len("vol"))
	}
	if err := s.Set("vol", 50, "c", ts(50)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if n := size(t, logFile(dir, "vol")); n != 0 {
		t.Errorf("the first write should compact away the torn tail, log size %d", n)
	}
	if mustOpen(t, dir).Len("vol") != 5 {
		t.Error("the write after a failed compaction did not survive a reopen")
	}
}

func TestLog_DeleteAndReap(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	for _, v := range []string{"del", "old", "fresh-log"} {
		_ = s.Ensure(v)
		_ = s.Set(v, 0, "c", ts(1))
		mustWrite(t, logFile(dir, v)+".corrupt", "x")
	}
	if err := s.Delete("del"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for _, p := range []string{logFile(dir, "del"), logFile(dir, "del") + ".corrupt", emapFile(dir, "del")} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("Delete should remove %s, stat err = %v", filepath.Base(p), err)
		}
	}
	past := time.Now().Add(-2 * time.Hour)
	for _, p := range []string{emapFile(dir, "old"), logFile(dir, "old"), emapFile(dir, "fresh-log")} {
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatal(err)
		}
	}
	// fresh-log's snapshot is old but its log was just written: a volume still
	// being written must not be reaped on its snapshot's age.
	reaped, err := s.Reap(map[string]struct{}{}, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if !reflect.DeepEqual(reaped, []string{"old"}) {
		t.Errorf("reaped = %v, want [old]", reaped)
	}
	if _, err := os.Stat(logFile(dir, "old")); !os.IsNotExist(err) {
		t.Errorf("a reaped volume's log should be removed, stat err = %v", err)
	}
}

// A snapshot found under a name other than its volume's is rewritten under the
// right one on the first save, so its log never sits without a snapshot.
func TestLog_MisnamedSnapshotIsRewrittenBeforeAppending(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "copied.emap.json"), `{"volume_id":"vol","entries":[]}`)
	s := mustOpen(t, dir)
	if err := s.Set("vol", 0, "c0", ts(1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := os.Stat(emapFile(dir, "vol")); err != nil {
		t.Fatalf("the first save should write the snapshot under its own name: %v", err)
	}
	if re := mustOpen(t, dir); re.Lost("vol") || re.Len("vol") != 1 {
		t.Errorf("Lost=%v Len=%d after reopen", re.Lost("vol"), re.Len("vol"))
	}
}
