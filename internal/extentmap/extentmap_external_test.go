package extentmap_test

import (
	"encoding/base64"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/extentmap"
	"github.com/hyperized/silo/internal/hlc"
)

// emapFile is the on-disk path of a volume's persisted map under dir, mirroring
// the store's own base64url-of-id naming.
func emapFile(dir, volumeID string) string {
	return filepath.Join(dir, base64.RawURLEncoding.EncodeToString([]byte(volumeID))+".emap.json")
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func ts(wall int64) hlc.Timestamp { return hlc.Timestamp{Wall: wall} }

func TestStore_SetGetSnapshotBatch(t *testing.T) {
	s := extentmap.New(discard())

	s.Set("vol", 0, "c0", ts(10))
	s.Set("vol", 5, "c5", ts(11))

	if id, ok := s.Get("vol", 0); !ok || id != "c0" {
		t.Errorf("Get(0) = (%q,%v), want (c0,true)", id, ok)
	}
	if id, ok := s.Get("vol", 99); ok || id != "" {
		t.Errorf("Get(99) = (%q,%v), want (\"\",false)", id, ok)
	}
	if id, ok := s.Get("missing", 0); ok || id != "" {
		t.Errorf("Get(missing) = (%q,%v), want (\"\",false)", id, ok)
	}

	// Snapshot is sorted by extent index.
	want := []crdt.MapEntry[uint64, string]{{Key: 0, Value: "c0", TS: ts(10)}, {Key: 5, Value: "c5", TS: ts(11)}}
	if got := s.Snapshot("vol"); !reflect.DeepEqual(got, want) {
		t.Errorf("Snapshot = %v, want %v", got, want)
	}
	if got := s.Snapshot("missing"); got != nil {
		t.Errorf("Snapshot(missing) = %v, want nil", got)
	}

	if !s.Has("vol") || s.Has("missing") {
		t.Error("Has wrong")
	}
	if s.Len("vol") != 2 || s.Len("missing") != 0 {
		t.Errorf("Len = (%d,%d), want (2,0)", s.Len("vol"), s.Len("missing"))
	}

	// Batch: paired ok, mismatch errors, empty is a no-op.
	if err := s.SetBatch("vol", []uint64{1, 2}, []string{"c1", "c2"}, ts(12)); err != nil {
		t.Fatalf("SetBatch: %v", err)
	}
	if err := s.SetBatch("vol", []uint64{1}, []string{"a", "b"}, ts(13)); err == nil {
		t.Error("SetBatch with mismatched lengths should error")
	}
	if err := s.SetBatch("vol", nil, nil, ts(13)); err != nil {
		t.Errorf("empty SetBatch should be a no-op, got %v", err)
	}
	if s.Len("vol") != 4 {
		t.Errorf("after batch Len = %d, want 4", s.Len("vol"))
	}
}

func TestStore_MergeIsLWWByHLC(t *testing.T) {
	s := extentmap.New(discard())
	s.Set("vol", 0, "current", ts(10))

	// Older delta loses; newer delta wins; a fresh key is adopted.
	s.Merge("vol", []crdt.MapEntry[uint64, string]{
		{Key: 0, Value: "stale", TS: ts(5)},
		{Key: 1, Value: "fresh", TS: ts(7)},
	})
	if id, _ := s.Get("vol", 0); id != "current" {
		t.Errorf("extent 0 = %q, want current (older merge must not win)", id)
	}
	if id, _ := s.Get("vol", 1); id != "fresh" {
		t.Errorf("extent 1 = %q, want fresh", id)
	}
	s.Merge("vol", []crdt.MapEntry[uint64, string]{{Key: 0, Value: "newer", TS: ts(20)}})
	if id, _ := s.Get("vol", 0); id != "newer" {
		t.Errorf("extent 0 = %q, want newer", id)
	}
	// Merge can create a previously-unknown volume.
	s.Merge("vol2", []crdt.MapEntry[uint64, string]{{Key: 0, Value: "x", TS: ts(1)}})
	if !s.Has("vol2") {
		t.Error("Merge should create an unknown volume")
	}
}

func TestStore_EnsureAndVolumes(t *testing.T) {
	s := extentmap.New(discard())
	s.Ensure("a")
	s.Ensure("a") // idempotent
	s.Set("b", 0, "c", ts(1))
	if !s.Has("a") || s.Len("a") != 0 {
		t.Errorf("Ensure should create an empty map: Has=%v Len=%d", s.Has("a"), s.Len("a"))
	}
	if got := s.Volumes(); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("Volumes = %v, want [a b]", got)
	}
}

func TestStore_PersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// A node id with awkward characters must still round-trip (filename is encoded).
	vol := "inode-123.0.node/with:odd@chars"
	s.Set(vol, 0, "c0", ts(10))
	s.Set(vol, 7, "c7", ts(11))
	s.Ensure("empty-vol")

	reopened, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if id, ok := reopened.Get(vol, 7); !ok || id != "c7" {
		t.Errorf("after reload Get(7) = (%q,%v), want (c7,true)", id, ok)
	}
	if !reopened.Has("empty-vol") {
		t.Error("an Ensure'd empty map should survive a reload")
	}
	if got := reopened.Volumes(); !reflect.DeepEqual(got, []string{vol, "empty-vol"}) {
		// sorted: "empty-vol" < "inode-..."? compare lexically
		want := []string{"empty-vol", vol}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Volumes after reload = %v", got)
		}
	}
}

func TestStore_OpenSkipsNonMatchingAndSetsAsideDamagedFiles(t *testing.T) {
	dir := t.TempDir()
	// A valid map, hand-written.
	good := `{"volume_id":"v-good","entries":[{"key":0,"value":"c0","ts":{"wall":1,"logical":0,"node":"n"}}]}`
	mustWrite(t, filepath.Join(dir, "good"+".emap.json"), good)
	// Truncated JSON and an empty volume id are damaged maps of decodable volumes.
	mustWrite(t, emapFile(dir, "v-trunc"), `{"volume_id":"v-trunc","entr`)
	mustWrite(t, emapFile(dir, "v-noid"), `{"volume_id":"","entries":[]}`)
	// A damaged file whose name is not a volume id is left alone.
	mustWrite(t, filepath.Join(dir, "a.emap.json"), "{not json")
	mustWrite(t, filepath.Join(dir, "unrelated.txt"), "ignore me")
	mustWrite(t, filepath.Join(dir, ".emap.json"), "too short a name")
	if err := os.Mkdir(filepath.Join(dir, "sub.emap.json"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := s.Volumes(); !reflect.DeepEqual(got, []string{"v-good"}) {
		t.Errorf("Volumes = %v, want [v-good] (others skipped)", got)
	}
	if id, ok := s.Get("v-good", 0); !ok || id != "c0" {
		t.Errorf("good map not loaded: (%q,%v)", id, ok)
	}
	for _, vol := range []string{"v-trunc", "v-noid"} {
		if !s.Lost(vol) {
			t.Errorf("%s: a damaged map must flag its volume lost", vol)
		}
		if _, err := os.Stat(emapFile(dir, vol)); !os.IsNotExist(err) {
			t.Errorf("%s: the damaged file should have been moved, stat err = %v", vol, err)
		}
		if _, err := os.Stat(emapFile(dir, vol) + ".corrupt"); err != nil {
			t.Errorf("%s: the damaged file should be kept as .corrupt: %v", vol, err)
		}
	}
	if s.Lost("v-good") {
		t.Error("a healthy map must not be flagged lost")
	}
	if _, err := os.Stat(filepath.Join(dir, "a.emap.json")); err != nil {
		t.Errorf("an undecodable name should be left in place: %v", err)
	}
}

// The scenario from the field: a crash leaves a truncated map. The damaged bytes
// are kept verbatim under .corrupt, and a restart before the volume is rebuilt
// still reports it lost without ever loading the .corrupt file as a map.
func TestStore_TruncatedMapIsKeptAndNotReloaded(t *testing.T) {
	dir := t.TempDir()
	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Set("pg-data", 0, "c0", ts(1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	full, err := os.ReadFile(emapFile(dir, "pg-data"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	torn := full[:len(full)/2]
	mustWrite(t, emapFile(dir, "pg-data"), string(torn))

	first, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	if !first.Lost("pg-data") || first.Has("pg-data") {
		t.Fatalf("Lost=%v Has=%v, want the volume lost and its map not loaded", first.Lost("pg-data"), first.Has("pg-data"))
	}
	kept, err := os.ReadFile(emapFile(dir, "pg-data") + ".corrupt")
	if err != nil || string(kept) != string(torn) {
		t.Errorf("the .corrupt file should hold the damaged bytes unchanged: %q, %v", kept, err)
	}

	second, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	if second.Has("pg-data") || len(second.Volumes()) != 0 {
		t.Errorf("a .corrupt file must never load as a map: has=%v volumes=%v", second.Has("pg-data"), second.Volumes())
	}
	if !second.Lost("pg-data") {
		t.Error("a restart before the rebuild must keep the volume lost, or Warm would serve a single older replica")
	}
}

// Once a rebuilt map exists, the leftover .corrupt file is only kept for
// inspection and must not hold the volume hostage.
func TestStore_CorruptBesideRebuiltMapDoesNotFlag(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, emapFile(dir, "vol"), `{"volume_id":"vol","entries":[{"key":0,"value":"c0","ts":{"wall":1,"logical":0,"node":"n"}}]}`)
	mustWrite(t, emapFile(dir, "vol")+".corrupt", `{"volume_id":"vol","entr`)
	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if s.Lost("vol") {
		t.Error("a .corrupt file beside a good map must not flag the volume")
	}
	if id, ok := s.Get("vol", 0); !ok || id != "c0" {
		t.Errorf("the rebuilt map should load: (%q,%v)", id, ok)
	}
}

// A volume damaged on this boot while an older .corrupt file is still around is
// flagged once, and an undecodable .corrupt name is skipped.
func TestStore_CorruptLeftoversEdgeCases(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, emapFile(dir, "vol"), "{")
	mustWrite(t, emapFile(dir, "vol")+".corrupt", "{older")
	mustWrite(t, filepath.Join(dir, "a.emap.json.corrupt"), "{")
	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !s.Lost("vol") || s.Has("vol") {
		t.Errorf("Lost=%v Has=%v, want a lost volume with no map", s.Lost("vol"), s.Has("vol"))
	}
	if got := s.Volumes(); len(got) != 0 {
		t.Errorf("Volumes = %v, want none", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.emap.json.corrupt")); err != nil {
		t.Errorf("an undecodable .corrupt file should be left in place: %v", err)
	}
}

func TestStore_OpenRemovesStaleTempFiles(t *testing.T) {
	dir := t.TempDir()
	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Set("vol", 0, "old", ts(1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// A crash between writing the temp file and renaming it leaves this behind.
	mustWrite(t, emapFile(dir, "vol")+".tmp", `{"volume_id":"vol","entries":[{"key":0,"value":"ne`)

	reopened, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if id, ok := reopened.Get("vol", 0); !ok || id != "old" {
		t.Errorf("the committed map should load: (%q,%v)", id, ok)
	}
	if reopened.Lost("vol") {
		t.Error("a leftover temp file is not damage to the committed map")
	}
	if _, err := os.Stat(emapFile(dir, "vol") + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("the stale temp file should be removed, stat err = %v", err)
	}
}

func TestStore_PersistFailureIsReturned(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test is meaningless as root")
	}
	for _, tc := range []struct {
		name string
		op   func(*extentmap.Store) error
	}{
		{"SetBatch", func(s *extentmap.Store) error {
			return s.SetBatch("vol", []uint64{0}, []string{"c0"}, ts(1))
		}},
		{"Merge", func(s *extentmap.Store) error {
			return s.Merge("vol", []crdt.MapEntry[uint64, string]{{Key: 0, Value: "c0", TS: ts(1)}})
		}},
		{"Ensure", func(s *extentmap.Store) error { return s.Ensure("vol") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := extentmap.Open(dir, discard())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := os.Chmod(dir, 0o500); err != nil {
				t.Fatalf("chmod: %v", err)
			}
			defer func() { _ = os.Chmod(dir, 0o700) }()

			if err := tc.op(s); err == nil {
				t.Fatal("a failed save must be returned so the caller does not count it as durable")
			}
			if !s.Has("vol") {
				t.Error("the change should still apply in memory")
			}
		})
	}
}

func TestStore_LostAndClearLost(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, emapFile(dir, "vol"), "")
	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !s.Lost("vol") || s.Lost("other") {
		t.Fatalf("Lost(vol)=%v Lost(other)=%v", s.Lost("vol"), s.Lost("other"))
	}
	// A peer pushing its copy in does not make the volume whole again.
	if err := s.Merge("vol", []crdt.MapEntry[uint64, string]{{Key: 0, Value: "c", TS: ts(1)}}); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if !s.Has("vol") || !s.Lost("vol") {
		t.Errorf("after a single merge: Has=%v Lost=%v, want both true", s.Has("vol"), s.Lost("vol"))
	}
	s.ClearLost("vol")
	if s.Lost("vol") {
		t.Error("ClearLost should clear the flag")
	}
	if _, err := os.Stat(emapFile(dir, "vol") + ".corrupt"); err != nil {
		t.Errorf("ClearLost must leave the .corrupt file for inspection: %v", err)
	}
}

func TestStore_DeleteClearsLostAndCorruptFile(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, emapFile(dir, "vol"), "{")
	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Delete("vol"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if s.Lost("vol") {
		t.Error("Delete should clear the lost flag")
	}
	if _, err := os.Stat(emapFile(dir, "vol") + ".corrupt"); !os.IsNotExist(err) {
		t.Errorf("Delete should remove the .corrupt file, stat err = %v", err)
	}
}

func TestStore_ReapLostVolumes(t *testing.T) {
	dir := t.TempDir()
	for _, vol := range []string{"lost-old", "lost-young", "lost-live", "lost-vanished"} {
		mustWrite(t, emapFile(dir, vol), "{")
	}
	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(emapFile(dir, "lost-old")+".corrupt", past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if err := os.Remove(emapFile(dir, "lost-vanished") + ".corrupt"); err != nil {
		t.Fatalf("remove: %v", err)
	}

	reaped, err := s.Reap(map[string]struct{}{"lost-live": {}}, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if !reflect.DeepEqual(reaped, []string{"lost-old", "lost-vanished"}) {
		t.Errorf("reaped = %v, want [lost-old lost-vanished]", reaped)
	}
	if s.Lost("lost-old") || s.Lost("lost-vanished") || !s.Lost("lost-young") || !s.Lost("lost-live") {
		t.Error("Reap should clear the flag of exactly the reaped volumes")
	}
	if _, err := os.Stat(emapFile(dir, "lost-old") + ".corrupt"); !os.IsNotExist(err) {
		t.Errorf("a reaped volume's .corrupt file should be removed, stat err = %v", err)
	}
}

func TestStore_Delete(t *testing.T) {
	dir := t.TempDir()
	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	vol := "inode-1.0.node/odd:chars" // exercises the encoded filename
	s.Set(vol, 0, "c0", ts(1))
	if _, statErr := os.Stat(emapFile(dir, vol)); statErr != nil {
		t.Fatalf("expected a persisted file before delete: %v", statErr)
	}

	if err := s.Delete(vol); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if s.Has(vol) {
		t.Error("Delete should drop the in-memory map")
	}
	if _, statErr := os.Stat(emapFile(dir, vol)); !os.IsNotExist(statErr) {
		t.Errorf("Delete should remove the persisted file, stat err = %v", statErr)
	}

	// Idempotent: deleting an already-gone volume is a no-op.
	if err := s.Delete(vol); err != nil {
		t.Errorf("Delete of an unknown volume should be a no-op, got %v", err)
	}

	// Delete on an in-memory store drops the map and never touches disk.
	mem := extentmap.New(discard())
	mem.Set("v", 0, "c", ts(1))
	if err := mem.Delete("v"); err != nil || mem.Has("v") {
		t.Errorf("in-memory Delete failed: err=%v has=%v", err, mem.Has("v"))
	}
}

func TestStore_Reap(t *testing.T) {
	dir := t.TempDir()
	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.Set("live", 0, "c", ts(1))
	s.Set("orphan-old", 0, "c", ts(1))
	s.Set("orphan-young", 0, "c", ts(1))

	past := time.Now().Add(-2 * time.Hour)
	recent := time.Now()
	if err := os.Chtimes(emapFile(dir, "orphan-old"), past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if err := os.Chtimes(emapFile(dir, "orphan-young"), recent, recent); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	live := map[string]struct{}{"live": {}}
	reaped, err := s.Reap(live, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if !reflect.DeepEqual(reaped, []string{"orphan-old"}) {
		t.Errorf("reaped = %v, want [orphan-old]", reaped)
	}
	// In the live set: kept. Too young: kept. Old orphan: gone (memory + disk).
	if !s.Has("live") || !s.Has("orphan-young") || s.Has("orphan-old") {
		t.Errorf("after reap: live=%v young=%v old=%v", s.Has("live"), s.Has("orphan-young"), s.Has("orphan-old"))
	}
	if _, statErr := os.Stat(emapFile(dir, "orphan-old")); !os.IsNotExist(statErr) {
		t.Errorf("a reaped map's file should be removed, stat err = %v", statErr)
	}

	// An in-memory store has no file age to judge by, so it reaps nothing.
	mem := extentmap.New(discard())
	mem.Set("x", 0, "c", ts(1))
	if got, err := mem.Reap(map[string]struct{}{}, time.Now()); err != nil || got != nil {
		t.Errorf("in-memory Reap should be a no-op: reaped=%v err=%v", got, err)
	}
	if !mem.Has("x") {
		t.Error("in-memory Reap must not drop maps")
	}
}

func TestStore_ReapDropsEntryWhoseFileVanished(t *testing.T) {
	dir := t.TempDir()
	s, err := extentmap.Open(dir, discard())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.Set("gone", 0, "c", ts(1))
	if err := os.Remove(emapFile(dir, "gone")); err != nil { // file vanishes out of band
		t.Fatalf("remove: %v", err)
	}
	reaped, err := s.Reap(map[string]struct{}{}, time.Now())
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if !reflect.DeepEqual(reaped, []string{"gone"}) {
		t.Errorf("reaped = %v, want [gone]", reaped)
	}
	if s.Has("gone") {
		t.Error("a stale in-memory entry whose file vanished should be dropped")
	}
}

func TestStore_OpenMkdirAllError(t *testing.T) {
	// A regular file in the path where a directory is expected makes MkdirAll fail.
	file := filepath.Join(t.TempDir(), "iam-a-file")
	mustWrite(t, file, "x")
	if _, err := extentmap.Open(filepath.Join(file, "sub"), discard()); err == nil {
		t.Error("Open should fail when the data dir cannot be created")
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
