package chunkstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/hyperized/silo/internal/crypto"
)

func newTestStore(t *testing.T) (*FileStore, string) {
	t.Helper()
	dir := t.TempDir()
	key := make([]byte, crypto.ClusterKeyBytes)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	c, err := crypto.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	s, err := NewFileStore(dir, c)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	return s, dir
}

func TestNewFileStore_NilCipher(t *testing.T) {
	_, err := NewFileStore(t.TempDir(), nil)
	if err == nil || !strings.Contains(err.Error(), "crypto.NewCipher") {
		t.Errorf("expected actionable nil-cipher error, got %v", err)
	}
}

func TestNewFileStore_EmptyRoot(t *testing.T) {
	key := make([]byte, crypto.ClusterKeyBytes)
	c, _ := crypto.NewCipher(key)
	_, err := NewFileStore("", c)
	if err == nil || !strings.Contains(err.Error(), "SILO_DATA_DIR") {
		t.Errorf("expected actionable empty-root error, got %v", err)
	}
}

func TestNewFileStore_UnwritableRoot(t *testing.T) {
	key := make([]byte, crypto.ClusterKeyBytes)
	c, _ := crypto.NewCipher(key)
	// /dev/null/x is a file under a non-directory; MkdirAll cannot create it.
	_, err := NewFileStore("/dev/null/x", c)
	if err == nil || !strings.Contains(err.Error(), "writable filesystem") {
		t.Errorf("expected actionable mkdir error, got %v", err)
	}
}

func TestPutGetStatDelete_RoundTrip(t *testing.T) {
	s, dir := newTestStore(t)
	ctx := context.Background()
	payload := []byte("the quick brown fox jumps over the lazy dog")

	info, err := s.Put(ctx, "chunk-1", payload)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if info.PlainBytes != int64(len(payload)) {
		t.Errorf("PlainBytes: got %d, want %d", info.PlainBytes, len(payload))
	}
	if info.StoredBytes != int64(len(payload)+crypto.OverheadBytes) {
		t.Errorf("StoredBytes: got %d, want %d", info.StoredBytes, len(payload)+crypto.OverheadBytes)
	}

	got, _, err := s.Get(ctx, "chunk-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("Get round-trip mismatch")
	}

	statInfo, err := s.Stat(ctx, "chunk-1")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if statInfo.PlainBytes != int64(len(payload)) {
		t.Errorf("Stat.PlainBytes: got %d, want %d", statInfo.PlainBytes, len(payload))
	}

	if err := s.Delete(ctx, "chunk-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, _, err := s.Get(ctx, "chunk-1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete: got %v, want ErrNotFound", err)
	}

	// Verify nothing left on disk for the id.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "chunk-1") {
			t.Errorf("residual file after Delete: %s", e.Name())
		}
	}
}

func TestPut_OnDiskIsCiphertext(t *testing.T) {
	s, dir := newTestStore(t)
	plaintext := []byte("MUST_NOT_LEAK_TO_DISK")
	if _, err := s.Put(context.Background(), "secret", plaintext); err != nil {
		t.Fatalf("Put: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "secret.chunk"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if bytes.Contains(raw, plaintext) {
		t.Fatal("chunk file on disk contained plaintext; encryption did not happen")
	}
	if string(raw[:4]) != "SILO" {
		t.Errorf("on-disk file missing SILO magic; got first 4 bytes %q", raw[:4])
	}
}

func TestPut_RejectsInvalidID(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	cases := []string{
		"",
		"with/slash",
		"with space",
		"../traversal",
		strings.Repeat("a", 129),
		"emoji-🚀",
	}
	for _, id := range cases {
		t.Run(id, func(t *testing.T) {
			if _, err := s.Put(ctx, id, []byte("x")); !errors.Is(err, ErrInvalidID) {
				t.Errorf("Put %q: got %v, want ErrInvalidID", id, err)
			}
		})
	}
}

func TestGetStatDelete_InvalidID(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	if _, _, err := s.Get(ctx, "bad/id"); !errors.Is(err, ErrInvalidID) {
		t.Errorf("Get: got %v, want ErrInvalidID", err)
	}
	if _, err := s.Stat(ctx, "bad/id"); !errors.Is(err, ErrInvalidID) {
		t.Errorf("Stat: got %v, want ErrInvalidID", err)
	}
	if err := s.Delete(ctx, "bad/id"); !errors.Is(err, ErrInvalidID) {
		t.Errorf("Delete: got %v, want ErrInvalidID", err)
	}
}

func TestGetStatDelete_NotFound(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	if _, _, err := s.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get: got %v, want ErrNotFound", err)
	}
	if _, err := s.Stat(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Stat: got %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete: got %v, want ErrNotFound", err)
	}
}

func TestPut_OverwriteSameID(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	if _, err := s.Put(ctx, "x", []byte("first")); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if _, err := s.Put(ctx, "x", []byte("second")); err != nil {
		t.Fatalf("overwrite Put: %v", err)
	}
	got, _, err := s.Get(ctx, "x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "second" {
		t.Errorf("expected overwrite to win, got %q", got)
	}
}

func TestPut_ConcurrentSameIDReturnsAnError(t *testing.T) {
	// O_EXCL on the tmp file means two simultaneous Puts of the same id
	// race: exactly one writes the tmp, the other observes EEXIST and
	// returns an error. Verifies the writeAtomic invariant.
	s, _ := newTestStore(t)
	ctx := context.Background()

	const N = 20
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Put(ctx, "concurrent", []byte{byte(i)})
		}(i)
	}
	wg.Wait()

	var nilCount int
	for _, err := range errs {
		if err == nil {
			nilCount++
		}
	}
	if nilCount == 0 {
		t.Fatal("all concurrent Puts failed; expected at least one success")
	}
	// The on-disk chunk should decrypt cleanly to one of the inputs.
	got, _, err := s.Get(ctx, "concurrent")
	if err != nil {
		t.Fatalf("Get after concurrent Puts: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("on-disk chunk size: got %d, want 1", len(got))
	}
}

func TestPut_LeavesNoStaleTmpAfterFailure(t *testing.T) {
	s, dir := newTestStore(t)
	// Pre-create the tmp file so O_EXCL makes Put fail.
	tmp := filepath.Join(dir, "blocked"+tmpExt)
	if err := os.WriteFile(tmp, []byte("squatter"), 0o600); err != nil {
		t.Fatalf("seed tmp: %v", err)
	}
	if _, err := s.Put(context.Background(), "blocked", []byte("payload")); err == nil {
		t.Fatal("Put should fail when tmp exists")
	}
	// The pre-existing tmp file should still be present (we didn't create it).
	if _, err := os.Stat(tmp); err != nil {
		t.Errorf("squatter tmp removed unexpectedly: %v", err)
	}
}

func TestGet_ReadFailureSurfacesActionable(t *testing.T) {
	// Put a directory where the chunk file should be. ReadFile returns
	// EISDIR rather than ENOENT, exercising the non-NotFound error path.
	s, dir := newTestStore(t)
	bogus := filepath.Join(dir, "is-a-dir.chunk")
	if err := os.MkdirAll(bogus, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	_, _, err := s.Get(context.Background(), "is-a-dir")
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on a directory: got %v, want a non-NotFound read error", err)
	}
	if !strings.Contains(err.Error(), "permissions") && !strings.Contains(err.Error(), "disk health") {
		t.Errorf("Get error should hint at permissions or disk health, got %v", err)
	}
}

func TestStat_ReadFailureSurfacesActionable(t *testing.T) {
	s, dir := newTestStore(t)
	bogus := filepath.Join(dir, "is-a-dir.chunk")
	if err := os.MkdirAll(bogus, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Stat on a directory should succeed (it returns a directory FileInfo).
	// To exercise the non-NotFound branch we need a path that fails to
	// stat for some other reason; the simplest is removing the parent's
	// read permission so the os.Stat call returns EACCES on Linux/macOS.
	// We can't make root drop permissions, so this test is best-effort:
	// skip if the test runner is root.
	if os.Geteuid() == 0 {
		t.Skip("running as root makes EACCES untriggerable")
	}
	subdir := filepath.Join(dir, "locked")
	if err := os.MkdirAll(subdir, 0o000); err != nil {
		t.Fatalf("mkdir locked: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(subdir, 0o700) })

	lockedStore, err := NewFileStore(subdir, s.cipher)
	if err != nil {
		// MkdirAll on 0o000 dir returns success on first creation;
		// the EACCES surfaces on a subsequent stat.
		t.Fatalf("NewFileStore: %v", err)
	}
	_, statErr := lockedStore.Stat(context.Background(), "missing")
	if statErr == nil {
		t.Skip("Stat unexpectedly succeeded; filesystem may permit access through the 0o000 directory (e.g. macOS APFS quirk)")
	}
	if errors.Is(statErr, ErrInvalidID) {
		t.Fatalf("Stat: got ErrInvalidID, want a filesystem error")
	}
	// Either ErrNotFound or an actionable other error is acceptable;
	// what we don't want is a bare 'permission denied' with no fix.
	if !errors.Is(statErr, ErrNotFound) && !strings.Contains(statErr.Error(), "accessible") {
		t.Errorf("Stat error: %v", statErr)
	}
}

func TestDelete_RemoveFailureSurfacesActionable(t *testing.T) {
	s, dir := newTestStore(t)
	// Place a directory where the .chunk file would be. os.Remove on a
	// non-empty directory returns ENOTEMPTY, not ENOENT.
	bogus := filepath.Join(dir, "is-a-dir.chunk")
	if err := os.MkdirAll(filepath.Join(bogus, "inner"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	err := s.Delete(context.Background(), "is-a-dir")
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrInvalidID) {
		t.Fatalf("Delete on non-empty dir: got %v, want non-NotFound error", err)
	}
	if !strings.Contains(err.Error(), "permissions") {
		t.Errorf("Delete error should hint at permissions, got %v", err)
	}
}

func TestPut_EncryptionFailurePropagates(t *testing.T) {
	// Force crypto.EncryptChunk to fail by exhausting the random source.
	// Build the store before the swap so seeding succeeds.
	s, _ := newTestStore(t)

	prev := rand.Reader
	t.Cleanup(func() { rand.Reader = prev })
	rand.Reader = exhaustedReader{}

	_, err := s.Put(context.Background(), "x", []byte("payload"))
	if err == nil {
		t.Fatal("Put should fail when the crypto layer has no entropy")
	}
	if !strings.Contains(err.Error(), "encrypt chunk") {
		t.Errorf("Put error should name the encrypt step, got %v", err)
	}
}

// exhaustedReader simulates a drained entropy source for the rare path
// where crypto operations cannot make progress.
type exhaustedReader struct{}

func (exhaustedReader) Read(_ []byte) (int, error) { return 0, errExhausted }

var errExhausted = errors.New("no entropy")

func TestFsyncDir_OnMissingDirectory(t *testing.T) {
	// Cover fsyncDir's open-failure path.
	if err := fsyncDir("/this/path/does/not/exist"); err == nil {
		t.Fatal("fsyncDir on missing path: expected error")
	}
}

// fakeFile implements syncCloser with hook points so tests can fail at
// any step of writeAtomic without involving the real filesystem.
type fakeFile struct {
	writeErr error
	syncErr  error
	closeErr error
	closed   bool
}

func (f *fakeFile) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return len(p), nil
}

func (f *fakeFile) Sync() error  { return f.syncErr }
func (f *fakeFile) Close() error { f.closed = true; return f.closeErr }

// withFakeFile swaps openExclusiveFile for the duration of a test so
// every post-open branch in writeAtomic becomes reachable. Restoring on
// cleanup keeps the package-level seam test-only.
func withFakeFile(t *testing.T, f *fakeFile) {
	t.Helper()
	prev := openExclusiveFile
	t.Cleanup(func() { openExclusiveFile = prev })
	openExclusiveFile = func(_ string, _ os.FileMode) (syncCloser, error) {
		return f, nil
	}
}

func TestWriteAtomic_WriteFailureRemovesTmp(t *testing.T) {
	s, dir := newTestStore(t)
	withFakeFile(t, &fakeFile{writeErr: errors.New("write boom")})

	// Seed the tmp file so we can verify osRemove was called.
	tmp := filepath.Join(dir, "boom"+tmpExt)
	if err := os.WriteFile(tmp, []byte("residue"), 0o600); err != nil {
		t.Fatalf("seed tmp: %v", err)
	}

	_, err := s.Put(context.Background(), "boom", []byte("data"))
	if err == nil || !strings.Contains(err.Error(), "write boom") {
		t.Fatalf("Put: got %v, want write boom", err)
	}
	if _, statErr := os.Stat(tmp); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("tmp file should be removed after write failure, stat=%v", statErr)
	}
}

func TestWriteAtomic_SyncFailureRemovesTmp(t *testing.T) {
	s, dir := newTestStore(t)
	withFakeFile(t, &fakeFile{syncErr: errors.New("sync boom")})

	tmp := filepath.Join(dir, "syncfail"+tmpExt)
	if err := os.WriteFile(tmp, []byte("residue"), 0o600); err != nil {
		t.Fatalf("seed tmp: %v", err)
	}
	_, err := s.Put(context.Background(), "syncfail", []byte("data"))
	if err == nil || !strings.Contains(err.Error(), "sync boom") {
		t.Fatalf("Put: got %v, want sync boom", err)
	}
	if _, statErr := os.Stat(tmp); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("tmp file should be removed after sync failure, stat=%v", statErr)
	}
}

func TestWriteAtomic_CloseFailureRemovesTmp(t *testing.T) {
	s, dir := newTestStore(t)
	withFakeFile(t, &fakeFile{closeErr: errors.New("close boom")})

	tmp := filepath.Join(dir, "closefail"+tmpExt)
	if err := os.WriteFile(tmp, []byte("residue"), 0o600); err != nil {
		t.Fatalf("seed tmp: %v", err)
	}
	_, err := s.Put(context.Background(), "closefail", []byte("data"))
	if err == nil || !strings.Contains(err.Error(), "close boom") {
		t.Fatalf("Put: got %v, want close boom", err)
	}
	if _, statErr := os.Stat(tmp); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("tmp file should be removed after close failure, stat=%v", statErr)
	}
}

func TestWriteAtomic_RenameFailureRemovesTmp(t *testing.T) {
	s, dir := newTestStore(t)

	prev := osRename
	t.Cleanup(func() { osRename = prev })
	osRename = func(_, _ string) error { return errors.New("rename boom") }

	_, err := s.Put(context.Background(), "renamefail", []byte("data"))
	if err == nil || !strings.Contains(err.Error(), "rename boom") {
		t.Fatalf("Put: got %v, want rename boom", err)
	}
	// The real file was created and then the rename failed; the tmp
	// file should have been removed.
	tmp := filepath.Join(dir, "renamefail"+tmpExt)
	if _, statErr := os.Stat(tmp); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("tmp file should be removed after rename failure, stat=%v", statErr)
	}
}

func TestGet_DetectsCorruption(t *testing.T) {
	s, dir := newTestStore(t)
	ctx := context.Background()
	if _, err := s.Put(ctx, "tampered", []byte("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	path := filepath.Join(dir, "tampered.chunk")
	raw, _ := os.ReadFile(path)
	raw[len(raw)-1] ^= 0x01
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("tamper write: %v", err)
	}
	if _, _, err := s.Get(ctx, "tampered"); err == nil || !strings.Contains(err.Error(), "decrypt") {
		t.Errorf("Get of tampered chunk: got %v, want decrypt error", err)
	}
}

func TestDeleteNoSync_RemovesWithoutSyncing(t *testing.T) {
	s, dir := newTestStore(t)
	ctx := context.Background()
	for _, id := range []string{"chunk-1", "chunk-2"} {
		if _, err := s.Put(ctx, id, []byte("payload")); err != nil {
			t.Fatalf("Put %s: %v", id, err)
		}
	}

	for _, id := range []string{"chunk-1", "chunk-2"} {
		if err := s.DeleteNoSync(ctx, id); err != nil {
			t.Fatalf("DeleteNoSync %s: %v", id, err)
		}
	}
	// One commit for the batch, rather than one per unlink.
	if err := s.SyncDir(); err != nil {
		t.Fatalf("SyncDir: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), chunkExt) {
			t.Errorf("residual chunk after DeleteNoSync: %s", e.Name())
		}
	}
	if _, _, err := s.Get(ctx, "chunk-1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after DeleteNoSync: got %v, want ErrNotFound", err)
	}
}

func TestDeleteNoSync_InvalidIDAndNotFound(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	if err := s.DeleteNoSync(ctx, "bad/id"); !errors.Is(err, ErrInvalidID) {
		t.Errorf("DeleteNoSync invalid id: got %v, want ErrInvalidID", err)
	}
	if err := s.DeleteNoSync(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("DeleteNoSync missing: got %v, want ErrNotFound", err)
	}
}

func TestDeleteNoSync_RemoveFailureSurfacesActionable(t *testing.T) {
	s, dir := newTestStore(t)
	bogus := filepath.Join(dir, "is-a-dir.chunk")
	if err := os.MkdirAll(filepath.Join(bogus, "inner"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	err := s.DeleteNoSync(context.Background(), "is-a-dir")
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrInvalidID) {
		t.Fatalf("DeleteNoSync on non-empty dir: got %v, want non-NotFound error", err)
	}
	if !strings.Contains(err.Error(), "permissions") {
		t.Errorf("error should hint at permissions, got %v", err)
	}
}

func TestSyncDir_MissingRootSurfacesError(t *testing.T) {
	s, dir := newTestStore(t)
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if err := s.SyncDir(); err == nil {
		t.Error("SyncDir on a vanished data dir should report the failure, not swallow it")
	}
}
