package chunkstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crypto"
)

func newTestStore(t *testing.T) (*chunkstore.FileStore, string) {
	t.Helper()
	key := make([]byte, crypto.ClusterKeyBytes)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	c, err := crypto.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	dir := t.TempDir()
	fs, err := chunkstore.NewFileStore(dir, c)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	return fs, dir
}

func TestFileStore_List(t *testing.T) {
	fs, dir := newTestStore(t)
	ctx := context.Background()
	for _, id := range []string{"alpha", "beta", "gamma"} {
		if _, err := fs.Put(ctx, id, []byte(id)); err != nil {
			t.Fatalf("Put %s: %v", id, err)
		}
	}
	// A half-written temp file and a non-chunk file must both be ignored.
	if err := os.WriteFile(filepath.Join(dir, "partial.chunk.tmp"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write txt: %v", err)
	}
	// A subdirectory must be skipped, not mistaken for a chunk.
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	ids, err := fs.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	for _, want := range []string{"alpha", "beta", "gamma"} {
		if !got[want] {
			t.Errorf("List missing %q; got %v", want, ids)
		}
	}
	if len(ids) != 3 {
		t.Errorf("List returned %d ids, want 3 (tmp + non-chunk excluded): %v", len(ids), ids)
	}
}

func TestFileStore_ListReadDirError(t *testing.T) {
	fs, dir := newTestStore(t)
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("rm: %v", err)
	}
	if _, err := fs.List(context.Background()); err == nil {
		t.Fatal("List should error when the data directory is gone")
	}
}

// TestStore_InterfaceContract pins the public surface: FileStore must
// satisfy Store and round-trip via the interface, not just the struct.
func TestStore_InterfaceContract(t *testing.T) {
	key := make([]byte, crypto.ClusterKeyBytes)
	_, _ = rand.Read(key)
	c, _ := crypto.NewCipher(key)
	fs, err := chunkstore.NewFileStore(t.TempDir(), c)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	var s chunkstore.Store = fs

	ctx := context.Background()
	payload := []byte("through the interface")
	if _, err := s.Put(ctx, "via-iface", payload); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, _, err := s.Get(ctx, "via-iface")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch via Store interface")
	}
}

func TestValidateID_Accepts(t *testing.T) {
	for _, id := range []string{
		"a",
		"with-dash",
		"with_underscore",
		"MixedCase123",
		"0123456789",
	} {
		if err := chunkstore.ValidateID(id); err != nil {
			t.Errorf("ValidateID(%q): got %v, want nil", id, err)
		}
	}
}

func TestFileStore_RawChunk(t *testing.T) {
	fs, _ := newTestStore(t)
	ctx := context.Background()
	plaintext := []byte("the secret chunk bytes")
	if _, err := fs.Put(ctx, "raw-1", plaintext); err != nil {
		t.Fatalf("Put: %v", err)
	}

	raw, err := fs.RawChunk(ctx, "raw-1")
	if err != nil {
		t.Fatalf("RawChunk: %v", err)
	}
	// The raw on-disk bytes are the encrypted envelope, not the plaintext.
	if len(raw) == 0 || string(raw) == string(plaintext) {
		t.Errorf("RawChunk returned plaintext or nothing (%d bytes)", len(raw))
	}

	// Missing chunk and invalid id both error.
	if _, err := fs.RawChunk(ctx, "does-not-exist"); err == nil {
		t.Error("RawChunk of a missing chunk should error")
	}
	if _, err := fs.RawChunk(ctx, "bad/id"); err == nil {
		t.Error("RawChunk of an invalid id should error")
	}
}
