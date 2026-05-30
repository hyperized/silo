package crypto_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperized/silo/internal/crypto"
)

func TestStaticKeyProvider(t *testing.T) {
	key := make([]byte, crypto.ClusterKeyBytes)
	p := crypto.StaticKeyProvider(key)
	if p.SourceName() != "static" {
		t.Errorf("source = %q", p.SourceName())
	}
	got, err := p.ClusterKey()
	if err != nil || len(got) != crypto.ClusterKeyBytes {
		t.Errorf("ClusterKey = (%d bytes, %v)", len(got), err)
	}
}

func TestFileKeyProvider(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "key")
	if err := os.WriteFile(good, make([]byte, crypto.ClusterKeyBytes), 0o400); err != nil {
		t.Fatal(err)
	}
	p := crypto.FileKeyProvider(good)
	if p.SourceName() != "file" {
		t.Errorf("source = %q", p.SourceName())
	}
	key, err := p.ClusterKey()
	if err != nil || len(key) != crypto.ClusterKeyBytes {
		t.Fatalf("ClusterKey = (%d, %v), want a 32-byte key", len(key), err)
	}
	// The key actually builds a cipher (end-to-end check that the source works).
	if _, err := crypto.NewCipher(key); err != nil {
		t.Errorf("NewCipher from file key: %v", err)
	}

	// A missing file is an actionable error.
	if _, err := crypto.FileKeyProvider(filepath.Join(dir, "nope")).ClusterKey(); err == nil {
		t.Error("a missing key file should error")
	}

	// A wrong-length file is rejected (e.g. someone base64-encoded it).
	wrong := filepath.Join(dir, "wrong")
	if err := os.WriteFile(wrong, make([]byte, 44), 0o400); err != nil {
		t.Fatal(err)
	}
	if _, err := crypto.FileKeyProvider(wrong).ClusterKey(); err == nil {
		t.Error("a wrong-length key file should error")
	}
}
