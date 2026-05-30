package crypto_test

import (
	"context"
	"fmt"
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

type fakeDecrypter struct {
	out []byte
	err error
}

func (f fakeDecrypter) Decrypt(_ context.Context, _ []byte) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.out, nil
}
func (fakeDecrypter) Name() string { return "fake-kms" }

func TestKMSKeyProvider(t *testing.T) {
	dir := t.TempDir()
	wrapped := filepath.Join(dir, "wrapped.key")
	if err := os.WriteFile(wrapped, []byte("ciphertext-blob"), 0o400); err != nil {
		t.Fatal(err)
	}

	key := make([]byte, crypto.ClusterKeyBytes)
	p := crypto.KMSKeyProvider(fakeDecrypter{out: key}, wrapped)
	if p.SourceName() != "fake-kms" {
		t.Errorf("source = %q", p.SourceName())
	}
	got, err := p.ClusterKey()
	if err != nil || len(got) != crypto.ClusterKeyBytes {
		t.Fatalf("ClusterKey = (%d, %v)", len(got), err)
	}

	// Missing path.
	if _, err := crypto.KMSKeyProvider(fakeDecrypter{out: key}, "").ClusterKey(); err == nil {
		t.Error("empty ciphertext path should error")
	}
	// Missing file.
	if _, err := crypto.KMSKeyProvider(fakeDecrypter{out: key}, filepath.Join(dir, "nope")).ClusterKey(); err == nil {
		t.Error("missing wrapped-key file should error")
	}
	// Decrypt failure.
	if _, err := crypto.KMSKeyProvider(fakeDecrypter{err: errFake}, wrapped).ClusterKey(); err == nil {
		t.Error("a KMS decrypt failure should error")
	}
	// Wrong plaintext length.
	if _, err := crypto.KMSKeyProvider(fakeDecrypter{out: []byte("short")}, wrapped).ClusterKey(); err == nil {
		t.Error("a non-32-byte decrypted key should error")
	}
}

var errFake = fmt.Errorf("kms unavailable")
