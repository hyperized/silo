package chunkstore_test

import (
	"context"
	"crypto/rand"
	"strconv"
	"testing"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crypto"
)

func newBenchStore(b *testing.B) *chunkstore.FileStore {
	b.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		b.Fatal(err)
	}
	cipher, err := crypto.NewCipher(key)
	if err != nil {
		b.Fatal(err)
	}
	fs, err := chunkstore.NewFileStore(b.TempDir(), cipher)
	if err != nil {
		b.Fatal(err)
	}
	return fs
}

// BenchmarkFileStorePut measures the full local write: encrypt + atomic temp
// write + fsync(file) + rename + fsync(dir). This is the per-replica durability
// cost on the write path; the two fsyncs dominate on real disks (b.TempDir may
// be tmpfs in CI, where it instead reports the crypto+syscall floor).
func BenchmarkFileStorePut(b *testing.B) {
	fs := newBenchStore(b)
	data := make([]byte, 4<<20)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := fs.Put(ctx, "bench"+strconv.Itoa(i), data); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFileStoreGet measures the local read: ReadFile + decrypt.
func BenchmarkFileStoreGet(b *testing.B) {
	fs := newBenchStore(b)
	data := make([]byte, 4<<20)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if _, err := fs.Put(ctx, "bench", data); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, err := fs.Get(ctx, "bench"); err != nil {
			b.Fatal(err)
		}
	}
}
