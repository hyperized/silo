package crypto_test

import (
	"crypto/rand"
	"testing"

	"github.com/hyperized/silo/internal/crypto"
)

// chunkSizes spans the small-write and default-chunk cases so the per-byte
// throughput (large) and the fixed per-chunk overhead (small) are both visible.
var chunkSizes = []struct {
	name string
	size int
}{
	{"4KiB", 4 << 10},
	{"64KiB", 64 << 10},
	{"4MiB", 4 << 20},
}

func newBenchCipher(b *testing.B) *crypto.Cipher {
	b.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		b.Fatal(err)
	}
	c, err := crypto.NewCipher(key)
	if err != nil {
		b.Fatal(err)
	}
	return c
}

// BenchmarkEncryptChunk measures the AES-256-GCM seal throughput — the per-byte
// floor for every write, since each replica encrypts before it touches disk.
func BenchmarkEncryptChunk(b *testing.B) {
	for _, cs := range chunkSizes {
		b.Run(cs.name, func(b *testing.B) {
			c := newBenchCipher(b)
			data := make([]byte, cs.size)
			if _, err := rand.Read(data); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(cs.size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := c.EncryptChunk(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecryptChunk measures the open+verify throughput — the per-byte floor
// for every read served from a local replica.
func BenchmarkDecryptChunk(b *testing.B) {
	for _, cs := range chunkSizes {
		b.Run(cs.name, func(b *testing.B) {
			c := newBenchCipher(b)
			data := make([]byte, cs.size)
			if _, err := rand.Read(data); err != nil {
				b.Fatal(err)
			}
			env, err := c.EncryptChunk(data)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(cs.size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := c.DecryptChunk(env); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
