package volume_test

import (
	"context"
	"crypto/rand"
	mathrand "math/rand/v2"
	"testing"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crypto"
	"github.com/hyperized/silo/internal/volume"
)

// fileChunks adapts a real encrypted chunkstore.FileStore to volume.Chunks.
// We bench against the real store (not the in-memory fakeChunks) because
// each volume WriteAt produces a fresh chunk that goes through AES-GCM
// seal + fsync(file) + fsync(dir) — that's the NBD-write hot path silod
// actually pays per extent, and it's what an operator cares about when
// sizing a deployment.
type fileChunks struct{ store *chunkstore.FileStore }

func newFileChunks(b *testing.B) *fileChunks {
	b.Helper()
	key := make([]byte, crypto.ClusterKeyBytes)
	if _, err := rand.Read(key); err != nil {
		b.Fatalf("rand: %v", err)
	}
	cipher, err := crypto.NewCipher(key)
	if err != nil {
		b.Fatalf("NewCipher: %v", err)
	}
	fs, err := chunkstore.NewFileStore(b.TempDir(), cipher)
	if err != nil {
		b.Fatalf("NewFileStore: %v", err)
	}
	return &fileChunks{store: fs}
}

func (c *fileChunks) GetChunk(ctx context.Context, id string) ([]byte, error) {
	data, _, err := c.store.Get(ctx, id)
	return data, err
}

func (c *fileChunks) PutChunk(ctx context.Context, id string, data []byte) error {
	_, err := c.store.Put(ctx, id, data)
	return err
}

// benchMeta is a minimal in-memory volume.Metadata so the bench measures
// I/O cost — extent slicing, COW pipeline, real chunk seal/fsync — without
// dragging the CRDT namespace's gossip/anti-entropy machinery in. The
// namespace surface is benched separately by ClusterNamespaceMkdir.
type benchMeta struct {
	size    int64
	extents map[uint64]string
}

func newBenchMeta(extentSize int64) *benchMeta {
	return &benchMeta{size: extentSize, extents: make(map[uint64]string)}
}

func (m *benchMeta) ExtentSize(string) (int64, error) { return m.size, nil }

func (m *benchMeta) Extent(_ string, idx uint64) (string, bool, error) {
	id, ok := m.extents[idx]
	return id, ok, nil
}

func (m *benchMeta) WriteExtent(_ string, idx uint64, id, _ string) error {
	m.extents[idx] = id
	return nil
}

// openBenchVolume opens a Volume backed by a real encrypted chunk store
// and an in-memory metadata sized to extentSize.
func openBenchVolume(b *testing.B, extentSize int64) *volume.Volume {
	b.Helper()
	v, err := volume.Open(context.Background(), newBenchMeta(extentSize), newFileChunks(b), "/bench", "bench-holder")
	if err != nil {
		b.Fatalf("volume.Open: %v", err)
	}
	return v
}

// BenchmarkVolumeWriteAt_AlignedFullExtent measures the best-case write:
// the IO is aligned to extentSize and exactly extentSize wide, so the
// volume skips the read-modify-write (no GetChunk on the COW path) and
// just seals + persists a new chunk. Approximates a sequential streaming
// writer (mkfs, dd if=/dev/zero) once it's past any sub-extent fragments.
func BenchmarkVolumeWriteAt_AlignedFullExtent(b *testing.B) {
	const extentSize = 64 << 10 // namespace.DefaultExtentSize
	v := openBenchVolume(b, extentSize)
	payload := make([]byte, extentSize)
	if _, err := rand.Read(payload); err != nil {
		b.Fatalf("rand: %v", err)
	}
	b.SetBytes(extentSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := v.WriteAt(payload, int64(i)*extentSize); err != nil {
			b.Fatalf("WriteAt: %v", err)
		}
	}
}

// BenchmarkVolumeWriteAt_SmallWriteHotExtent measures the worst-case write:
// a small (4 KiB) write inside an already-mapped 64 KiB extent. Every
// iteration reads the current chunk (decrypt), overlays the new bytes,
// seals + persists the result, and rebinds the extent. This is the
// hottest path for an NBD client writing small blocks (the page-cache
// flusher, a database journal append) and the most punishing one — the
// volume's per-extent writeMu serialises the loop, so it doubles as the
// floor for single-volume throughput.
func BenchmarkVolumeWriteAt_SmallWriteHotExtent(b *testing.B) {
	const extentSize = 64 << 10
	const writeSize = 4 << 10
	v := openBenchVolume(b, extentSize)
	// Pre-write the extent so every iteration goes through the
	// read-modify-write path (an unmapped extent would skip the GetChunk).
	seed := make([]byte, extentSize)
	if _, err := rand.Read(seed); err != nil {
		b.Fatalf("rand: %v", err)
	}
	if _, err := v.WriteAt(seed, 0); err != nil {
		b.Fatalf("seed WriteAt: %v", err)
	}
	payload := make([]byte, writeSize)
	if _, err := rand.Read(payload); err != nil {
		b.Fatalf("rand: %v", err)
	}
	b.SetBytes(writeSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		// Cycle the offset within the extent so we don't all collide on
		// byte 0 — keeps the bench honest if a future implementation
		// caches the last-written extent.
		off := int64((i * writeSize) % (extentSize - writeSize))
		if _, err := v.WriteAt(payload, off); err != nil {
			b.Fatalf("WriteAt: %v", err)
		}
	}
}

// BenchmarkVolumeReadAt_Sequential measures sequential aligned reads at
// extent granularity — the streaming-read floor (cp, dd, fsck). Each
// iteration triggers one GetChunk + AES-GCM open. Reads are concurrent
// at the volume layer, but the bench is single-goroutine because that's
// what individual NBD requests do.
func BenchmarkVolumeReadAt_Sequential(b *testing.B) {
	const extentSize = 64 << 10
	const extents = 64 // 4 MiB working set
	v := openBenchVolume(b, extentSize)
	seed := make([]byte, extentSize)
	if _, err := rand.Read(seed); err != nil {
		b.Fatalf("rand: %v", err)
	}
	for i := range extents {
		if _, err := v.WriteAt(seed, int64(i)*extentSize); err != nil {
			b.Fatalf("seed WriteAt: %v", err)
		}
	}
	out := make([]byte, extentSize)
	b.SetBytes(extentSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		off := int64(i%extents) * extentSize
		if _, err := v.ReadAt(out, off); err != nil {
			b.Fatalf("ReadAt: %v", err)
		}
	}
}

// BenchmarkVolumeReadAt_RandomSmall measures small (4 KiB) random reads
// scattered across the working set — the worst case for a read-heavy
// workload (database OLTP, FS metadata). Each read still loads the full
// 64 KiB extent it lands in, so the per-byte throughput will look low
// compared to Sequential at the same chunk size; that delta is the
// extent-amplification cost an operator pays for small-block reads.
func BenchmarkVolumeReadAt_RandomSmall(b *testing.B) {
	const extentSize = 64 << 10
	const readSize = 4 << 10
	const extents = 64
	v := openBenchVolume(b, extentSize)
	seed := make([]byte, extentSize)
	if _, err := rand.Read(seed); err != nil {
		b.Fatalf("rand: %v", err)
	}
	for i := range extents {
		if _, err := v.WriteAt(seed, int64(i)*extentSize); err != nil {
			b.Fatalf("seed WriteAt: %v", err)
		}
	}
	rng := mathrand.New(mathrand.NewPCG(1, 2)) //nolint:gosec // perf bench, not security-relevant
	out := make([]byte, readSize)
	const maxOff = extents*extentSize - readSize
	b.SetBytes(readSize)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		off := rng.Int64N(maxOff)
		if _, err := v.ReadAt(out, off); err != nil {
			b.Fatalf("ReadAt: %v", err)
		}
	}
}
