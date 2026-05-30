// Package volume is silo's block-I/O surface over a CRDT volume inode. It
// turns random-access reads and writes (the io.ReaderAt / io.WriterAt an NBD
// server drives) into extent-sized chunk reads and copy-on-write chunk writes:
// a write that does not cover a whole extent reads the current chunk, overlays
// the new bytes, and stores a fresh chunk, then rebinds the extent. Chunks are
// immutable, so every write produces a new chunk and the old one is left for
// garbage collection once unreferenced.
package volume

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hyperized/silo/internal/writer"
)

// newWriterID is the identity seam. Production derives a fresh writer id;
// tests override it to exercise the entropy-failure path.
var newWriterID = writer.NewWriterID

// Metadata is the slice of the namespace a volume's block I/O needs. The
// namespace satisfies it.
type Metadata interface {
	ExtentSize(path string) (int64, error)
	Extent(path string, index uint64) (chunkID string, mapped bool, err error)
	WriteExtent(path string, index uint64, chunkID, holder string) error
}

// Chunks reads and writes whole chunks by id, in plaintext. A node's
// replication coordinator satisfies it (placement and encryption happen
// underneath).
type Chunks interface {
	GetChunk(ctx context.Context, id string) ([]byte, error)
	PutChunk(ctx context.Context, id string, data []byte) error
}

// Volume is an open block-I/O session over the volume at a path. It satisfies
// io.ReaderAt and io.WriterAt. Writes are fenced to holder, which must hold the
// volume's lease, and are serialized so each extent's read-modify-write is
// atomic; reads run concurrently.
type Volume struct {
	ctx        context.Context //nolint:containedctx // bound to the mount session so Volume can satisfy io.ReaderAt/WriterAt
	meta       Metadata
	chunks     Chunks
	path       string
	holder     string
	extentSize int64
	writerID   string

	counter atomic.Uint64
	writeMu sync.Mutex // serializes read-modify-write so concurrent writes can't lose an update
}

// Open binds a block-I/O session to the volume at path, writing as holder
// (which must hold the lease for writes to be honoured). It reads the volume's
// fixed extent size once; ctx governs every chunk read and write.
func Open(ctx context.Context, meta Metadata, chunks Chunks, path, holder string) (*Volume, error) {
	size, err := meta.ExtentSize(path)
	if err != nil {
		return nil, err
	}
	if size <= 0 {
		// locate divides the byte offset by extentSize; a zero or negative
		// extent size would panic (divide by zero) on the first read or write,
		// crashing the goroutine serving the NBD connection. Refuse to open.
		return nil, fmt.Errorf("volume: extent size for %q must be positive, got %d", path, size)
	}
	id, err := newWriterID(holder)
	if err != nil {
		return nil, err
	}
	return &Volume{ctx: ctx, meta: meta, chunks: chunks, path: path, holder: holder, extentSize: size, writerID: id}, nil
}

// ReadAt fills p with the volume's bytes starting at off, fetching each
// extent's backing chunk; unmapped extents read as zeros. It satisfies
// io.ReaderAt: a short read returns a non-nil error.
func (v *Volume) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("volume: negative read offset %d", off)
	}
	read := 0
	for read < len(p) {
		idx, within := v.locate(off + int64(read))
		extent, err := v.extentBytes(idx)
		if err != nil {
			return read, err
		}
		read += copy(p[read:], extent[within:])
	}
	return read, nil
}

// WriteAt stores p into the volume starting at off via copy-on-write: each
// extent the write touches is rewritten as a fresh chunk and rebound. It
// satisfies io.WriterAt. A write is fenced (ErrLeaseHeld) if holder no longer
// holds the lease.
func (v *Volume) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("volume: negative write offset %d", off)
	}
	v.writeMu.Lock()
	defer v.writeMu.Unlock()

	written := 0
	for written < len(p) {
		idx, within := v.locate(off + int64(written))
		n := min(len(p)-written, int(v.extentSize)-within)

		extent := make([]byte, v.extentSize)
		if within != 0 || n != int(v.extentSize) {
			// Partial extent: read the current contents before overlaying, so
			// the bytes outside the written range are preserved.
			cur, err := v.extentBytes(idx)
			if err != nil {
				return written, err
			}
			copy(extent, cur)
		}
		copy(extent[within:within+n], p[written:written+n])

		id := writer.ChunkID(v.writerID, 0, v.counter.Add(1)-1)
		if err := v.chunks.PutChunk(v.ctx, id, extent); err != nil {
			return written, err
		}
		if err := v.meta.WriteExtent(v.path, idx, id, v.holder); err != nil {
			return written, err
		}
		written += n
	}
	return written, nil
}

// locate maps an absolute byte offset to its extent index and the offset
// within that extent. pos is non-negative — ReadAt and WriteAt reject negative
// offsets before calling.
func (v *Volume) locate(pos int64) (index uint64, within int) {
	return uint64(pos / v.extentSize), int(pos % v.extentSize) // #nosec G115 -- pos is non-negative
}

// extentBytes returns the full extentSize-byte contents of the extent at idx:
// the backing chunk if mapped (zero-padded if somehow short), or zeros if not.
func (v *Volume) extentBytes(idx uint64) ([]byte, error) {
	id, mapped, err := v.meta.Extent(v.path, idx)
	if err != nil {
		return nil, err
	}
	if !mapped {
		return make([]byte, v.extentSize), nil
	}
	data, err := v.chunks.GetChunk(v.ctx, id)
	if err != nil {
		return nil, fmt.Errorf("volume: could not read chunk %s backing extent %d of %q (%w)", id, idx, v.path, err)
	}
	if int64(len(data)) != v.extentSize {
		full := make([]byte, v.extentSize)
		copy(full, data)
		return full, nil
	}
	return data, nil
}
