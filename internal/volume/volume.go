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
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/hyperized/silo/internal/writer"
)

// writeAtParallelism caps the number of extent puts a single WriteAt
// can have in flight at once. A 4 MiB streaming write at the default
// 64 KiB extent already spans 64 extents; without a cap we'd fire 64
// PutChunk+WriteExtent goroutines, and each one fans out to N replicas
// — that's NxN replica RPCs from one syscall. The cap is loose, just
// enough to keep the replication coordinator's per-peer queues from
// piling up.
var writeAtParallelism = max(2*runtime.GOMAXPROCS(0), 16)

// newWriterID is the identity seam. Production derives a fresh writer id;
// tests override it to exercise the entropy-failure path.
var newWriterID = writer.NewWriterID

// Metadata is the slice of the namespace a volume's block I/O needs. The
// namespace satisfies it.
//
// WriteExtents is the batch counterpart of WriteExtent and is what the
// parallel WriteAt path uses to coalesce 64-ish per-extent rebinds (a 4 MiB
// write at the default 64 KiB extent) into one lock + one persist on the
// namespace side. Implementations whose persist step is cheap may forward
// WriteExtents to WriteExtent in a loop; the production namespace coalesces.
// indexes and chunkIDs are positionally paired.
type Metadata interface {
	ExtentSize(path string) (int64, error)
	Extent(path string, index uint64) (chunkID string, mapped bool, err error)
	WriteExtent(path string, index uint64, chunkID, holder string) error
	WriteExtents(path string, indexes []uint64, chunkIDs []string, holder string) error
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
//
// A single WriteAt touches each extent at most once (the loop steps to the
// next extent boundary each iteration), so the per-extent jobs are independent
// — different chunk ids, different extent-map entries. Multi-extent writes
// dispatch the per-extent work in parallel; the per-volume writeMu still
// serializes WriteAt calls against each other.
func (v *Volume) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("volume: negative write offset %d", off)
	}
	v.writeMu.Lock()
	defer v.writeMu.Unlock()

	// Plan the jobs: one entry per extent the write touches. The plan
	// is cheap (no IO) and lets us decide between the sequential path
	// (single extent) and the parallel path (many extents) without
	// duplicating logic.
	type job struct {
		idx    uint64
		within int
		nBytes int
		srcOff int // offset into p
	}
	jobs := make([]job, 0, 4)
	planned := 0
	for planned < len(p) {
		idx, within := v.locate(off + int64(planned))
		n := min(len(p)-planned, int(v.extentSize)-within)
		jobs = append(jobs, job{idx: idx, within: within, nBytes: n, srcOff: planned})
		planned += n
	}

	// Single-extent writes (the common case for small block IO) skip the
	// goroutine + semaphore machinery entirely and use the single-WriteExtent
	// fast path.
	if len(jobs) == 1 {
		j := jobs[0]
		extent := make([]byte, v.extentSize)
		if j.within != 0 || j.nBytes != int(v.extentSize) {
			cur, err := v.extentBytes(j.idx)
			if err != nil {
				return 0, err
			}
			copy(extent, cur)
		}
		copy(extent[j.within:j.within+j.nBytes], p[j.srcOff:j.srcOff+j.nBytes])
		id := writer.ChunkID(v.writerID, 0, v.counter.Add(1)-1)
		if err := v.chunks.PutChunk(v.ctx, id, extent); err != nil {
			return 0, err
		}
		if err := v.meta.WriteExtent(v.path, j.idx, id, v.holder); err != nil {
			return 0, err
		}
		return j.nBytes, nil
	}

	// Multi-extent parallel path: each worker only does its own chunk Put;
	// the metadata rebind for every successful job is coalesced into a single
	// WriteExtents at the end so the namespace lock + persist is paid once
	// instead of len(jobs) times.
	type result struct {
		chunkID string
		err     error
	}
	results := make([]result, len(jobs))
	sem := make(chan struct{}, writeAtParallelism)
	var wg sync.WaitGroup
	for i, j := range jobs {
		wg.Add(1)
		sem <- struct{}{} // bound in-flight extent puts
		go func(i int, j job) {
			defer wg.Done()
			defer func() { <-sem }()
			extent := make([]byte, v.extentSize)
			if j.within != 0 || j.nBytes != int(v.extentSize) {
				cur, err := v.extentBytes(j.idx)
				if err != nil {
					results[i] = result{err: err}
					return
				}
				copy(extent, cur)
			}
			copy(extent[j.within:j.within+j.nBytes], p[j.srcOff:j.srcOff+j.nBytes])
			id := writer.ChunkID(v.writerID, 0, v.counter.Add(1)-1)
			if err := v.chunks.PutChunk(v.ctx, id, extent); err != nil {
				results[i] = result{err: err}
				return
			}
			results[i] = result{chunkID: id}
		}(i, j)
	}
	wg.Wait()

	// Walk the results in order, collecting the longest contiguous prefix
	// that succeeded. Anything past the first error is "indeterminate" by
	// the io.WriterAt contract — a later worker may have persisted its chunk
	// but we never rebind an extent to it, so the chunk is unreferenced and
	// left for GC.
	var (
		indexes  []uint64
		chunkIDs []string
		written  int
		firstErr error
	)
	for i, r := range results {
		if r.err != nil {
			firstErr = r.err
			break
		}
		indexes = append(indexes, jobs[i].idx)
		chunkIDs = append(chunkIDs, r.chunkID)
		written += jobs[i].nBytes
	}
	if len(indexes) > 0 {
		if err := v.meta.WriteExtents(v.path, indexes, chunkIDs, v.holder); err != nil {
			// Metadata batch failed: none of the chunks are visible to a
			// reader (no extents rebound). The chunk Puts that did complete
			// are unreferenced and left for GC.
			return 0, err
		}
	}
	return written, firstErr
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
