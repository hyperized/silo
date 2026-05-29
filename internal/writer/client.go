package writer

import (
	"context"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
)

const (
	// DefaultChunkSize is the amount of plaintext a writer accumulates before
	// sealing it into one chunk. 4 MiB matches the chunk store's working size
	// and stays under gRPC's default message ceiling once framed.
	DefaultChunkSize = 4 << 20

	// putFrameSize bounds a single Put data frame so a large chunk streams in
	// pieces well under gRPC's 4 MiB receive limit.
	putFrameSize = 64 << 10
)

// Writer streams data into the cluster as writer-owned chunks. It derives
// every chunk id locally from its identity and a monotonic counter, stores
// the chunk on a silod node, and records the id in the file's manifest, so a
// reader can later reassemble the file in write order. A Writer is safe for
// concurrent use: the chunk counter is atomic, so streams opened from the
// same Writer never collide on a chunk id.
type Writer struct {
	chunks    chunkv1.ChunkStoreClient
	ns        namespacev1.NamespaceStoreClient
	id        string
	epoch     uint64
	chunkSize int
	counter   atomic.Uint64
}

// Option customises a Writer at construction.
type Option func(*Writer)

// WithChunkSize sets the plaintext bytes per chunk. Non-positive values are
// ignored so a misconfigured caller falls back to the default rather than a
// writer that seals a chunk per byte.
func WithChunkSize(n int) Option {
	return func(w *Writer) {
		if n > 0 {
			w.chunkSize = n
		}
	}
}

// WithEpoch sets the epoch component of derived chunk ids. Distinct epochs let
// one writer identity partition its chunk-id space across runs; the default of
// 0 is sufficient when each run mints a fresh writer id.
func WithEpoch(epoch uint64) Option {
	return func(w *Writer) { w.epoch = epoch }
}

// New builds a Writer that talks to the cluster through the given chunk and
// namespace clients — typically both dialed over one mTLS connection to a
// silod node. It mints a fresh, globally-unique writer id rooted at nodeID.
func New(chunks chunkv1.ChunkStoreClient, ns namespacev1.NamespaceStoreClient, nodeID string, opts ...Option) (*Writer, error) {
	id, err := NewWriterID(nodeID)
	if err != nil {
		return nil, err
	}
	w := &Writer{chunks: chunks, ns: ns, id: id, chunkSize: DefaultChunkSize}
	for _, opt := range opts {
		opt(w)
	}
	return w, nil
}

// ID returns the writer's derived identity.
func (w *Writer) ID() string { return w.id }

// Create opens path for writing, creating the file if it does not yet exist;
// writing to an existing file appends to its manifest. The returned Stream
// carries ctx through to every chunk Put and manifest append, so cancelling
// ctx aborts an in-flight write.
func (w *Writer) Create(ctx context.Context, path string) (*Stream, error) {
	if _, err := w.ns.Touch(ctx, &namespacev1.TouchRequest{Path: path}); err != nil {
		// An already-existing file is the append case, not a failure.
		if status.Code(err) != codes.AlreadyExists {
			return nil, fmt.Errorf("writer: could not open %q for writing (%w); check the path is valid and silod is reachable", path, err)
		}
	}
	return &Stream{w: w, ctx: ctx, path: path, buf: make([]byte, 0, w.chunkSize)}, nil
}

// putChunk stores one chunk on silod, framing the data so a large chunk stays
// under gRPC's message ceiling. The data slice is not retained: every Send
// marshals before it returns, and the caller does not mutate the buffer until
// putChunk has returned.
func (w *Writer) putChunk(ctx context.Context, id string, data []byte) error {
	stream, err := w.chunks.Put(ctx)
	if err != nil {
		return fmt.Errorf("writer: could not open a chunk put stream to silod (%w); check the daemon is reachable", err)
	}
	if err := stream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Header{Header: &chunkv1.PutHeader{ChunkId: id}}}); err != nil {
		return fmt.Errorf("writer: could not send the header for chunk %s (%w)", id, err)
	}
	for off := 0; off < len(data); off += putFrameSize {
		end := off + putFrameSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Data{Data: data[off:end]}}); err != nil {
			return fmt.Errorf("writer: could not send data for chunk %s (%w)", id, err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("writer: silod rejected chunk %s (%w)", id, err)
	}
	return nil
}

// Stream is an append session for one file. It buffers writes and seals a
// chunk each time a full chunkSize has accumulated; Close seals the final
// partial chunk. A Stream is not safe for concurrent use — give each goroutine
// its own Stream (the parent Writer is safe to share).
type Stream struct {
	w    *Writer
	ctx  context.Context //nolint:containedctx // bound to the write session so Stream can satisfy io.Writer
	path string
	buf  []byte
	err  error // sticky: once a flush fails, every later call returns it
}

// Write buffers p and seals as many full chunks as have accumulated. It
// satisfies io.Writer, so io.Copy streams a source straight into the cluster.
func (s *Stream) Write(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	s.buf = append(s.buf, p...)
	for len(s.buf) >= s.w.chunkSize {
		if err := s.flush(s.w.chunkSize); err != nil {
			s.err = err
			return 0, err
		}
	}
	return len(p), nil
}

// Close seals any buffered remainder. After Close the Stream must not be
// written to again.
func (s *Stream) Close() error {
	if s.err != nil {
		return s.err
	}
	if len(s.buf) > 0 {
		if err := s.flush(len(s.buf)); err != nil {
			s.err = err
			return err
		}
	}
	return nil
}

// flush seals the first n buffered bytes into a chunk: derive the next id,
// store the chunk, record it in the manifest, then drop those bytes. The
// manifest append happens after the chunk is durable so a reader never sees a
// chunk id it cannot fetch.
func (s *Stream) flush(n int) error {
	id := ChunkID(s.w.id, s.w.epoch, s.w.counter.Add(1)-1)
	if err := s.w.putChunk(s.ctx, id, s.buf[:n]); err != nil {
		return err
	}
	if _, err := s.w.ns.AppendChunk(s.ctx, &namespacev1.AppendChunkRequest{Path: s.path, ChunkId: id}); err != nil {
		return fmt.Errorf("writer: stored chunk %s but could not record it in the manifest for %q (%w); retry the write to relink it", id, s.path, err)
	}
	s.buf = s.buf[:copy(s.buf, s.buf[n:])]
	return nil
}
