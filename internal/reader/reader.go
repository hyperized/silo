// Package reader is silo's reader SDK, the counterpart to the writer SDK. It
// reads a file's manifest from the namespace and streams the file's chunks
// back in write order, reassembling the original byte stream.
package reader

import (
	"context"
	"errors"
	"fmt"
	"io"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
)

// Reader reads files written by the writer SDK. It resolves a file's manifest
// through the namespace, then fetches the listed chunks in order. A Reader is
// stateless and safe for concurrent use; each Open returns an independent
// Stream.
type Reader struct {
	chunks chunkv1.ChunkStoreClient
	ns     namespacev1.NamespaceStoreClient
}

// New builds a Reader over the given chunk and namespace clients — typically
// both dialed over one mTLS connection to a silod node.
func New(chunks chunkv1.ChunkStoreClient, ns namespacev1.NamespaceStoreClient) *Reader {
	return &Reader{chunks: chunks, ns: ns}
}

// Open resolves path's manifest and returns a Stream over the file's bytes.
// The bytes are produced lazily: each chunk is fetched only as the reader
// consumes it, so opening a large file is cheap and reading it is bounded by
// one chunk in flight. The returned Stream must be closed to release the
// in-flight fetch.
func (r *Reader) Open(ctx context.Context, path string) (*Stream, error) {
	resp, err := r.ns.Manifest(ctx, &namespacev1.ManifestRequest{Path: path})
	if err != nil {
		return nil, fmt.Errorf("reader: could not read the manifest for %q (%w); check the path exists and silod is reachable", path, err)
	}
	cctx, cancel := context.WithCancel(ctx)
	return &Stream{
		ctx:    cctx,
		cancel: cancel,
		chunks: r.chunks,
		ids:    resp.GetChunkIds(),
	}, nil
}

// Stream reads one file's bytes in chunk order. It satisfies io.ReadCloser, so
// io.Copy drains a file out of the cluster. A Stream is not safe for
// concurrent use.
type Stream struct {
	ctx    context.Context //nolint:containedctx // bound to the read session so Stream can satisfy io.Reader
	cancel context.CancelFunc
	chunks chunkv1.ChunkStoreClient
	ids    []string
	idx    int                          // next chunk to fetch
	cur    chunkv1.ChunkStore_GetClient // current chunk's frame stream, nil between chunks
	buf    []byte                       // unread bytes from the last frame
}

// Read fills p from the current chunk, advancing to the next chunk when the
// current one is exhausted. It returns io.EOF once every chunk in the manifest
// has been drained.
func (s *Stream) Read(p []byte) (int, error) {
	for len(s.buf) == 0 {
		if s.cur == nil {
			if s.idx >= len(s.ids) {
				return 0, io.EOF
			}
			stream, err := s.chunks.Get(s.ctx, &chunkv1.GetRequest{ChunkId: s.ids[s.idx]})
			if err != nil {
				s.cancel() // release the read session; io.Copy callers never Close on error
				return 0, fmt.Errorf("reader: could not fetch chunk %s (%w); the chunk may be missing or silod is unreachable", s.ids[s.idx], err)
			}
			s.cur = stream
			s.idx++
		}
		msg, err := s.cur.Recv()
		if errors.Is(err, io.EOF) {
			s.cur = nil // chunk drained; loop to the next one
			continue
		}
		if err != nil {
			s.cancel() // terminal stream error: cancel the in-flight Get so it can't leak
			return 0, fmt.Errorf("reader: could not read chunk %s (%w)", s.ids[s.idx-1], err)
		}
		s.buf = msg.GetData()
	}
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

// Close releases any in-flight chunk fetch. It is safe to call more than once.
func (s *Stream) Close() error {
	s.cancel()
	return nil
}
