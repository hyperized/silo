package silofuse

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	"github.com/hyperized/silo/internal/reader"
	"github.com/hyperized/silo/internal/writer"
)

// GRPCBackend implements Backend against a live silod over gRPC: directory
// operations go to the namespace service, file bytes through the reader/writer
// SDKs. It is the thin wiring between SiloFS (tested in isolation) and the
// cluster; it needs a running silod, so it is exercised by integration rather
// than unit tests.
type GRPCBackend struct {
	ns     namespacev1.NamespaceStoreClient
	chunks chunkv1.ChunkStoreClient
	reader *reader.Reader
	writer *writer.Writer
}

// NewGRPCBackend wires a backend over the namespace and chunk clients. nodeID
// seeds the writer's chunk-id space.
func NewGRPCBackend(ns namespacev1.NamespaceStoreClient, chunks chunkv1.ChunkStoreClient, nodeID string) (*GRPCBackend, error) {
	w, err := writer.New(chunks, ns, nodeID)
	if err != nil {
		return nil, fmt.Errorf("silofuse: could not start the writer SDK (%w)", err)
	}
	return &GRPCBackend{ns: ns, chunks: chunks, reader: reader.New(chunks, ns), writer: w}, nil
}

// Mkdir creates a directory in the namespace.
func (b *GRPCBackend) Mkdir(ctx context.Context, path string) error {
	_, err := b.ns.Mkdir(ctx, &namespacev1.MkdirRequest{Path: path})
	return err
}

// Touch creates an empty file in the namespace.
func (b *GRPCBackend) Touch(ctx context.Context, path string) error {
	_, err := b.ns.Touch(ctx, &namespacev1.TouchRequest{Path: path})
	return err
}

// Remove deletes a namespace entry.
func (b *GRPCBackend) Remove(ctx context.Context, path string) error {
	_, err := b.ns.Remove(ctx, &namespacev1.RemoveRequest{Path: path})
	return err
}

// List returns a directory's children.
func (b *GRPCBackend) List(ctx context.Context, path string) ([]DirItem, error) {
	resp, err := b.ns.List(ctx, &namespacev1.ListRequest{Path: path})
	if err != nil {
		return nil, err
	}
	out := make([]DirItem, 0, len(resp.GetEntries()))
	for _, e := range resp.GetEntries() {
		out = append(out, DirItem{Name: e.GetName(), IsDir: e.GetType() == namespacev1.EntryType_ENTRY_TYPE_DIR})
	}
	return out, nil
}

// ReadFile reconstructs a file's bytes through the reader SDK.
func (b *GRPCBackend) ReadFile(ctx context.Context, path string) ([]byte, error) {
	stream, err := b.reader.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stream.Close() }()
	return io.ReadAll(stream)
}

// WriteFile replaces a file's contents. silo files are append-only, so a
// replace is a remove-then-rewrite: the old chunks are tombstoned and the new
// content is streamed as fresh writer-owned chunks. This is the close-to-open
// commit — atomicity beyond the single writer is out of scope for v1.
func (b *GRPCBackend) WriteFile(ctx context.Context, path string, data []byte) error {
	if _, err := b.ns.Remove(ctx, &namespacev1.RemoveRequest{Path: path}); err != nil && status.Code(err) != codes.NotFound {
		return err
	}
	stream, err := b.writer.Create(ctx, path)
	if err != nil {
		return err
	}
	if _, err := stream.Write(data); err != nil {
		_ = stream.Close()
		return err
	}
	return stream.Close()
}

// FileSize sums the plaintext sizes of a file's chunks from its manifest,
// without decrypting them.
func (b *GRPCBackend) FileSize(ctx context.Context, path string) (int64, error) {
	manifest, err := b.ns.Manifest(ctx, &namespacev1.ManifestRequest{Path: path})
	if err != nil {
		return 0, err
	}
	var total int64
	for _, id := range manifest.GetChunkIds() {
		info, err := b.chunks.Stat(ctx, &chunkv1.StatRequest{ChunkId: id})
		if err != nil {
			return 0, err
		}
		total += info.GetInfo().GetPlainBytes()
	}
	return total, nil
}

// Compile-time check that GRPCBackend satisfies Backend.
var _ Backend = (*GRPCBackend)(nil)
