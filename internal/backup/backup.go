// Package backup exports a silod node's durable state — its encrypted chunks
// and its namespace snapshot — to a blobstore.Target (local disk, S3, GCS, or
// Azure Blob). Chunks are copied as-is, still AES-GCM encrypted under the
// cluster key, so a backup is no less protected than the live data. Because the
// namespace is a CRDT replicated to every node, any node's snapshot is the
// cluster manifest; chunks are node-local, so a full cluster backup is the union
// of every node's chunk export.
package backup

import (
	"context"
	"fmt"
	"path"

	"github.com/hyperized/silo/internal/blobstore"
)

// ChunkSource enumerates and reads the node's encrypted chunks. *chunkstore.FileStore
// satisfies it.
type ChunkSource interface {
	List(ctx context.Context) ([]string, error)
	RawChunk(ctx context.Context, id string) ([]byte, error)
}

// NamespaceSnapshot serialises the node's namespace replica. *namespace.Namespace
// satisfies it.
type NamespaceSnapshot interface {
	Snapshot() ([]byte, error)
}

// Stats summarises one export run.
type Stats struct {
	Chunks int
	Bytes  int64
}

// Exporter writes one node's chunks and namespace snapshot to a Target.
type Exporter struct {
	chunks ChunkSource
	ns     NamespaceSnapshot
	nodeID string
}

// NewExporter builds an exporter for a node's local state.
func NewExporter(chunks ChunkSource, ns NamespaceSnapshot, nodeID string) *Exporter {
	return &Exporter{chunks: chunks, ns: ns, nodeID: nodeID}
}

// Export writes the namespace snapshot under namespace/<node>.json and each
// chunk under chunks/<id>, returning what was written. A read or write failure
// aborts the run so a partial backup is never reported as complete.
func (e *Exporter) Export(ctx context.Context, target blobstore.Target) (Stats, error) {
	snap, err := e.ns.Snapshot()
	if err != nil {
		return Stats{}, fmt.Errorf("backup: could not snapshot the namespace (%w)", err)
	}
	if err := target.Put(ctx, path.Join("namespace", e.nodeID+".json"), snap); err != nil {
		return Stats{}, fmt.Errorf("backup: could not write the namespace snapshot (%w)", err)
	}

	ids, err := e.chunks.List(ctx)
	if err != nil {
		return Stats{}, fmt.Errorf("backup: could not list local chunks (%w)", err)
	}
	var stats Stats
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		data, err := e.chunks.RawChunk(ctx, id)
		if err != nil {
			return stats, fmt.Errorf("backup: could not read chunk %s (%w)", id, err)
		}
		if err := target.Put(ctx, path.Join("chunks", id), data); err != nil {
			return stats, fmt.Errorf("backup: could not upload chunk %s (%w)", id, err)
		}
		stats.Chunks++
		stats.Bytes += int64(len(data))
	}
	return stats, nil
}
