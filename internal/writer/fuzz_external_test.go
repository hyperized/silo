package writer_test

import (
	"testing"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/writer"
)

// FuzzChunkIDAlwaysValid is the safety invariant of the writer's local id
// derivation: for any node id and any epoch/counter, the chunk id a writer
// produces must pass the chunk store's validation. Because that validation
// is also what keeps a chunk id from escaping the data directory, this
// guarantees a writer can never derive an id the store would reject or that
// could traverse the filesystem.
func FuzzChunkIDAlwaysValid(f *testing.F) {
	f.Add("silo-a", uint64(0), uint64(0))
	f.Add("", uint64(1<<63), uint64(1<<63))
	f.Add("weird/node id with spaces.and.dots", uint64(42), uint64(99))

	f.Fuzz(func(t *testing.T, nodeID string, epoch, counter uint64) {
		id, err := writer.NewWriterID(nodeID)
		if err != nil {
			t.Skip() // entropy failure is unrelated to the invariant
		}
		chunkID := writer.ChunkID(id, epoch, counter)
		if err := chunkstore.ValidateID(chunkID); err != nil {
			t.Fatalf("derived chunk id %q is invalid: %v", chunkID, err)
		}
	})
}
