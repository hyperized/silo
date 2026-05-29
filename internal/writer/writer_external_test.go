package writer_test

import (
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/writer"
)

func TestChunkID_Deterministic(t *testing.T) {
	if a, b := writer.ChunkID("w-1", 2, 3), writer.ChunkID("w-1", 2, 3); a != b {
		t.Errorf("ChunkID not deterministic: %q vs %q", a, b)
	}
	if writer.ChunkID("w-1", 2, 3) == writer.ChunkID("w-1", 2, 4) {
		t.Error("different counters must yield different ids")
	}
	if writer.ChunkID("w-1", 2, 3) == writer.ChunkID("w-1", 9, 3) {
		t.Error("different epochs must yield different ids")
	}
	if got := writer.ChunkID("w-1", 0, 0); got != "w-1-0-0" {
		t.Errorf("ChunkID format: got %q, want w-1-0-0", got)
	}
}

func TestNewWriterID_UniqueAndSafe(t *testing.T) {
	a, err := writer.NewWriterID("silo-a")
	if err != nil {
		t.Fatalf("NewWriterID: %v", err)
	}
	b, err := writer.NewWriterID("silo-a")
	if err != nil {
		t.Fatalf("NewWriterID: %v", err)
	}
	if a == b {
		t.Error("two writer ids on the same node must differ")
	}
	if !strings.HasPrefix(a, "silo-a-") {
		t.Errorf("writer id %q should be rooted at the node id", a)
	}
	// A chunk id derived from a freshly-minted writer id is store-valid.
	if err := chunkstore.ValidateID(writer.ChunkID(a, 0, 0)); err != nil {
		t.Errorf("derived chunk id is not valid: %v", err)
	}
}
