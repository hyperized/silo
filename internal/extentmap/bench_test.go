package extentmap_test

import (
	"fmt"
	"testing"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/extentmap"
	"github.com/hyperized/silo/internal/hlc"
)

// benchEntries is the map size of a 40 GiB volume at the default 256 KiB
// extent size.
const benchEntries = 160_000

// benchChunkID shapes a chunk id like a real one (writer node, session, counter)
// so the bytes per entry match production rather than a toy.
func benchChunkID(i int) string {
	return fmt.Sprintf("rk1-2-0b8c4f2e9a7d1c3b5e6f8a9b0c1d2e3f-0-%d", i)
}

// BenchmarkSetBatchPersist measures one 16-extent write into a fully mapped
// 40 GiB volume, including making it durable. This is the per-write cost every
// replica pays, so it has to be independent of the volume's size.
func BenchmarkSetBatchPersist(b *testing.B) {
	s, err := extentmap.Open(b.TempDir(), discard())
	if err != nil {
		b.Fatal(err)
	}
	seed := make([]crdt.MapEntry[uint64, string], benchEntries)
	for i := range seed {
		seed[i] = crdt.MapEntry[uint64, string]{Key: uint64(i), Value: benchChunkID(i), TS: hlc.Timestamp{Wall: 1, Node: "silo-a"}}
	}
	if err := s.Merge("vol", seed); err != nil {
		b.Fatal(err)
	}
	indexes := make([]uint64, 16)
	chunkIDs := make([]string, 16)
	wall := int64(2)
	b.ReportAllocs()
	for b.Loop() {
		base := uint64(wall*16) % benchEntries
		for i := range indexes {
			indexes[i] = base + uint64(i)
			chunkIDs[i] = benchChunkID(int(wall)*16 + i)
		}
		if err := s.SetBatch("vol", indexes, chunkIDs, hlc.Timestamp{Wall: wall, Node: "silo-a"}); err != nil {
			b.Fatal(err)
		}
		wall++
	}
}
