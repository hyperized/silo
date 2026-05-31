package placement_test

import (
	"strconv"
	"testing"

	"github.com/hyperized/silo/internal/placement"
)

// BenchmarkReplicas measures the per-operation locator cost: Replicas runs on
// every read, write, delete, and scrub, so it sits on the hot path. It should
// be O(log(nodes*vnodes) + rf) and allocation-light, independent of cluster
// size. This benchmark confirms the ring lookup never becomes a bottleneck.
func BenchmarkReplicas(b *testing.B) {
	for _, n := range []int{3, 10, 50, 200} {
		b.Run(strconv.Itoa(n)+"nodes", func(b *testing.B) {
			ids := make([]string, n)
			for i := range ids {
				ids[i] = "node-" + strconv.Itoa(i)
			}
			ring := placement.New(ids, 128)
			keys := make([]string, 1024)
			for i := range keys {
				keys[i] = "chunk-" + strconv.Itoa(i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				_ = ring.Replicas(keys[i&1023], 3)
			}
		})
	}
}
