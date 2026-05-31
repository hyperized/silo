package replication_test

import (
	"context"
	"crypto/rand"
	"strconv"
	"testing"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/hyperized/silo/internal/replication"
)

// peersBenchSizes spans the small-chunk, default-frame, and default-chunk
// ranges so the per-byte throughput and per-op overhead are both visible.
// Matches the shapes used by the crypto and chunkstore micro-benchmarks.
var peersBenchSizes = []struct {
	name string
	size int
}{
	{"4KiB", 4 << 10},
	{"64KiB", 64 << 10},
	{"4MiB", 4 << 20},
}

// BenchmarkGRPCPeers_Store measures the replica-write hot path: dial the
// peer, stream the chunk header + framed data, await the ack. Cached
// connection is reused across iterations (production behaviour).
func BenchmarkGRPCPeers_Store(b *testing.B) {
	addr := startReplicaServer(b)
	peers := replication.NewGRPCPeers(insecure.NewCredentials(), discardLogger())
	b.Cleanup(func() { _ = peers.Close() })

	for _, sz := range peersBenchSizes {
		b.Run(sz.name, func(b *testing.B) {
			payload := make([]byte, sz.size)
			if _, err := rand.Read(payload); err != nil {
				b.Fatalf("rand: %v", err)
			}
			b.SetBytes(int64(sz.size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, err := peers.Store(context.Background(), addr, "bench-store-"+sz.name+"-"+strconv.Itoa(i), payload); err != nil {
					b.Fatalf("Store: %v", err)
				}
			}
		})
	}
}

// BenchmarkGRPCPeers_Fetch measures replica-read reassembly: the path
// the read coordinator falls back to when the local replica is missing,
// and the path the scrubber uses to backfill. The fix to pre-size the
// reassembly buffer from the Info frame shows up here as the allocation
// count dropping from ~chunk_size/64KiB + constant down to a small
// constant.
func BenchmarkGRPCPeers_Fetch(b *testing.B) {
	addr := startReplicaServer(b)
	peers := replication.NewGRPCPeers(insecure.NewCredentials(), discardLogger())
	b.Cleanup(func() { _ = peers.Close() })

	for _, sz := range peersBenchSizes {
		b.Run(sz.name, func(b *testing.B) {
			payload := make([]byte, sz.size)
			if _, err := rand.Read(payload); err != nil {
				b.Fatalf("rand: %v", err)
			}
			id := "bench-fetch-" + sz.name
			if _, err := peers.Store(context.Background(), addr, id, payload); err != nil {
				b.Fatalf("Store: %v", err)
			}
			b.SetBytes(int64(sz.size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				data, _, err := peers.Fetch(context.Background(), addr, id)
				if err != nil {
					b.Fatalf("Fetch: %v", err)
				}
				if len(data) != sz.size {
					b.Fatalf("short fetch: %d, want %d", len(data), sz.size)
				}
			}
		})
	}
}
