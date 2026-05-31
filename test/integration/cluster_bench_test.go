//go:build integration

// Cluster-level benchmarks: spawn three real silod processes under shared
// CA + key material, mint a client cert from that CA, then measure the
// end-to-end data plane through the mTLS gRPC API. These exercise paths
// the in-process micro-benchmarks under internal/{crypto,chunkstore,placement}
// can't reach — quorum write fan-out, replica-preferring reads, cross-node
// peers_grpc.Fetch, and the writer/reader SDKs end to end.
//
// Run with: make bench-cluster
package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	"github.com/hyperized/silo/internal/clustertls"
	"github.com/hyperized/silo/internal/placement"
	"github.com/hyperized/silo/internal/reader"
	"github.com/hyperized/silo/internal/writer"
)

// chunkSizesBench spans the small-write and default-chunk ranges so the
// per-byte throughput (large) and the fixed per-op overhead (small) are
// both visible — same shape as the crypto micro-benchmarks.
var chunkSizesBench = []struct {
	name string
	size int
}{
	{"4KiB", 4 << 10},
	{"64KiB", 64 << 10},
	{"4MiB", 4 << 20},
}

// benchCluster is the harness state shared across the cluster benches.
// One harness backs many sub-benches so we only pay the gossip-converge
// cost once per top-level Benchmark.
type benchCluster struct {
	nodes  []*gossipNode
	caPEM  []byte
	cert   tls.Certificate
	caPool *x509.CertPool
}

// startBenchCluster spawns a 3-node silod cluster with the supplied
// replication factor and waits for gossip convergence. A replication
// factor of 3 puts every chunk on every node (the production default,
// best for the write fan-out bench); a factor of 1 puts each chunk on
// exactly one node so the read-side benches can force a cross-node
// peers_grpc.Fetch on demand.
func startBenchCluster(b *testing.B, replication int) *benchCluster {
	b.Helper()
	bin := buildSilod(b)
	caCert, caKey, encKey := mintSharedCA(b, bin)

	ids := []string{"alpha", "beta", "gamma"}
	nodes := make([]*gossipNode, len(ids))
	var seeds []string
	for i, id := range ids {
		n := startBenchGossipNode(b, bin, id, seeds, caCert, caKey, encKey, replication)
		b.Cleanup(n.kill)
		nodes[i] = n
		seeds = append(seeds, n.gossipAddr)
	}

	// Wait for full convergence — every node must see every other peer
	// alive before any data-plane RPC is meaningful (the coordinator
	// computes Replicas off the gossiped ring).
	for i, n := range nodes {
		others := make([]string, 0, len(ids)-1)
		for j, peer := range ids {
			if i != j {
				others = append(others, peer)
			}
		}
		if err := waitForConvergence(n, others, 15*time.Second); err != nil {
			b.Fatalf("%s: %v\n---log:\n%s", n.id, err, n.readLog())
		}
	}

	// Mint a client cert straight from the shared CA so the bench can
	// skip the bootstrap-token scrape (Join writes to stdout, which the
	// gossip scaffold redirects to a log file). Same trust path silod
	// expects on every mTLS RPC.
	caCertPEM, err := os.ReadFile(caCert)
	if err != nil {
		b.Fatalf("read CA cert: %v", err)
	}
	caKeyPEM, err := os.ReadFile(caKey)
	if err != nil {
		b.Fatalf("read CA key: %v", err)
	}
	ca, err := clustertls.LoadCA(caCertPEM, caKeyPEM)
	if err != nil {
		b.Fatalf("LoadCA: %v", err)
	}
	clientCert, err := clustertls.MintClientCert(ca, "bench@integration", time.Hour)
	if err != nil {
		b.Fatalf("MintClientCert: %v", err)
	}
	pair, err := tls.X509KeyPair(clientCert.CertPEM, clientCert.KeyPEM)
	if err != nil {
		b.Fatalf("X509KeyPair: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCertPEM) {
		b.Fatal("CA PEM did not parse")
	}

	return &benchCluster{
		nodes:  nodes,
		caPEM:  caCertPEM,
		cert:   pair,
		caPool: pool,
	}
}

// startBenchGossipNode is startGossipNode with an extra knob for
// SILO_REPLICATION so the read-side benches can pick a factor that
// forces the path under test. Everything else mirrors the gossip
// integration scaffold.
func startBenchGossipNode(b *testing.B, bin, id string, seeds []string, sharedCACert, sharedCAKey, encryptionKey string, replication int) *gossipNode {
	b.Helper()
	n := startGossipNode(b, bin, id, seeds, sharedCACert, sharedCAKey, encryptionKey, false)
	if replication == 3 {
		return n
	}
	// startGossipNode already started the process. Restart with the
	// replication override applied. Cheap relative to the convergence
	// wait that follows, and keeps the shared helper unchanged.
	n.kill()
	n.env = append(n.env, "SILO_REPLICATION="+strconv.Itoa(replication))
	n.spawn(b, bin)
	return n
}

// dial returns an mTLS client connection to the bench cluster's node i,
// ready for any of the silod service clients (chunk, namespace, …).
func (c *benchCluster) dial(b *testing.B, i int) *grpc.ClientConn {
	b.Helper()
	addr := c.nodes[i].grpcAddr
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		b.Fatalf("SplitHostPort: %v", err)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{c.cert},
		RootCAs:      c.caPool,
		ServerName:   host,
		MinVersion:   tls.VersionTLS13,
	})))
	if err != nil {
		b.Fatalf("dial %s: %v", addr, err)
	}
	return conn
}

// putOne streams a single chunk to client and returns the response info.
// Shared by Put benches and by Get-setup phases that need a chunk on
// disk before the timed loop starts.
func putOne(b *testing.B, client chunkv1.ChunkStoreClient, id string, data []byte) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Put(ctx)
	if err != nil {
		b.Fatalf("Put open: %v", err)
	}
	if err := stream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Header{Header: &chunkv1.PutHeader{ChunkId: id}}}); err != nil {
		b.Fatalf("Put header: %v", err)
	}
	// Frame in 64 KiB pieces to stay well under gRPC's 4 MiB cap for
	// the 4 MiB chunk case, mirroring the writer SDK's frame size.
	const frame = 64 << 10
	for off := 0; off < len(data); off += frame {
		end := off + frame
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Data{Data: data[off:end]}}); err != nil {
			b.Fatalf("Put data: %v", err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		b.Fatalf("Put close: %v", err)
	}
}

// getOne drains a Get stream into a single buffer, returning the total
// bytes received. Errors fail the bench.
func getOne(b *testing.B, client chunkv1.ChunkStoreClient, id string) int {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Get(ctx, &chunkv1.GetRequest{ChunkId: id})
	if err != nil {
		b.Fatalf("Get %s: %v", id, err)
	}
	var n int
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			b.Fatalf("Get recv %s: %v", id, err)
		}
		n += len(msg.GetData())
	}
	return n
}

// BenchmarkClusterChunkPut measures the end-to-end write path: client
// gRPC stream → coordinator on the entry node → quorum fan-out to the
// other replicas → remote fsync → majority ack. With the default
// replication factor (3), every chunk lands on every node, so this is
// the production hot path for chunk writes.
func BenchmarkClusterChunkPut(b *testing.B) {
	c := startBenchCluster(b, 3)
	conn := c.dial(b, 0)
	defer conn.Close()
	chunks := chunkv1.NewChunkStoreClient(conn)

	for _, cs := range chunkSizesBench {
		b.Run(cs.name, func(b *testing.B) {
			data := make([]byte, cs.size)
			if _, err := rand.Read(data); err != nil {
				b.Fatalf("rand: %v", err)
			}
			b.SetBytes(int64(cs.size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				putOne(b, chunks, "bench-put-"+cs.name+"-"+strconv.Itoa(i), data)
			}
		})
	}
}

// BenchmarkClusterChunkGet_Local exercises the replica-preferring read
// path when the queried node holds a copy (replication=3, every node
// is a replica). This is the cluster's read fast path.
func BenchmarkClusterChunkGet_Local(b *testing.B) {
	c := startBenchCluster(b, 3)
	conn := c.dial(b, 0)
	defer conn.Close()
	chunks := chunkv1.NewChunkStoreClient(conn)

	for _, cs := range chunkSizesBench {
		b.Run(cs.name, func(b *testing.B) {
			data := make([]byte, cs.size)
			if _, err := rand.Read(data); err != nil {
				b.Fatalf("rand: %v", err)
			}
			id := "bench-get-local-" + cs.name
			putOne(b, chunks, id, data)
			b.SetBytes(int64(cs.size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if n := getOne(b, chunks, id); n != cs.size {
					b.Fatalf("get short read: %d, want %d", n, cs.size)
				}
			}
		})
	}
}

// BenchmarkClusterChunkGet_CrossNode forces the coordinator to fall
// back through peers_grpc.Fetch by querying chunk IDs that don't hash
// to the entry node. The cluster runs with replication=1 so each chunk
// has exactly one home; the bench pre-filters IDs whose primary is not
// the entry node, so every Get traverses the network + remote read +
// frame reassembly.
func BenchmarkClusterChunkGet_CrossNode(b *testing.B) {
	c := startBenchCluster(b, 1)
	conn := c.dial(b, 0)
	defer conn.Close()
	chunks := chunkv1.NewChunkStoreClient(conn)

	// Mirror the ring silod builds from the gossiped membership so the
	// bench can predict where each chunk lands without an RPC. vnodes
	// matches placement.DefaultVNodes — the same value silod uses.
	ids := make([]string, len(c.nodes))
	for i, n := range c.nodes {
		ids[i] = n.id
	}
	ring := placement.New(ids, placement.DefaultVNodes)
	entry := c.nodes[0].id

	pickRemoteID := func(prefix string, seed int) string {
		for n := seed; ; n++ {
			candidate := prefix + strconv.Itoa(n)
			rep := ring.Replicas(candidate, 1)
			if len(rep) == 1 && rep[0] != entry {
				return candidate
			}
		}
	}

	for _, cs := range chunkSizesBench {
		b.Run(cs.name, func(b *testing.B) {
			data := make([]byte, cs.size)
			if _, err := rand.Read(data); err != nil {
				b.Fatalf("rand: %v", err)
			}
			id := pickRemoteID("bench-get-remote-"+cs.name+"-", 0)
			putOne(b, chunks, id, data)
			b.SetBytes(int64(cs.size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if n := getOne(b, chunks, id); n != cs.size {
					b.Fatalf("get short read: %d, want %d", n, cs.size)
				}
			}
		})
	}
}

// BenchmarkClusterChunkStat measures the cheap metadata RPC. Mostly a
// floor for gRPC + mTLS round-trip; useful as a control when reading
// the Put/Get numbers (anything close to Stat is overhead-dominated).
func BenchmarkClusterChunkStat(b *testing.B) {
	c := startBenchCluster(b, 3)
	conn := c.dial(b, 0)
	defer conn.Close()
	chunks := chunkv1.NewChunkStoreClient(conn)

	const id = "bench-stat"
	putOne(b, chunks, id, []byte("payload"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := chunks.Stat(ctx, &chunkv1.StatRequest{ChunkId: id}); err != nil {
			b.Fatalf("Stat: %v", err)
		}
	}
}

// BenchmarkClusterWriterSDK measures the writer-owned-chunk path the
// reader/writer SDKs actually take: derive chunk ids locally, fan each
// chunk across the quorum, append to the inode manifest, repeat. The
// payload is 4 MiB at the default chunk size so the bench shows
// streaming throughput, not per-chunk overhead — that's what the chunk
// Put bench is for.
func BenchmarkClusterWriterSDK(b *testing.B) {
	c := startBenchCluster(b, 3)
	conn := c.dial(b, 0)
	defer conn.Close()
	chunks := chunkv1.NewChunkStoreClient(conn)
	ns := namespacev1.NewNamespaceStoreClient(conn)

	const totalSize = 4 << 20 // 4 MiB
	payload := make([]byte, totalSize)
	if _, err := rand.Read(payload); err != nil {
		b.Fatalf("rand: %v", err)
	}
	// Parent directory must exist; the namespace API does not auto-create
	// path prefixes (and shouldn't, since CRDT semantics need each entry
	// to be tagged with the issuing HLC).
	mkdirOrFatal(b, ns, "/bench")
	b.SetBytes(int64(totalSize))
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		w, err := writer.New(chunks, ns, "bench-writer-"+strconv.Itoa(i))
		if err != nil {
			b.Fatalf("writer.New: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		st, err := w.Create(ctx, "/bench/"+strconv.Itoa(i))
		if err != nil {
			cancel()
			b.Fatalf("Create: %v", err)
		}
		if _, err := io.Copy(st, bytes.NewReader(payload)); err != nil {
			cancel()
			b.Fatalf("Copy: %v", err)
		}
		if err := st.Close(); err != nil {
			cancel()
			b.Fatalf("Close: %v", err)
		}
		cancel()
	}
}

// BenchmarkClusterReaderSDK measures the inverse: reconstruct a file
// from its manifest, fetching each chunk in HLC append order. Pairs
// with the writer bench so the read/write asymmetry is measurable.
func BenchmarkClusterReaderSDK(b *testing.B) {
	c := startBenchCluster(b, 3)
	conn := c.dial(b, 0)
	defer conn.Close()
	chunks := chunkv1.NewChunkStoreClient(conn)
	ns := namespacev1.NewNamespaceStoreClient(conn)

	const totalSize = 4 << 20
	payload := make([]byte, totalSize)
	if _, err := rand.Read(payload); err != nil {
		b.Fatalf("rand: %v", err)
	}
	w, err := writer.New(chunks, ns, "bench-reader")
	if err != nil {
		b.Fatalf("writer.New: %v", err)
	}
	mkdirOrFatal(b, ns, "/bench-reader")
	const path = "/bench-reader/file"
	setupCtx, setupCancel := context.WithTimeout(context.Background(), time.Minute)
	st, err := w.Create(setupCtx, path)
	if err != nil {
		setupCancel()
		b.Fatalf("Create: %v", err)
	}
	if _, err := io.Copy(st, bytes.NewReader(payload)); err != nil {
		setupCancel()
		b.Fatalf("Copy: %v", err)
	}
	if err := st.Close(); err != nil {
		setupCancel()
		b.Fatalf("Close: %v", err)
	}
	setupCancel()

	b.SetBytes(int64(totalSize))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		r, err := reader.New(chunks, ns).Open(ctx, path)
		if err != nil {
			cancel()
			b.Fatalf("Open: %v", err)
		}
		if _, err := io.Copy(io.Discard, r); err != nil {
			cancel()
			_ = r.Close()
			b.Fatalf("Copy: %v", err)
		}
		if err := r.Close(); err != nil {
			cancel()
			b.Fatalf("Close: %v", err)
		}
		cancel()
	}
}

// BenchmarkClusterNamespaceMkdir measures the CRDT mkdir hot path.
// Useful as the floor for any tool that creates a directory tree
// (siloctl ns mkdir, CSI volume provisioner, …).
func BenchmarkClusterNamespaceMkdir(b *testing.B) {
	c := startBenchCluster(b, 3)
	conn := c.dial(b, 0)
	defer conn.Close()
	ns := namespacev1.NewNamespaceStoreClient(conn)

	mkdirOrFatal(b, ns, "/bench-mkdir")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := ns.Mkdir(ctx, &namespacev1.MkdirRequest{Path: "/bench-mkdir/d-" + strconv.Itoa(i)}); err != nil {
			b.Fatalf("Mkdir: %v", err)
		}
	}
}

// mkdirOrFatal creates a directory entry the bench will write into.
// The CRDT namespace surfaces existing-path collisions as an explicit
// error; the bench treats "already exists" as success since the
// preceding b.N loop may have already created the parent in a prior
// sub-bench against the same cluster.
func mkdirOrFatal(b *testing.B, ns namespacev1.NamespaceStoreClient, path string) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := ns.Mkdir(ctx, &namespacev1.MkdirRequest{Path: path}); err != nil {
		b.Fatalf("setup Mkdir %s: %v", path, err)
	}
}

