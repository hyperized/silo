//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	"github.com/hyperized/silo/internal/writer"
)

// mtlsConn dials a node's gRPC port with the freshly-claimed credentials,
// returning a connection the caller can layer any service client over.
func mtlsConn(t *testing.T, node *silodNode, caPEM, certPEM, keyPEM []byte) (*grpc.ClientConn, func()) {
	t.Helper()
	host, _, err := net.SplitHostPort(node.grpcAddr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("CA PEM did not parse")
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	conn, err := grpc.NewClient(node.grpcAddr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   host,
		MinVersion:   tls.VersionTLS13,
	})))
	if err != nil {
		t.Fatalf("dial gRPC: %v", err)
	}
	return conn, func() { _ = conn.Close() }
}

// TestWriterSDK_RoundTripAgainstRealSilod proves the writer-owned-chunk path
// end to end: the SDK seals a multi-chunk file straight into a live silod and
// records each chunk in the namespace manifest; reading the manifest back and
// fetching the chunks in order reconstructs the exact bytes.
func TestWriterSDK_RoundTripAgainstRealSilod(t *testing.T) {
	node := startSilod(t)
	defer node.teardown()

	caPEM, certPEM, keyPEM := claimClientCert(t, node)
	conn, closeConn := mtlsConn(t, node, caPEM, certPEM, keyPEM)
	defer closeConn()

	chunks := chunkv1.NewChunkStoreClient(conn)
	ns := namespacev1.NewNamespaceStoreClient(conn)

	const chunkSize = 4096
	w, err := writer.New(chunks, ns, "integration", writer.WithChunkSize(chunkSize))
	if err != nil {
		t.Fatalf("writer.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const path = "/movie.bin"
	st, err := w.Create(ctx, path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	payload := bytes.Repeat([]byte("silo-writer-"), 900) // 10800 bytes -> 3 chunks
	if _, err := io.Copy(st, bytes.NewReader(payload)); err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	wantChunks := (len(payload) + chunkSize - 1) / chunkSize
	mresp, err := ns.Manifest(ctx, &namespacev1.ManifestRequest{Path: path})
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	ids := mresp.GetChunkIds()
	if len(ids) != wantChunks {
		t.Fatalf("manifest has %d chunks, want %d", len(ids), wantChunks)
	}
	// Ids are exactly the ones the writer derives locally, in order.
	for i, id := range ids {
		if want := writer.ChunkID(w.ID(), 0, uint64(i)); id != want {
			t.Errorf("chunk %d id = %s, want %s", i, id, want)
		}
	}

	var got []byte
	for _, id := range ids {
		gs, err := chunks.Get(ctx, &chunkv1.GetRequest{ChunkId: id})
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		for {
			msg, err := gs.Recv()
			if err != nil {
				break
			}
			got = append(got, msg.GetData()...)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("reassembled %d bytes, want %d; round-trip mismatch", len(got), len(payload))
	}
}
