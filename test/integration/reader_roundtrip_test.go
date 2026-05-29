//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	"github.com/hyperized/silo/internal/reader"
	"github.com/hyperized/silo/internal/writer"
)

// TestReaderSDK_RoundTripAgainstRealSilod exercises the writer and reader SDKs
// together against a live silod: a multi-chunk file is streamed in through the
// writer SDK and streamed back out through the reader SDK, which resolves the
// manifest and fetches the chunks in order. The bytes must match exactly.
func TestReaderSDK_RoundTripAgainstRealSilod(t *testing.T) {
	node := startSilod(t)
	defer node.teardown()

	caPEM, certPEM, keyPEM := claimClientCert(t, node)
	conn, closeConn := mtlsConn(t, node, caPEM, certPEM, keyPEM)
	defer closeConn()

	chunks := chunkv1.NewChunkStoreClient(conn)
	ns := namespacev1.NewNamespaceStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const (
		path      = "/archive.bin"
		chunkSize = 4096
	)
	payload := bytes.Repeat([]byte("reader-sdk-roundtrip|"), 700) // 14700 bytes -> 4 chunks

	w, err := writer.New(chunks, ns, "integration", writer.WithChunkSize(chunkSize))
	if err != nil {
		t.Fatalf("writer.New: %v", err)
	}
	st, err := w.Create(ctx, path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.Copy(st, bytes.NewReader(payload)); err != nil {
		t.Fatalf("write io.Copy: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rs, err := reader.New(chunks, ns).Open(ctx, path)
	if err != nil {
		t.Fatalf("reader Open: %v", err)
	}
	defer rs.Close()

	var out bytes.Buffer
	if _, err := io.Copy(&out, rs); err != nil {
		t.Fatalf("read io.Copy: %v", err)
	}
	if !bytes.Equal(out.Bytes(), payload) {
		t.Errorf("round-trip mismatch: read %d bytes, wrote %d", out.Len(), len(payload))
	}
}
