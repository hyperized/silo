//go:build integration

// Package integration runs end-to-end tests against the real silod
// binary. They are kept under a build tag because they:
//   - require `go build` to have produced bin/silod (or they build it),
//   - spawn an OS process,
//   - bind real TCP ports,
//   - touch the filesystem outside of the test's working dir.
//
// Run with: make test-integration
package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
)

// startSilod builds silod into a temp path, launches it with an
// ephemeral encryption key + temp data dir, and returns the gRPC
// address plus a teardown function.
func startSilod(t *testing.T) (grpcAddr string, teardown func()) {
	t.Helper()

	silodBin := filepath.Join(t.TempDir(), "silod")
	cmdBuild := exec.Command("go", "build", "-o", silodBin, "../../cmd/silod")
	cmdBuild.Stderr = os.Stderr
	if err := cmdBuild.Run(); err != nil {
		t.Fatalf("go build silod: %v", err)
	}

	httpAddr := freePort(t)
	grpcAddr = freePort(t)
	dataDir := t.TempDir()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}

	cmd := exec.Command(silodBin)
	cmd.Env = append(os.Environ(),
		"SILO_NODE_ID=integration",
		"SILO_HTTP_ADDR="+httpAddr,
		"SILO_GRPC_ADDR="+grpcAddr,
		"SILO_DATA_DIR="+dataDir,
		"SILO_ENCRYPTION_KEY="+base64.StdEncoding.EncodeToString(key),
		"SILO_LOG_LEVEL=warn",
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start silod: %v", err)
	}

	if err := waitForGRPC(grpcAddr, 5*time.Second); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("silod did not become reachable on %s within 5s: %v", grpcAddr, err)
	}

	teardown = func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}
	return grpcAddr, teardown
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// waitForGRPC polls until a TCP dial to addr succeeds, mirroring the
// usual k8s readiness pattern.
func waitForGRPC(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout dialing %s", addr)
}

func TestChunkRoundTrip_AgainstRealSilodBinary(t *testing.T) {
	addr, teardown := startSilod(t)
	defer teardown()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := chunkv1.NewChunkStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload := bytes.Repeat([]byte("silo-integration-"), 1024) // ~17 KiB
	const id = "integration-demo"

	// Put.
	putStream, err := client.Put(ctx)
	if err != nil {
		t.Fatalf("Put open: %v", err)
	}
	if err := putStream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Header{Header: &chunkv1.PutHeader{ChunkId: id}}}); err != nil {
		t.Fatalf("send header: %v", err)
	}
	if err := putStream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Data{Data: payload}}); err != nil {
		t.Fatalf("send data: %v", err)
	}
	putResp, err := putStream.CloseAndRecv()
	if err != nil {
		t.Fatalf("Put close: %v", err)
	}
	if putResp.Info.PlainBytes != int64(len(payload)) {
		t.Errorf("put plain_bytes: got %d, want %d", putResp.Info.PlainBytes, len(payload))
	}

	// Get.
	getStream, err := client.Get(ctx, &chunkv1.GetRequest{ChunkId: id})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var received bytes.Buffer
	for {
		msg, err := getStream.Recv()
		if err != nil {
			break
		}
		if d := msg.GetData(); len(d) > 0 {
			received.Write(d)
		}
	}
	if !bytes.Equal(received.Bytes(), payload) {
		t.Errorf("get round-trip mismatch (%d vs %d bytes)", received.Len(), len(payload))
	}

	// Delete.
	if _, err := client.Delete(ctx, &chunkv1.DeleteRequest{ChunkId: id}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}
