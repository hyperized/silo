package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crypto"
)

// newTestServer wires a real grpc.Server bound to an ephemeral port,
// returns a client and a teardown. Single helper because the streaming
// nature of Put/Get makes pure-stub testing too noisy to be useful.
func newTestServer(t *testing.T) (chunkv1.ChunkStoreClient, func()) {
	t.Helper()
	key := make([]byte, crypto.ClusterKeyBytes)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	cipher, err := crypto.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	store, err := chunkstore.NewFileStore(t.TempDir(), cipher)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := NewChunkService(store, logger)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	chunkv1.RegisterChunkStoreServer(s, svc)
	go func() {
		_ = s.Serve(ln)
	}()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.Stop()
		_ = ln.Close()
		t.Fatalf("dial: %v", err)
	}
	client := chunkv1.NewChunkStoreClient(conn)

	teardown := func() {
		_ = conn.Close()
		s.GracefulStop()
		_ = ln.Close()
	}
	return client, teardown
}

func putBytes(t *testing.T, client chunkv1.ChunkStoreClient, id string, data []byte) *chunkv1.PutResponse {
	t.Helper()
	stream, err := client.Put(context.Background())
	if err != nil {
		t.Fatalf("Put open: %v", err)
	}
	if err := stream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Header{Header: &chunkv1.PutHeader{ChunkId: id}}}); err != nil {
		t.Fatalf("send header: %v", err)
	}
	for off := 0; off < len(data); off += 32 * 1024 {
		end := off + 32*1024
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Data{Data: data[off:end]}}); err != nil {
			t.Fatalf("send data: %v", err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	return resp
}

func getBytes(t *testing.T, client chunkv1.ChunkStoreClient, id string) ([]byte, *chunkv1.ChunkInfo) {
	t.Helper()
	stream, err := client.Get(context.Background(), &chunkv1.GetRequest{ChunkId: id})
	if err != nil {
		t.Fatalf("Get open: %v", err)
	}
	var info *chunkv1.ChunkInfo
	var data bytes.Buffer
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Get recv: %v", err)
		}
		switch body := msg.Body.(type) {
		case *chunkv1.GetResponse_Info:
			info = body.Info
		case *chunkv1.GetResponse_Data:
			data.Write(body.Data)
		}
	}
	return data.Bytes(), info
}

func TestChunkService_RoundTrip(t *testing.T) {
	client, teardown := newTestServer(t)
	defer teardown()

	payload := []byte("hello, gRPC chunks")
	resp := putBytes(t, client, "demo", payload)
	if resp.Info.ChunkId != "demo" {
		t.Errorf("Put info.chunk_id: got %q, want demo", resp.Info.ChunkId)
	}
	if resp.Info.PlainBytes != int64(len(payload)) {
		t.Errorf("Put info.plain_bytes: got %d, want %d", resp.Info.PlainBytes, len(payload))
	}

	got, info, err := func() ([]byte, *chunkv1.ChunkInfo, error) {
		got, info := getBytes(t, client, "demo")
		return got, info, nil
	}()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("Get round-trip mismatch")
	}
	if info == nil || info.ChunkId != "demo" {
		t.Errorf("Get info: %+v", info)
	}

	stat, err := client.Stat(context.Background(), &chunkv1.StatRequest{ChunkId: "demo"})
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Info.PlainBytes != int64(len(payload)) {
		t.Errorf("Stat plain_bytes: got %d, want %d", stat.Info.PlainBytes, len(payload))
	}

	if _, err := client.Delete(context.Background(), &chunkv1.DeleteRequest{ChunkId: "demo"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = client.Stat(context.Background(), &chunkv1.StatRequest{ChunkId: "demo"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Stat after Delete: got %v, want NotFound", err)
	}
}

func TestChunkService_LargePayloadStreams(t *testing.T) {
	client, teardown := newTestServer(t)
	defer teardown()

	payload := bytes.Repeat([]byte{'x'}, 1<<20) // 1 MiB
	putBytes(t, client, "big", payload)
	got, _ := getBytes(t, client, "big")
	if !bytes.Equal(got, payload) {
		t.Fatalf("large payload round-trip mismatch")
	}
}

func TestChunkService_PutValidations(t *testing.T) {
	client, teardown := newTestServer(t)
	defer teardown()

	cases := []struct {
		name string
		send func(stream chunkv1.ChunkStore_PutClient) error
		want codes.Code
	}{
		{
			name: "no messages sent",
			send: func(s chunkv1.ChunkStore_PutClient) error { return nil },
			want: codes.InvalidArgument,
		},
		{
			name: "data before header",
			send: func(s chunkv1.ChunkStore_PutClient) error {
				return s.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Data{Data: []byte("oops")}})
			},
			want: codes.InvalidArgument,
		},
		{
			name: "duplicate header",
			send: func(s chunkv1.ChunkStore_PutClient) error {
				if err := s.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Header{Header: &chunkv1.PutHeader{ChunkId: "a"}}}); err != nil {
					return err
				}
				return s.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Header{Header: &chunkv1.PutHeader{ChunkId: "b"}}})
			},
			want: codes.InvalidArgument,
		},
		{
			name: "empty header chunk_id",
			send: func(s chunkv1.ChunkStore_PutClient) error {
				return s.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Header{Header: &chunkv1.PutHeader{ChunkId: ""}}})
			},
			want: codes.InvalidArgument,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream, err := client.Put(context.Background())
			if err != nil {
				t.Fatalf("Put open: %v", err)
			}
			if err := tc.send(stream); err != nil {
				t.Fatalf("send: %v", err)
			}
			_, err = stream.CloseAndRecv()
			if status.Code(err) != tc.want {
				t.Errorf("got code %v (%v), want %v", status.Code(err), err, tc.want)
			}
		})
	}
}

func TestChunkService_NotFoundMappings(t *testing.T) {
	client, teardown := newTestServer(t)
	defer teardown()
	ctx := context.Background()

	stream, err := client.Get(ctx, &chunkv1.GetRequest{ChunkId: "missing"})
	if err != nil {
		t.Fatalf("Get open: %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.NotFound {
		t.Errorf("Get missing: got %v, want NotFound", err)
	}

	if _, err := client.Stat(ctx, &chunkv1.StatRequest{ChunkId: "missing"}); status.Code(err) != codes.NotFound {
		t.Errorf("Stat missing: got %v, want NotFound", err)
	}

	if _, err := client.Delete(ctx, &chunkv1.DeleteRequest{ChunkId: "missing"}); status.Code(err) != codes.NotFound {
		t.Errorf("Delete missing: got %v, want NotFound", err)
	}
}

func TestChunkService_InvalidIDMappings(t *testing.T) {
	client, teardown := newTestServer(t)
	defer teardown()
	ctx := context.Background()

	stream, err := client.Get(ctx, &chunkv1.GetRequest{ChunkId: "bad/id"})
	if err != nil {
		t.Fatalf("Get open: %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Get invalid: got %v, want InvalidArgument", err)
	}

	if _, err := client.Stat(ctx, &chunkv1.StatRequest{ChunkId: "bad/id"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Stat invalid: got %v, want InvalidArgument", err)
	}

	if _, err := client.Delete(ctx, &chunkv1.DeleteRequest{ChunkId: "bad/id"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Delete invalid: got %v, want InvalidArgument", err)
	}
}

func TestMapStoreError(t *testing.T) {
	cases := []struct {
		in   error
		want codes.Code
	}{
		{chunkstore.ErrNotFound, codes.NotFound},
		{chunkstore.ErrInvalidID, codes.InvalidArgument},
		{errors.New("disk on fire"), codes.Internal},
	}
	for _, tc := range cases {
		err := mapStoreError(tc.in, "x")
		if status.Code(err) != tc.want {
			t.Errorf("mapStoreError(%v): got %v, want %v", tc.in, status.Code(err), tc.want)
		}
	}
}

func TestGRPCServer_LifecycleAndAddrIsRaceFree(t *testing.T) {
	key := make([]byte, crypto.ClusterKeyBytes)
	_, _ = rand.Read(key)
	cipher, _ := crypto.NewCipher(key)
	store, _ := chunkstore.NewFileStore(t.TempDir(), cipher)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := NewGRPCServer("127.0.0.1:0", store, logger)

	if srv.Addr() != "" {
		t.Errorf("Addr before Start: got %q, want empty", srv.Addr())
	}

	startCh := make(chan error, 1)
	go func() { startCh <- srv.Start() }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Addr() != "" {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("server did not bind within 2s")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}

	select {
	case err := <-startCh:
		if err != nil {
			t.Errorf("Start after Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("Start did not return within 1s of Shutdown")
	}
}

func TestGRPCServer_StartFailsOnBadAddress(t *testing.T) {
	key := make([]byte, crypto.ClusterKeyBytes)
	_, _ = rand.Read(key)
	cipher, _ := crypto.NewCipher(key)
	store, _ := chunkstore.NewFileStore(t.TempDir(), cipher)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv := NewGRPCServer("not-an-address", store, logger)
	err := srv.Start()
	if err == nil || !strings.Contains(err.Error(), "SILO_GRPC_ADDR") {
		t.Errorf("expected SILO_GRPC_ADDR error, got %v", err)
	}
}

func TestGRPCServer_ShutdownDeadlineForcesStop(t *testing.T) {
	key := make([]byte, crypto.ClusterKeyBytes)
	_, _ = rand.Read(key)
	cipher, _ := crypto.NewCipher(key)
	store, _ := chunkstore.NewFileStore(t.TempDir(), cipher)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv := NewGRPCServer("127.0.0.1:0", store, logger)
	go func() { _ = srv.Start() }()
	// Wait until bound.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Addr() != "" {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Open a hanging Get RPC so GracefulStop has something to wait for.
	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := chunkv1.NewChunkStoreClient(conn)
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	stream, err := client.Get(streamCtx, &chunkv1.GetRequest{ChunkId: "any"})
	if err == nil {
		// Drain one msg if available (probably an error since "any" doesn't exist).
		_, _ = stream.Recv()
	}

	// Use an already-expired context to force the timeout path.
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := srv.Shutdown(expired); err == nil || !strings.Contains(err.Error(), "deadline expired") {
		t.Errorf("Shutdown with expired ctx: got %v, want deadline-expired error", err)
	}
}
