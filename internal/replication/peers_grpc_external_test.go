package replication_test

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crypto"
	"github.com/hyperized/silo/internal/replication"
	"github.com/hyperized/silo/internal/transport"
)

// errCoordinator fails if the replica path ever routes through the
// coordinator: peer Store/Fetch must hit the local store directly.
type errCoordinator struct{}

func (errCoordinator) Write(context.Context, string, []byte) (chunkstore.Info, error) {
	return chunkstore.Info{}, errors.New("coordinator must not be used for peer replica traffic")
}

func (errCoordinator) Read(context.Context, string) ([]byte, chunkstore.Info, error) {
	return nil, chunkstore.Info{}, errors.New("coordinator must not be used for peer replica traffic")
}

func (errCoordinator) Delete(context.Context, string) error {
	return errors.New("coordinator must not be used for peer replica traffic")
}

func (errCoordinator) Stat(context.Context, string) (chunkstore.Info, error) {
	return chunkstore.Info{}, errors.New("coordinator must not be used for peer replica traffic")
}

// startReplicaServer runs a real ChunkStore over insecure gRPC backed by a
// fresh encrypted FileStore, returning its dial address and a stopper.
func startReplicaServer(t *testing.T) string {
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
	svc := transport.NewChunkService(store, errCoordinator{}, discardLogger())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	chunkv1.RegisterChunkStoreServer(s, svc)
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() {
		s.Stop()
		_ = ln.Close()
	})
	return ln.Addr().String()
}

func TestGRPCPeers_StoreFetchRoundTrip(t *testing.T) {
	addr := startReplicaServer(t)
	peers := replication.NewGRPCPeers(insecure.NewCredentials(), discardLogger())
	t.Cleanup(func() { _ = peers.Close() })

	payload := []byte("replicated bytes across the data plane")
	info, err := peers.Store(context.Background(), addr, "c1", payload)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if info.PlainBytes != int64(len(payload)) {
		t.Errorf("Store info.PlainBytes: got %d, want %d", info.PlainBytes, len(payload))
	}

	data, info2, err := peers.Fetch(context.Background(), addr, "c1")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(data) != string(payload) {
		t.Errorf("Fetch data: got %q, want %q", data, payload)
	}
	if info2.PlainBytes != int64(len(payload)) || info2.CreatedAt.IsZero() {
		t.Errorf("Fetch info: %+v", info2)
	}

	// A second call reuses the cached connection rather than re-dialing.
	if _, _, err := peers.Fetch(context.Background(), addr, "c1"); err != nil {
		t.Fatalf("second Fetch: %v", err)
	}

	// Stat reports the chunk present, and reports an error for one that is
	// absent — the scrubber relies on both.
	if _, err := peers.Stat(context.Background(), addr, "c1"); err != nil {
		t.Errorf("Stat of an existing chunk: %v", err)
	}
	if _, err := peers.Stat(context.Background(), addr, "ghost"); err == nil {
		t.Error("Stat of a missing chunk should error")
	}
}

func TestGRPCPeers_DeleteRoundTrip(t *testing.T) {
	addr := startReplicaServer(t)
	peers := replication.NewGRPCPeers(insecure.NewCredentials(), discardLogger())
	t.Cleanup(func() { _ = peers.Close() })

	if _, err := peers.Store(context.Background(), addr, "c1", []byte("bytes")); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := peers.Delete(context.Background(), addr, "c1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := peers.Stat(context.Background(), addr, "c1"); err == nil {
		t.Error("chunk should be gone after Delete")
	}
}

func TestGRPCPeers_DeleteMissingMapsToErrNotFound(t *testing.T) {
	addr := startReplicaServer(t)
	peers := replication.NewGRPCPeers(insecure.NewCredentials(), discardLogger())
	t.Cleanup(func() { _ = peers.Close() })

	// A delete of an absent chunk must come back as the sentinel so the
	// coordinator can treat it as already-deleted.
	if err := peers.Delete(context.Background(), addr, "ghost"); !errors.Is(err, chunkstore.ErrNotFound) {
		t.Fatalf("got %v, want chunkstore.ErrNotFound", err)
	}
}

func TestGRPCPeers_DeleteUnreachablePeer(t *testing.T) {
	peers := replication.NewGRPCPeers(insecure.NewCredentials(), discardLogger())
	t.Cleanup(func() { _ = peers.Close() })

	if err := peers.Delete(context.Background(), "127.0.0.1:1", "c1"); err == nil || errors.Is(err, chunkstore.ErrNotFound) {
		t.Fatalf("got %v, want a non-NotFound transport error", err)
	}
}

func TestGRPCPeers_FetchMissingChunk(t *testing.T) {
	addr := startReplicaServer(t)
	peers := replication.NewGRPCPeers(insecure.NewCredentials(), discardLogger())
	t.Cleanup(func() { _ = peers.Close() })

	if _, _, err := peers.Fetch(context.Background(), addr, "ghost"); err == nil {
		t.Fatal("Fetch of a missing chunk should error")
	}
}

func TestGRPCPeers_StoreToUnreachablePeer(t *testing.T) {
	peers := replication.NewGRPCPeers(insecure.NewCredentials(), discardLogger())
	t.Cleanup(func() { _ = peers.Close() })

	// Port 1 refuses connections fast, so this fails without a long wait.
	if _, err := peers.Store(context.Background(), "127.0.0.1:1", "c1", []byte("x")); err == nil {
		t.Fatal("Store to an unreachable peer should error")
	}
}

func TestGRPCPeers_FetchFromUnreachablePeer(t *testing.T) {
	peers := replication.NewGRPCPeers(insecure.NewCredentials(), discardLogger())
	t.Cleanup(func() { _ = peers.Close() })

	if _, _, err := peers.Fetch(context.Background(), "127.0.0.1:1", "c1"); err == nil {
		t.Fatal("Fetch from an unreachable peer should error")
	}
}
