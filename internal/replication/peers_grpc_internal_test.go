package replication

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
)

func TestGRPCPeers_DialErrorSurfaces(t *testing.T) {
	prev := newClientConn
	t.Cleanup(func() { newClientConn = prev })
	newClientConn = func(string, ...grpc.DialOption) (*grpc.ClientConn, error) {
		return nil, errors.New("simulated dial failure")
	}

	p := NewGRPCPeers(insecure.NewCredentials(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := p.Store(context.Background(), "peer:7000", "c1", []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "could not create a data-plane client") {
		t.Fatalf("got %v, want a dial-failure wrap", err)
	}
}

func TestInfoFromProto_HandlesMissingTimestamp(t *testing.T) {
	info := infoFromProto(&chunkv1.ChunkInfo{ChunkId: "c1", PlainBytes: 5, StoredBytes: 11})
	if info.ID != "c1" || info.PlainBytes != 5 || info.StoredBytes != 11 {
		t.Errorf("info fields: got %+v", info)
	}
	if !info.CreatedAt.IsZero() {
		t.Errorf("CreatedAt should be zero when the wire timestamp is absent, got %v", info.CreatedAt)
	}
}
