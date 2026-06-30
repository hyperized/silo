package replication

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// When the data-plane connection cannot be created, every extent-map peer call
// fails at the client() step rather than panicking on a nil connection.
func TestExtentGRPCPeers_DialError(t *testing.T) {
	orig := newClientConn
	newClientConn = func(string, ...grpc.DialOption) (*grpc.ClientConn, error) {
		return nil, errors.New("dial boom")
	}
	t.Cleanup(func() { newClientConn = orig })

	p := NewExtentGRPCPeers(insecure.NewCredentials(), quietLog())
	ctx := context.Background()

	if err := p.Apply(ctx, "x:1", "vol", nil, false); err == nil {
		t.Error("Apply should fail when the peer client cannot be created")
	}
	if _, err := p.Fetch(ctx, "x:1", "vol"); err == nil {
		t.Error("Fetch should fail when the peer client cannot be created")
	}
	if _, _, err := p.Stat(ctx, "x:1", "vol"); err == nil {
		t.Error("Stat should fail when the peer client cannot be created")
	}
	if err := p.Delete(ctx, "x:1", "vol"); err == nil {
		t.Error("Delete should fail when the peer client cannot be created")
	}
}
