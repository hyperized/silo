package csi

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
)

// serve returns the underlying serving error when grpc.Serve fails on its own
// (here: a listener closed before serving begins) rather than via cancellation.
// A non-nil logger also exercises the start-up log line.
func TestServer_ServeListenerError(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = lis.Close() // serving a closed listener fails immediately

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := NewServer("tcp://127.0.0.1:0", logger, WithIdentity(NewIdentityService("v1")))
	if err := srv.serve(context.Background(), lis); err == nil {
		t.Error("serve on a closed listener should return the serving error")
	}
}
