package csi_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
	"github.com/hyperized/silo/internal/csi"
)

func TestServer_ServesIdentityOverSocket(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "csi.sock")
	endpoint := "unix://" + socket

	srv := csi.NewServer(endpoint, nil,
		csi.WithIdentity(csi.NewIdentityService("v9")),
		csi.WithController(csi.NewControllerService(&fakeStore{})),
		csi.WithNode(csi.NewNodeService("node-1", &fakeAttacher{}, &fakeMounter{})),
	)

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx) }()

	conn := dialUnix(t, socket)
	defer func() { _ = conn.Close() }()

	resp, err := csiv1.NewIdentityClient(conn).GetPluginInfo(context.Background(), &csiv1.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo over socket: %v", err)
	}
	if resp.GetName() != csi.DriverName || resp.GetVendorVersion() != "v9" {
		t.Errorf("plugin info = %+v", resp)
	}

	// The Controller service is reachable too.
	if _, err := csiv1.NewControllerClient(conn).ControllerGetCapabilities(context.Background(), &csiv1.ControllerGetCapabilitiesRequest{}); err != nil {
		t.Errorf("ControllerGetCapabilities over socket: %v", err)
	}

	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("Serve returned %v, want nil on graceful stop", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Serve did not return after cancellation")
	}
}

func TestServer_BadEndpoint(t *testing.T) {
	if err := csi.NewServer("http://nope", nil).Serve(context.Background()); err == nil {
		t.Error("an endpoint without a unix:// or tcp:// scheme should error")
	}
}

func TestServer_ListenError(t *testing.T) {
	// A unix socket under a directory that does not exist cannot be bound.
	endpoint := "unix://" + filepath.Join(t.TempDir(), "missing-dir", "csi.sock")
	if err := csi.NewServer(endpoint, nil, csi.WithIdentity(csi.NewIdentityService("v1"))).Serve(context.Background()); err == nil {
		t.Error("Serve should fail to listen on an unbindable socket")
	}
}

func TestServer_StaleSocketRemoveError(t *testing.T) {
	// Point the endpoint at a non-empty directory: removing it (as the stale
	// socket) fails, which Serve reports rather than silently proceeding.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "keep"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := csi.NewServer("unix://"+dir, nil, csi.WithIdentity(csi.NewIdentityService("v1"))).Serve(context.Background()); err == nil {
		t.Error("Serve should report a stale-socket removal failure")
	}
}

func TestServer_TCPEndpoint(t *testing.T) {
	srv := csi.NewServer("tcp://127.0.0.1:0", nil, csi.WithIdentity(csi.NewIdentityService("v1")))
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx) }()
	time.Sleep(50 * time.Millisecond) // let it bind
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("tcp Serve = %v, want nil on graceful stop", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("tcp Serve did not return after cancellation")
	}
}

func dialUnix(t *testing.T, socket string) *grpc.ClientConn {
	t.Helper()
	// Retry briefly: Serve binds the socket in a goroutine.
	var lastErr error
	for range 50 {
		conn, err := grpc.NewClient(
			"unix://"+socket,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			}),
		)
		if err != nil {
			lastErr = err
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return conn
	}
	t.Fatalf("could not dial CSI socket: %v", lastErr)
	return nil
}
