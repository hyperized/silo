package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	nodev1 "github.com/hyperized/silo/api/proto/silo/node/v1"
)

func TestNode_UsageAndUnknown(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runNode(nil, &out, &errBuf); code != 2 {
		t.Errorf("no args code = %d, want 2", code)
	}
	if !strings.Contains(out.String(), "siloctl node") {
		t.Error("usage should print on no args")
	}
	out.Reset()
	if code := runNode([]string{"help"}, &out, &errBuf); code != 0 {
		t.Errorf("help code = %d, want 0", code)
	}
	errBuf.Reset()
	if code := runNode([]string{"frobnicate"}, &out, &errBuf); code != 2 || !strings.Contains(errBuf.String(), "unknown subcommand") {
		t.Errorf("unknown subcommand code = %d", code)
	}
}

func TestNodeDrain_Succeeds(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()

	var out, errBuf bytes.Buffer
	if code := runNode([]string{"drain", "--server=" + addr}, &out, &errBuf); code != 0 {
		t.Fatalf("drain code = %d, err = %s", code, errBuf.String())
	}
	got := out.String()
	if !strings.Contains(got, "Node silo-a is draining") || !strings.Contains(got, "shortfall") {
		t.Errorf("drain output = %q, want a draining confirmation with guidance", got)
	}
}

func TestNodeDrain_UsageErrors(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runNode([]string{"drain", "extra"}, &out, &errBuf); code != 2 {
		t.Errorf("trailing-arg code = %d, want 2", code)
	}
	errBuf.Reset()
	if code := runNode([]string{"drain", "--bogus"}, &out, &errBuf); code != 2 {
		t.Errorf("bad-flag code = %d, want 2", code)
	}
}

func TestNodeDrain_DialError(t *testing.T) {
	prev := dialer
	t.Cleanup(func() { dialer = prev })
	dialer = func(string) (*grpc.ClientConn, error) { return nil, errors.New("dial boom") }

	var out, errBuf bytes.Buffer
	if code := runNode([]string{"drain", "--server=x"}, &out, &errBuf); code != 1 {
		t.Errorf("dial-error code = %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "could not dial") {
		t.Errorf("stderr = %q, want a dial failure", errBuf.String())
	}
}

// errNodeAdminClient and alreadyDrainingClient exercise the RPC-error and
// already-draining branches without a live server.
type errNodeAdminClient struct{}

func (errNodeAdminClient) Drain(context.Context, *nodev1.DrainRequest, ...grpc.CallOption) (*nodev1.DrainResponse, error) {
	return nil, status.Error(codes.Unavailable, "silod is down")
}

type alreadyDrainingClient struct{}

func (alreadyDrainingClient) Drain(context.Context, *nodev1.DrainRequest, ...grpc.CallOption) (*nodev1.DrainResponse, error) {
	return &nodev1.DrainResponse{NodeId: "silo-a", Announced: false}, nil
}

func TestNodeDrain_RPCErrorAndAlreadyDraining(t *testing.T) {
	prevDial, prevClient := dialer, newNodeAdminClient
	t.Cleanup(func() { dialer = prevDial; newNodeAdminClient = prevClient })
	dialer = func(string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///x", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	newNodeAdminClient = func(*grpc.ClientConn) nodev1.NodeAdminClient { return errNodeAdminClient{} }
	var out, errBuf bytes.Buffer
	if code := runNode([]string{"drain", "--server=x"}, &out, &errBuf); code == 0 {
		t.Error("an RPC failure should be a non-zero exit")
	}

	newNodeAdminClient = func(*grpc.ClientConn) nodev1.NodeAdminClient { return alreadyDrainingClient{} }
	out.Reset()
	errBuf.Reset()
	if code := runNode([]string{"drain", "--server=x"}, &out, &errBuf); code != 0 {
		t.Errorf("already-draining code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "already draining") {
		t.Errorf("output = %q, want already-draining message", out.String())
	}
}
