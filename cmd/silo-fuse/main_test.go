package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hyperized/silo/pkg/fuse"
)

// eofConn is a fuse.Conn that immediately reports EOF, so Serve returns at once.
type eofConn struct{}

func (eofConn) ReadRequest() ([]byte, error) { return nil, io.EOF }
func (eofConn) WriteResponse([]byte) error   { return nil }
func (eofConn) Close() error                 { return nil }

func TestRunMain_Usage(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runMain(nil, &out, &errBuf); code != 2 {
		t.Errorf("no args code = %d, want 2", code)
	}
	if !strings.Contains(out.String(), "Usage: silo-fuse") {
		t.Error("usage should be printed")
	}
	out.Reset()
	if code := runMain([]string{"help"}, &out, &errBuf); code != 0 {
		t.Errorf("help code = %d, want 0", code)
	}
}

func TestRunMain_DialError(t *testing.T) {
	prev := dialer
	t.Cleanup(func() { dialer = prev })
	dialer = func(string) (*grpc.ClientConn, error) { return nil, errors.New("dial boom") }

	var out, errBuf bytes.Buffer
	if code := runMain([]string{"/mnt/silo"}, &out, &errBuf); code != 1 {
		t.Errorf("dial-error code = %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "could not dial") {
		t.Errorf("stderr = %q, want a dial failure", errBuf.String())
	}
}

func TestRunMain_MountError(t *testing.T) {
	prevDial, prevMount := dialer, mountFn
	t.Cleanup(func() { dialer = prevDial; mountFn = prevMount })
	dialer = func(string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///silod", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	mountFn = func(string) (fuse.Conn, error) { return nil, errors.New("no /dev/fuse") }

	t.Setenv("SILO_NODE_ID", "node-test")
	var out, errBuf bytes.Buffer
	if code := runMain([]string{"/mnt/silo"}, &out, &errBuf); code != 1 {
		t.Errorf("mount-error code = %d, want 1", code)
	}
}

func TestRunMain_ServesThenStops(t *testing.T) {
	prevDial, prevMount, prevSig := dialer, mountFn, signalContext
	t.Cleanup(func() { dialer = prevDial; mountFn = prevMount; signalContext = prevSig })
	dialer = func(string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///silod", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	mountFn = func(string) (fuse.Conn, error) { return eofConn{}, nil }
	signalContext = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, cancel
	}

	t.Setenv("SILO_NODE_ID", "node-test")
	var out, errBuf bytes.Buffer
	if code := runMain([]string{"/mnt/silo"}, &out, &errBuf); code != 0 {
		t.Errorf("serve code = %d, want 0 (stderr=%q)", code, errBuf.String())
	}
}

func TestRunMain_BadLogFormat(t *testing.T) {
	t.Setenv("SILO_LOG_FORMAT", "smoke-signals")
	var out, errBuf bytes.Buffer
	if code := runMain([]string{"/mnt/silo"}, &out, &errBuf); code != 1 {
		t.Errorf("bad-log-format code = %d, want 1", code)
	}
}

func TestResolveNodeID(t *testing.T) {
	if id, err := resolveNodeID("node-x"); err != nil || id != "node-x" {
		t.Errorf("resolveNodeID(node-x) = (%q, %v)", id, err)
	}
	if id, err := resolveNodeID(""); err != nil || id == "" {
		t.Errorf("resolveNodeID(\"\") = (%q, %v), want the host name", id, err)
	}
}
