package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// cancelledSignal makes runMain's Serve return immediately.
func cancelledSignal() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx, cancel
}

func TestRunMain_BadConfig(t *testing.T) {
	t.Setenv("SILO_CSI_MODE", "sideways")
	var out, errBuf bytes.Buffer
	if code := runMain(&out, &errBuf); code != 1 {
		t.Errorf("code = %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "invalid configuration") {
		t.Errorf("stderr = %q, want a config error", errBuf.String())
	}
}

func TestRunMain_BadLogFormat(t *testing.T) {
	t.Setenv("SILO_CSI_MODE", "node")
	t.Setenv("SILO_LOG_FORMAT", "smoke-signals")
	var out, errBuf bytes.Buffer
	if code := runMain(&out, &errBuf); code != 1 {
		t.Errorf("code = %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "structured logger") {
		t.Errorf("stderr = %q, want a logger error", errBuf.String())
	}
}

func TestRunMain_DialError(t *testing.T) {
	prev := dialer
	t.Cleanup(func() { dialer = prev })
	dialer = func(string) (*grpc.ClientConn, error) { return nil, errors.New("dial boom") }

	t.Setenv("SILO_CSI_MODE", "controller")
	t.Setenv("SILO_CSI_ENDPOINT", "unix://"+filepath.Join(t.TempDir(), "csi.sock"))
	var out, errBuf bytes.Buffer
	if code := runMain(&out, &errBuf); code != 1 {
		t.Errorf("code = %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "dial boom") {
		t.Errorf("stderr = %q, want the dial failure", errBuf.String())
	}
}

func TestRunMain_ServesAllModesThenStops(t *testing.T) {
	prevSig := signalContext
	prevDial := dialer
	t.Cleanup(func() { signalContext = prevSig; dialer = prevDial })
	signalContext = cancelledSignal
	dialer = func(string) (*grpc.ClientConn, error) {
		// grpc.NewClient is lazy, so a real (unconnected) client is fine.
		return grpc.NewClient("passthrough:///silod", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	t.Setenv("SILO_CSI_MODE", "all")
	t.Setenv("SILO_CSI_NODE_ID", "node-test")
	t.Setenv("SILO_CSI_ENDPOINT", "unix://"+filepath.Join(t.TempDir(), "csi.sock"))
	var out, errBuf bytes.Buffer
	if code := runMain(&out, &errBuf); code != 0 {
		t.Errorf("code = %d, want 0 (stderr=%q)", code, errBuf.String())
	}
}

func TestRunMain_BadNBDAddr(t *testing.T) {
	t.Setenv("SILO_CSI_MODE", "node")
	t.Setenv("SILO_CSI_NODE_ID", "n")
	t.Setenv("SILO_CSI_NBD_ADDR", "garbage-no-port")
	var out, errBuf bytes.Buffer
	if code := runMain(&out, &errBuf); code != 1 {
		t.Errorf("code = %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "NBD address") {
		t.Errorf("stderr = %q, want an NBD address error", errBuf.String())
	}
}

func TestRunMain_ServeError(t *testing.T) {
	prev := signalContext
	t.Cleanup(func() { signalContext = prev })
	signalContext = cancelledSignal

	t.Setenv("SILO_CSI_MODE", "node")
	t.Setenv("SILO_CSI_NODE_ID", "n")
	t.Setenv("SILO_CSI_ENDPOINT", "http://not-a-csi-endpoint")
	var out, errBuf bytes.Buffer
	if code := runMain(&out, &errBuf); code != 1 {
		t.Errorf("code = %d, want 1 on a serve failure", code)
	}
}

func TestResolveNodeID(t *testing.T) {
	if id, err := resolveNodeID("node-x"); err != nil || id != "node-x" {
		t.Errorf("resolveNodeID(node-x) = (%q, %v)", id, err)
	}
	// Empty falls back to the host name, which is always resolvable in tests.
	if id, err := resolveNodeID(""); err != nil || id == "" {
		t.Errorf("resolveNodeID(\"\") = (%q, %v), want the host name", id, err)
	}
}
