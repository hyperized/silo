package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hyperized/silo/internal/csi"
)

// cancelledSignal makes runMain's Serve return immediately.
func cancelledSignal() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx, cancel
}

// fakeAttacher stands in for the NBD attacher so node mode can run on hosts
// without a Linux NBD stack (CI, macOS).
type fakeAttacher struct{}

func (fakeAttacher) Attach(context.Context, string) (string, error) { return "/dev/nbd0", nil }
func (fakeAttacher) Detach(context.Context, string) error           { return nil }

// useFakeAttacher swaps the attacher seam for the duration of a test.
func useFakeAttacher(t *testing.T) {
	t.Helper()
	prev := newAttacher
	t.Cleanup(func() { newAttacher = prev })
	newAttacher = func(csi.Config, *slog.Logger) (csi.VolumeAttacher, func() error, error) {
		return fakeAttacher{}, func() error { return nil }, nil
	}
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
	useFakeAttacher(t)
	dialer = func(string) (*grpc.ClientConn, error) {
		// grpc.NewClient is lazy, so a real (unconnected) client is fine.
		return grpc.NewClient("passthrough:///silod", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	t.Setenv("SILO_CSI_MODE", "all")
	t.Setenv("SILO_CSI_NODE_ID", "node-test")
	t.Setenv("SILO_CSI_ENDPOINT", "unix://"+shortSocketPath(t))
	var out, errBuf bytes.Buffer
	if code := runMain(&out, &errBuf); code != 0 {
		t.Errorf("code = %d, want 0 (stderr=%q, out=%q)", code, errBuf.String(), out.String())
	}
}

// shortSocketPath returns a socket path short enough for every OS —
// t.TempDir's long test-name paths overflow Darwin's 104-byte sun_path limit.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "csi")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "csi.sock")
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
	useFakeAttacher(t)

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
