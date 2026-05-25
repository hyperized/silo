package main

import (
	"bytes"
	"crypto/rand"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crypto"
	"github.com/hyperized/silo/internal/transport"
)

// newTestServer mirrors the transport test helper: real gRPC server, real
// chunk store under a tempdir, returned address ready for --server=.
func newTestServer(t *testing.T) (addr string, teardown func()) {
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
	svc := transport.NewChunkService(store, logger)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	chunkv1.RegisterChunkStoreServer(s, svc)
	go func() { _ = s.Serve(ln) }()

	teardown = func() {
		s.GracefulStop()
		_ = ln.Close()
	}
	return ln.Addr().String(), teardown
}

func TestRunMain_NoArgsShowsUsage(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := runMain(nil, nil, &out, &errBuf)
	if code != 2 {
		t.Errorf("exit code: got %d, want 2", code)
	}
	if !strings.Contains(out.String(), "siloctl") {
		t.Errorf("usage should mention siloctl, got %q", out.String())
	}
}

func TestRunMain_Help(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runMain([]string{"help"}, nil, &out, &errBuf); code != 0 {
		t.Errorf("help exit: got %d, want 0", code)
	}
	if !strings.Contains(out.String(), "Commands:") {
		t.Errorf("help should list commands, got %q", out.String())
	}
}

func TestRunMain_Version(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runMain([]string{"version"}, nil, &out, &errBuf); code != 0 {
		t.Errorf("version exit: got %d, want 0", code)
	}
	if !strings.Contains(out.String(), "siloctl") {
		t.Errorf("version output should mention siloctl, got %q", out.String())
	}
}

func TestRunMain_UnknownCommand(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runMain([]string{"banana"}, nil, &out, &errBuf); code != 2 {
		t.Errorf("unknown command exit: got %d, want 2", code)
	}
	if !strings.Contains(errBuf.String(), "unknown command") {
		t.Errorf("stderr should explain the failure, got %q", errBuf.String())
	}
}

func TestRunChunk_NoArgsShowsUsage(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runMain([]string{"chunk"}, nil, &out, &errBuf); code != 2 {
		t.Errorf("chunk no-args exit: got %d, want 2", code)
	}
	if !strings.Contains(out.String(), "siloctl chunk") {
		t.Errorf("chunk help missing, got %q", out.String())
	}
}

func TestRunChunk_Help(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runMain([]string{"chunk", "help"}, nil, &out, &errBuf); code != 0 {
		t.Errorf("chunk help exit: got %d, want 0", code)
	}
}

func TestRunChunk_UnknownSubcommand(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runMain([]string{"chunk", "smell"}, nil, &out, &errBuf); code != 2 {
		t.Errorf("unknown subcommand exit: got %d, want 2", code)
	}
}

func TestRunChunk_PutGetStatDelete_RoundTrip(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()

	// Stage a file for `chunk put`.
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "src.bin")
	payload := []byte("the cli round trips through grpc")
	if err := os.WriteFile(src, payload, 0o600); err != nil {
		t.Fatalf("write src: %v", err)
	}

	// put
	{
		var out, errBuf bytes.Buffer
		code := runMain([]string{"chunk", "put", "--server", addr, "cli-demo", src}, nil, &out, &errBuf)
		if code != 0 {
			t.Fatalf("put: code=%d stderr=%q", code, errBuf.String())
		}
		if !strings.Contains(out.String(), "Stored chunk cli-demo") {
			t.Errorf("put stdout missing confirmation: %q", out.String())
		}
	}

	// stat
	{
		var out, errBuf bytes.Buffer
		code := runMain([]string{"chunk", "stat", "--server", addr, "cli-demo"}, nil, &out, &errBuf)
		if code != 0 {
			t.Fatalf("stat: code=%d stderr=%q", code, errBuf.String())
		}
		if !strings.Contains(out.String(), "plaintext bytes:") {
			t.Errorf("stat stdout missing plaintext bytes: %q", out.String())
		}
	}

	// get to a file
	{
		dst := filepath.Join(tmpDir, "dst.bin")
		var out, errBuf bytes.Buffer
		code := runMain([]string{"chunk", "get", "--server", addr, "cli-demo", dst}, nil, &out, &errBuf)
		if code != 0 {
			t.Fatalf("get: code=%d stderr=%q", code, errBuf.String())
		}
		got, err := os.ReadFile(dst)
		if err != nil {
			t.Fatalf("read dst: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("get round-trip mismatch: got %q, want %q", got, payload)
		}
	}

	// get to stdout
	{
		var out, errBuf bytes.Buffer
		code := runMain([]string{"chunk", "get", "--server", addr, "cli-demo"}, nil, &out, &errBuf)
		if code != 0 {
			t.Fatalf("get-stdout: code=%d stderr=%q", code, errBuf.String())
		}
		if !bytes.Equal(out.Bytes(), payload) {
			t.Errorf("get-stdout payload mismatch")
		}
	}

	// delete
	{
		var out, errBuf bytes.Buffer
		code := runMain([]string{"chunk", "delete", "--server", addr, "cli-demo"}, nil, &out, &errBuf)
		if code != 0 {
			t.Fatalf("delete: code=%d stderr=%q", code, errBuf.String())
		}
		if !strings.Contains(out.String(), "Deleted chunk cli-demo") {
			t.Errorf("delete stdout missing confirmation: %q", out.String())
		}
	}

	// stat after delete
	{
		var out, errBuf bytes.Buffer
		code := runMain([]string{"chunk", "stat", "--server", addr, "cli-demo"}, nil, &out, &errBuf)
		if code == 0 {
			t.Fatalf("stat after delete should fail, code=%d stderr=%q", code, errBuf.String())
		}
		if !strings.Contains(errBuf.String(), "not found") {
			t.Errorf("stat-after-delete stderr should mention not found: %q", errBuf.String())
		}
	}
}

func TestRunChunk_PutFromStdin(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()

	stdin := strings.NewReader("payload from stdin")
	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "put", "--server", addr, "stdin-chunk", "-"}, stdin, &out, &errBuf)
	if code != 0 {
		t.Fatalf("put stdin: code=%d stderr=%q", code, errBuf.String())
	}
	if !strings.Contains(out.String(), "Stored chunk stdin-chunk") {
		t.Errorf("put stdin missing confirmation: %q", out.String())
	}
}

func TestRunChunk_PutMissingFile(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()

	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "put", "--server", addr, "id", "/no/such/path.bin"}, nil, &out, &errBuf)
	if code != 1 {
		t.Fatalf("put missing-file: code=%d stderr=%q", code, errBuf.String())
	}
	if !strings.Contains(errBuf.String(), "could not open") {
		t.Errorf("stderr should explain the open failure, got %q", errBuf.String())
	}
}

func TestRunChunk_UsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"put no args", []string{"chunk", "put"}},
		{"put one arg", []string{"chunk", "put", "id"}},
		{"get no args", []string{"chunk", "get"}},
		{"get three args", []string{"chunk", "get", "a", "b", "c"}},
		{"delete no args", []string{"chunk", "delete"}},
		{"stat no args", []string{"chunk", "stat"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			code := runMain(tc.args, nil, &out, &errBuf)
			if code != 2 {
				t.Errorf("got exit %d, want 2 for usage error", code)
			}
			if !strings.Contains(errBuf.String(), "Usage:") {
				t.Errorf("stderr should print Usage, got %q", errBuf.String())
			}
		})
	}
}

func TestRunChunk_InvalidIDSurfacesActionable(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()

	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "src.bin")
	_ = os.WriteFile(src, []byte("x"), 0o600)

	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "put", "--server", addr, "bad/id", src}, nil, &out, &errBuf)
	if code != 2 {
		t.Errorf("bad-id put: got %d, want 2", code)
	}
	if !strings.Contains(errBuf.String(), "invalid") {
		t.Errorf("stderr should mention invalid id, got %q", errBuf.String())
	}
}

func TestRunChunk_DialFailureSurfacesActionable(t *testing.T) {
	// Use a port that nothing's listening on.
	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "stat", "--server", "127.0.0.1:1", "x"}, nil, &out, &errBuf)
	if code == 0 {
		t.Fatalf("stat against dead address should fail, got %q", errBuf.String())
	}
	// The exact phrasing depends on the gRPC status, but the message must
	// be actionable — naming silod and pointing at SILO_SERVER.
	out2 := errBuf.String()
	if !strings.Contains(out2, "siloctl") {
		t.Errorf("stderr should prefix with siloctl, got %q", out2)
	}
}
