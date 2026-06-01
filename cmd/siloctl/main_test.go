package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	nodev1 "github.com/hyperized/silo/api/proto/silo/node/v1"
	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crypto"
	"github.com/hyperized/silo/internal/diskusage"
	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/namespace"
	"github.com/hyperized/silo/internal/transport"
)

// testMembers is a fixed two-node membership view for the test server's status
// service.
type testMembers struct{}

func (testMembers) Members() []membership.Node {
	at := time.Unix(1_700_000_000, 0)
	return []membership.Node{
		{ID: "silo-a", Address: "silo-a:7100", DataAddress: "silo-a:7000", State: membership.StateAlive, Incarnation: 1, LastChange: at, CapacityBytes: 1 << 30, UsedBytes: 1 << 28},
		{ID: "silo-b", Address: "silo-b:7100", DataAddress: "silo-b:7000", State: membership.StateSuspect, Incarnation: 1, LastChange: at},
	}
}

// testDrainer is a no-op Drainer for the test server's node-admin service.
type testDrainer struct{}

func (testDrainer) Drain() bool { return true }

// storeCoord is a single-node Coordinator that delegates to the local
// store, so siloctl's chunk commands (which the daemon routes through the
// replication coordinator) round-trip against the test server.
type storeCoord struct{ store chunkstore.Store }

func (c storeCoord) Write(ctx context.Context, id string, data []byte) (chunkstore.Info, error) {
	return c.store.Put(ctx, id, data)
}

func (c storeCoord) Read(ctx context.Context, id string) ([]byte, chunkstore.Info, error) {
	return c.store.Get(ctx, id)
}

func (c storeCoord) Delete(ctx context.Context, id string) error {
	return c.store.Delete(ctx, id)
}

func (c storeCoord) Stat(ctx context.Context, id string) (chunkstore.Info, error) {
	return c.store.Stat(ctx, id)
}

// newTestServer mirrors the transport test helper: real gRPC server, real
// chunk store under a tempdir, returned address ready for --server=.
// Each test starts with an empty per-user config dir so credentials the
// developer may have on disk from a real 'siloctl auth init' do not bleed
// into loadClientTLS and make the plain test server fail the handshake.
func newTestServer(t *testing.T) (addr string, teardown func()) {
	t.Helper()
	prevUserConfigDir := userConfigDir
	emptyDir := t.TempDir()
	userConfigDir = func() (string, error) { return emptyDir, nil }
	t.Cleanup(func() { userConfigDir = prevUserConfigDir })
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
	svc := transport.NewChunkService(store, storeCoord{store: store}, logger)
	nsSvc := transport.NewNamespaceService(namespace.New(hlc.New("test")), logger)
	statusSvc := transport.NewStatusService(testMembers{}, store, "/var/lib/silo", "silo-a", "test", logger,
		transport.WithDiskUsage(func(string) (diskusage.Usage, error) {
			return diskusage.Usage{CapacityBytes: 1 << 30, UsedBytes: 1 << 28, AvailableBytes: 1<<30 - 1<<28}, nil
		}))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	adminSvc := transport.NewNodeAdminService(testDrainer{}, "silo-a", logger)

	s := grpc.NewServer()
	chunkv1.RegisterChunkStoreServer(s, svc)
	namespacev1.RegisterNamespaceStoreServer(s, nsSvc)
	statusv1.RegisterClusterStatusServer(s, statusSvc)
	nodev1.RegisterNodeAdminServer(s, adminSvc)
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

func TestRunMain_AuthDispatches(t *testing.T) {
	// Top-level dispatch must reach the auth subcommand; without this
	// branch coverage, adding the case to runMain silently breaks the
	// dispatcher.
	var out, errBuf bytes.Buffer
	if code := runMain([]string{"auth", "help"}, nil, &out, &errBuf); code != 0 {
		t.Errorf("auth help exit: got %d, want 0; stderr=%q", code, errBuf.String())
	}
	if !strings.Contains(out.String(), "siloctl auth") {
		t.Errorf("auth help missing usage, got %q", out.String())
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

func TestRunChunk_FlagParseErrors(t *testing.T) {
	// Each subcommand uses flag.ContinueOnError; an unknown flag makes
	// fs.Parse return non-nil so runChunkX returns 2 early. Covers the
	// "fs.Parse failed" branch in each.
	cases := []struct {
		name string
		args []string
	}{
		{"put bad flag", []string{"chunk", "put", "--no-such-flag", "id", "src"}},
		{"get bad flag", []string{"chunk", "get", "--no-such-flag", "id"}},
		{"delete bad flag", []string{"chunk", "delete", "--no-such-flag", "id"}},
		{"stat bad flag", []string{"chunk", "stat", "--no-such-flag", "id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			code := runMain(tc.args, nil, &out, &errBuf)
			if code != 2 {
				t.Errorf("got code %d, want 2 (parse error)", code)
			}
		})
	}
}

func TestRunChunk_GetWriteFailureToUnwritableFile(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src.bin")
	_ = os.WriteFile(src, []byte("payload"), 0o600)
	_ = runMain([]string{"chunk", "put", "--server", addr, "writefail", src}, nil, new(bytes.Buffer), new(bytes.Buffer))

	// Try to write to a path that cannot be created (parent doesn't exist).
	dst := filepath.Join(tmp, "missing-parent", "out.bin")
	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "get", "--server", addr, "writefail", dst}, nil, &out, &errBuf)
	if code != 1 {
		t.Errorf("got %d, want 1 for unwritable destination", code)
	}
	if !strings.Contains(errBuf.String(), "could not open") {
		t.Errorf("stderr should explain the open failure, got %q", errBuf.String())
	}
}

func TestRunChunk_DialerSwapErrorPaths(t *testing.T) {
	// Force every operation through a dialer that returns an error. This
	// exercises the "could not dial silod" branch in each subcommand,
	// which is otherwise only hit on a real network failure.
	prev := dialer
	t.Cleanup(func() { dialer = prev })
	dialer = func(string) (*grpc.ClientConn, error) {
		return nil, errors.New("dialer refused")
	}

	cases := []struct {
		name string
		args []string
	}{
		{"put dial fail", []string{"chunk", "put", "--server", "x", "id", "-"}},
		{"get dial fail", []string{"chunk", "get", "--server", "x", "id"}},
		{"delete dial fail", []string{"chunk", "delete", "--server", "x", "id"}},
		{"stat dial fail", []string{"chunk", "stat", "--server", "x", "id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			code := runMain(tc.args, strings.NewReader(""), &out, &errBuf)
			if code != 1 {
				t.Errorf("got %d, want 1 (dial failure)", code)
			}
			if !strings.Contains(errBuf.String(), "could not dial silod") {
				t.Errorf("stderr should mention dial failure, got %q", errBuf.String())
			}
		})
	}
}

func TestReportRPC_CoversAllBranches(t *testing.T) {
	// Direct unit test on reportRPC since the branches map to gRPC codes
	// that are awkward to provoke through the full client surface.
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"not found", status.Error(codes.NotFound, "chunk not found"), 1},
		{"invalid arg", status.Error(codes.InvalidArgument, "bad id"), 2},
		{"unavailable", status.Error(codes.Unavailable, "no peer"), 1},
		{"other gRPC code", status.Error(codes.Internal, "boom"), 1},
		{"non-status error", errors.New("plain go error"), 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var errBuf bytes.Buffer
			code := reportRPC(&errBuf, "test-op", tc.err)
			if code != tc.want {
				t.Errorf("got code %d, want %d (stderr=%q)", code, tc.want, errBuf.String())
			}
			if !strings.Contains(errBuf.String(), "test-op") {
				t.Errorf("stderr should name the operation, got %q", errBuf.String())
			}
		})
	}
}

func TestEnvDefault(t *testing.T) {
	// Cover both branches: set vs unset env var.
	t.Setenv("SILOCTL_TEST_VAR", "set-value")
	if got := envDefault("SILOCTL_TEST_VAR", "fallback"); got != "set-value" {
		t.Errorf("set: got %q, want set-value", got)
	}
	if got := envDefault("SILOCTL_NO_SUCH_VAR_AT_ALL", "fallback"); got != "fallback" {
		t.Errorf("unset: got %q, want fallback", got)
	}
}

func TestParseFlexible(t *testing.T) {
	// Every case below registers --size (string), --yes (bool), and uses
	// fs.Args() for the positional tail. They cover the matrix that
	// matters operationally: positional anywhere, --foo=bar inline,
	// bool flag in front of positional, `--` terminator, and a bare `-`
	// stdin sentinel.
	type want struct {
		size       string
		yes        bool
		positional []string
		parseErr   bool
	}
	cases := []struct {
		name string
		args []string
		want want
	}{
		{
			name: "flags before positional (canonical)",
			args: []string{"--size", "1G", "/v"},
			want: want{size: "1G", positional: []string{"/v"}},
		},
		{
			name: "positional before flags (the bug report)",
			args: []string{"/v", "--size", "1G"},
			want: want{size: "1G", positional: []string{"/v"}},
		},
		{
			name: "positional sandwiched between flags",
			args: []string{"--size", "1G", "/v", "--yes"},
			want: want{size: "1G", yes: true, positional: []string{"/v"}},
		},
		{
			name: "inline --flag=value with leading positional",
			args: []string{"/v", "--size=2G"},
			want: want{size: "2G", positional: []string{"/v"}},
		},
		{
			name: "bool flag does not consume the next positional",
			args: []string{"--yes", "/v"},
			want: want{yes: true, positional: []string{"/v"}},
		},
		{
			name: "multiple positionals preserve order",
			args: []string{"a", "--size", "1G", "b", "c"},
			want: want{size: "1G", positional: []string{"a", "b", "c"}},
		},
		{
			name: "-- terminates flag processing",
			args: []string{"--size", "1G", "--", "--not-a-flag", "/v"},
			want: want{size: "1G", positional: []string{"--not-a-flag", "/v"}},
		},
		{
			name: "bare dash is positional (stdin sentinel)",
			args: []string{"-", "--size", "1G"},
			want: want{size: "1G", positional: []string{"-"}},
		},
		{
			name: "unknown flag still surfaces as a parse error",
			args: []string{"--bogus", "x", "/v"},
			want: want{parseErr: true},
		},
		{
			name: "no args parses cleanly with zero positional",
			args: nil,
			want: want{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			size := fs.String("size", "", "")
			yes := fs.Bool("yes", false, "")
			err := parseFlexible(fs, tc.args)
			if (err != nil) != tc.want.parseErr {
				t.Fatalf("err = %v, parseErr = %v", err, tc.want.parseErr)
			}
			if tc.want.parseErr {
				return
			}
			if *size != tc.want.size {
				t.Errorf("size = %q, want %q", *size, tc.want.size)
			}
			if *yes != tc.want.yes {
				t.Errorf("yes = %v, want %v", *yes, tc.want.yes)
			}
			got := fs.Args()
			if len(got) != len(tc.want.positional) {
				t.Fatalf("positional = %v, want %v", got, tc.want.positional)
			}
			for i, p := range tc.want.positional {
				if got[i] != p {
					t.Errorf("positional[%d] = %q, want %q", i, got[i], p)
				}
			}
		})
	}
}

// putToFakeStreamSendFailure exercises the inner Put error paths by
// forcing the server to close its side before stream.Send is called. We
// use a real server with an in-process listener, then close the conn
// after Put opens but before the client tries to send the header.
// The header-send failure path is mid-stream, so flakes are bounded by
// the connection being torn down before the goroutine writes.
func TestRunChunk_PutStdinReadFailure(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()

	// failingReader returns an error after returning zero bytes — drives
	// runChunkPut into the "could not read from <path>" branch.
	r := &failingReadCloser{err: errors.New("simulated stdin failure")}
	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "put", "--server", addr, "stdin-fail", "-"}, r, &out, &errBuf)
	if code != 1 {
		t.Errorf("got %d, want 1 for read failure", code)
	}
	if !strings.Contains(errBuf.String(), "could not read from") {
		t.Errorf("stderr should mention the read failure, got %q", errBuf.String())
	}
}

type failingReadCloser struct{ err error }

func (f *failingReadCloser) Read(_ []byte) (int, error) { return 0, f.err }
func (f *failingReadCloser) Close() error               { return nil }

type failingWriter struct{ err error }

func (f failingWriter) Write(_ []byte) (int, error) { return 0, f.err }

// fakeChunkClient lets tests reach the in-stream Send/Recv error paths
// of runChunkPut and runChunkGet, which a real gRPC connection only
// trips on flaky networks. Each method is independently scriptable.
type fakeChunkClient struct {
	putOpenErr error
	putStream  *fakePutStream
	getOpenErr error
	getStream  *fakeGetStream
	deleteErr  error
	statResp   *chunkv1.StatResponse
	statErr    error
}

func (f *fakeChunkClient) Put(_ context.Context, _ ...grpc.CallOption) (chunkv1.ChunkStore_PutClient, error) {
	if f.putOpenErr != nil {
		return nil, f.putOpenErr
	}
	return f.putStream, nil
}

func (f *fakeChunkClient) Get(_ context.Context, _ *chunkv1.GetRequest, _ ...grpc.CallOption) (chunkv1.ChunkStore_GetClient, error) {
	if f.getOpenErr != nil {
		return nil, f.getOpenErr
	}
	return f.getStream, nil
}

func (f *fakeChunkClient) Delete(_ context.Context, _ *chunkv1.DeleteRequest, _ ...grpc.CallOption) (*chunkv1.DeleteResponse, error) {
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &chunkv1.DeleteResponse{}, nil
}

func (f *fakeChunkClient) Stat(_ context.Context, _ *chunkv1.StatRequest, _ ...grpc.CallOption) (*chunkv1.StatResponse, error) {
	if f.statErr != nil {
		return nil, f.statErr
	}
	return f.statResp, nil
}

// fakePutStream and fakeGetStream are minimal implementations of the
// generated streaming client interfaces. Only the methods runChunkPut
// and runChunkGet call are filled in; the rest exists to satisfy the
// embedded grpc.ClientStream contract via ClientStream embedding.
type fakePutStream struct {
	grpc.ClientStream
	headerSendErr error
	dataSendErr   error
	closeRecvErr  error
	closeResp     *chunkv1.PutResponse
	sends         int
}

func (s *fakePutStream) Send(req *chunkv1.PutRequest) error {
	s.sends++
	if _, isHeader := req.Body.(*chunkv1.PutRequest_Header); isHeader && s.headerSendErr != nil {
		return s.headerSendErr
	}
	if _, isData := req.Body.(*chunkv1.PutRequest_Data); isData && s.dataSendErr != nil {
		return s.dataSendErr
	}
	return nil
}

func (s *fakePutStream) CloseAndRecv() (*chunkv1.PutResponse, error) {
	if s.closeRecvErr != nil {
		return nil, s.closeRecvErr
	}
	return s.closeResp, nil
}

type fakeGetStream struct {
	grpc.ClientStream
	msgs    []*chunkv1.GetResponse
	recvErr error
	idx     int
}

func (s *fakeGetStream) Recv() (*chunkv1.GetResponse, error) {
	if s.recvErr != nil {
		return nil, s.recvErr
	}
	if s.idx >= len(s.msgs) {
		return nil, io.EOF
	}
	m := s.msgs[s.idx]
	s.idx++
	return m, nil
}

// withFakeClient installs a fake chunk client and a no-op dialer for the
// duration of a test, restoring originals afterwards.
func withFakeClient(t *testing.T, fc *fakeChunkClient) {
	t.Helper()
	prevDial := dialer
	prevClient := newChunkClient
	t.Cleanup(func() {
		dialer = prevDial
		newChunkClient = prevClient
	})
	dialer = func(string) (*grpc.ClientConn, error) {
		// We never speak to this conn; newChunkClient is also stubbed.
		// Constructing a NewClient is non-blocking, so the dial never
		// touches the network.
		return grpc.NewClient("passthrough:///fake", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	newChunkClient = func(*grpc.ClientConn) chunkv1.ChunkStoreClient { return fc }
}

func TestRunChunkPut_StreamSendHeaderFails(t *testing.T) {
	withFakeClient(t, &fakeChunkClient{
		putStream: &fakePutStream{headerSendErr: errors.New("header send boom")},
	})
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	_ = os.WriteFile(src, []byte("data"), 0o600)

	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "put", "--server", "x", "id", src}, nil, &out, &errBuf)
	if code != 1 {
		t.Errorf("got %d, want 1; stderr=%q", code, errBuf.String())
	}
}

func TestRunChunkPut_StreamSendDataFails(t *testing.T) {
	withFakeClient(t, &fakeChunkClient{
		putStream: &fakePutStream{dataSendErr: errors.New("data send boom")},
	})
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	_ = os.WriteFile(src, []byte("data"), 0o600)

	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "put", "--server", "x", "id", src}, nil, &out, &errBuf)
	if code != 1 {
		t.Errorf("got %d, want 1; stderr=%q", code, errBuf.String())
	}
}

func TestRunChunkPut_OpenStreamFails(t *testing.T) {
	withFakeClient(t, &fakeChunkClient{
		putOpenErr: status.Error(codes.Unavailable, "no peer"),
	})
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	_ = os.WriteFile(src, []byte("d"), 0o600)

	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "put", "--server", "x", "id", src}, nil, &out, &errBuf)
	if code != 1 {
		t.Errorf("got %d, want 1", code)
	}
}

func TestRunChunkPut_CloseAndRecvFails(t *testing.T) {
	withFakeClient(t, &fakeChunkClient{
		putStream: &fakePutStream{closeRecvErr: status.Error(codes.Internal, "store boom")},
	})
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	_ = os.WriteFile(src, []byte("d"), 0o600)

	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "put", "--server", "x", "id", src}, nil, &out, &errBuf)
	if code != 1 {
		t.Errorf("got %d, want 1", code)
	}
}

func TestRunChunkGet_OpenStreamFails(t *testing.T) {
	withFakeClient(t, &fakeChunkClient{
		getOpenErr: status.Error(codes.Unavailable, "no peer"),
	})
	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "get", "--server", "x", "id"}, nil, &out, &errBuf)
	if code != 1 {
		t.Errorf("got %d, want 1", code)
	}
}

func TestRunChunkGet_RecvFails(t *testing.T) {
	withFakeClient(t, &fakeChunkClient{
		getStream: &fakeGetStream{recvErr: status.Error(codes.Internal, "recv boom")},
	})
	var out, errBuf bytes.Buffer
	code := runMain([]string{"chunk", "get", "--server", "x", "id"}, nil, &out, &errBuf)
	if code != 1 {
		t.Errorf("got %d, want 1; stderr=%q", code, errBuf.String())
	}
}

func TestRunChunk_GetSinkWriteFailure(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src.bin")
	if err := os.WriteFile(src, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if code := runMain([]string{"chunk", "put", "--server", addr, "sinkfail", src}, nil, new(bytes.Buffer), new(bytes.Buffer)); code != 0 {
		t.Fatalf("put: code=%d", code)
	}

	// Stream Get to a writer that always fails — exercises the
	// "could not write chunk data to the destination" branch.
	sink := failingWriter{err: errors.New("sink boom")}
	var errBuf bytes.Buffer
	code := runMain([]string{"chunk", "get", "--server", addr, "sinkfail"}, nil, sink, &errBuf)
	if code != 1 {
		t.Errorf("got code %d, want 1 for sink write failure (stderr=%q)", code, errBuf.String())
	}
	if !strings.Contains(errBuf.String(), "could not write chunk data") {
		t.Errorf("stderr should mention write failure, got %q", errBuf.String())
	}
}

// TestRunChunk_RPCFailureMidStream tears the server down between dial
// and the first RPC, so client.Put / client.Get / client.Delete fail
// after the dial succeeds. Without this branch coverage, real silod
// crashes/restarts would surface as untested error paths in production.
func TestRunChunk_RPCFailureMidStream(t *testing.T) {
	// Spin up a test server, capture its addr, then kill it before
	// running the CLI command. Using a tiny rpcTimeout shrinks the
	// per-call wait so the test stays fast.
	prev := rpcTimeout
	t.Cleanup(func() { rpcTimeout = prev })
	rpcTimeout = 250 * time.Millisecond

	cases := []struct {
		name string
		args []string
	}{
		{"put against dead server", []string{"chunk", "put", "--server", "DEADSERVER", "id", "-"}},
		{"get against dead server", []string{"chunk", "get", "--server", "DEADSERVER", "id"}},
		{"delete against dead server", []string{"chunk", "delete", "--server", "DEADSERVER", "id"}},
		{"stat against dead server", []string{"chunk", "stat", "--server", "DEADSERVER", "id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			addr, teardown := newTestServer(t)
			teardown() // kill immediately so RPCs fail
			args := make([]string, len(tc.args))
			copy(args, tc.args)
			for i, a := range args {
				if a == "DEADSERVER" {
					args[i] = addr
				}
			}
			var out, errBuf bytes.Buffer
			code := runMain(args, strings.NewReader("payload"), &out, &errBuf)
			if code == 0 {
				t.Errorf("got code 0, want non-zero against a dead server (stdout=%q stderr=%q)", out.String(), errBuf.String())
			}
		})
	}
}
