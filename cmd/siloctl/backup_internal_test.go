package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	nodev1 "github.com/hyperized/silo/api/proto/silo/node/v1"
	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

type fakeChunks map[string][]byte

func (f fakeChunks) List(context.Context) ([]string, error) {
	ids := make([]string, 0, len(f))
	for id := range f {
		ids = append(ids, id)
	}
	return ids, nil
}

func (f fakeChunks) RawChunk(_ context.Context, id string) ([]byte, error) { return f[id], nil }

type fakeNS struct{ data []byte }

func (f fakeNS) Snapshot() ([]byte, error) { return f.data, nil }

// seedBackup writes one real backup of node into a fresh directory target and
// returns the directory.
func seedBackup(t *testing.T, dir, node string, at time.Time, chunks fakeChunks) {
	t.Helper()
	snap, err := namespace.New(hlc.New("node-a")).Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	tgt, err := blobstore.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = backup.NewExporter(chunks, fakeNS{snap}, node,
		backup.WithClock(func() time.Time { return at }), backup.WithVersion("v1.2.3")).
		Export(context.Background(), tgt)
	if err != nil {
		t.Fatal(err)
	}
}

func clearBackupEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"SILO_BACKUP_TARGET", "SILO_BACKUP_S3_ENDPOINT", "SILO_BACKUP_S3_REGION", "SILO_BACKUP_S3_PATH_STYLE"} {
		t.Setenv(k, "")
	}
}

func runBk(args ...string) (int, string, string) {
	var out, errBuf bytes.Buffer
	code := runBackup(args, &out, &errBuf)
	return code, out.String(), errBuf.String()
}

func TestBackup_UsageAndDispatch(t *testing.T) {
	clearBackupEnv(t)
	tests := []struct {
		name    string
		args    []string
		code    int
		stdout  string
		stderr  string
		mainHas string
	}{
		{"no args", nil, 2, "siloctl backup", "", ""},
		{"help", []string{"help"}, 0, "Target flags", "", ""},
		{"-h", []string{"-h"}, 0, "AWS_PROFILE", "", ""},
		{"unknown", []string{"nope"}, 2, "", "Run 'siloctl backup help'", ""},
		{"run extra arg", []string{"run", "x"}, 2, "", "Usage: siloctl backup run", ""},
		{"run bad flag", []string{"run", "--bogus"}, 2, "", "", ""},
		{"run zero wait", []string{"run", "--wait=0s"}, 2, "", "--wait must be longer than zero", ""},
		{"list bad flag", []string{"list", "--bogus"}, 2, "", "", ""},
		{"list extra arg", []string{"list", "x"}, 2, "", "Usage: siloctl backup list", ""},
		{"list no target", []string{"list"}, 2, "", "SILO_BACKUP_TARGET", ""},
		{"list bad endpoint", []string{"list", "--target=s3://b/p", "--s3-endpoint=ftp://x"}, 2, "", "--s3-endpoint is not usable", ""},
		{"verify bad flag", []string{"verify", "--bogus"}, 2, "", "", ""},
		{"verify extra arg", []string{"verify", "x"}, 2, "", "Usage: siloctl backup verify", ""},
		{"verify no node", []string{"verify", "--target=/x"}, 2, "", "--node is required", ""},
		{"verify bad node", []string{"verify", "--target=/x", "--node=../x"}, 2, "", "invalid node id", ""},
		{"verify bad manifest", []string{"verify", "--target=/x", "--node=a", "--manifest=yesterday"}, 2, "", "invalid manifest name", ""},
		{"verify no target", []string{"verify", "--node=a"}, 2, "", "--target", ""},
		{"restore bad flag", []string{"restore", "--bogus"}, 2, "", "", ""},
		{"restore extra arg", []string{"restore", "x"}, 2, "", "Usage: siloctl backup restore", ""},
		{"restore no data-dir", []string{"restore", "--target=/x", "--node=a"}, 2, "", "--data-dir is required", ""},
		{"restore bad concurrency", []string{"restore", "--target=/x", "--node=a", "--data-dir=/d", "--concurrency=0"}, 2, "", "--concurrency must be at least 1", ""},
		{"restore no node", []string{"restore", "--target=/x", "--data-dir=/d"}, 2, "", "--node is required", ""},
		{"restore no target", []string{"restore", "--node=a", "--data-dir=/d"}, 2, "", "--target", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out, errOut := runBk(tt.args...)
			if code != tt.code {
				t.Fatalf("code = %d, want %d (stderr %q)", code, tt.code, errOut)
			}
			if !strings.Contains(out, tt.stdout) || !strings.Contains(errOut, tt.stderr) {
				t.Errorf("stdout %q / stderr %q missing %q / %q", out, errOut, tt.stdout, tt.stderr)
			}
		})
	}
}

func TestRunMain_BackupDispatch(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runMain([]string{"backup", "help"}, nil, &out, &errBuf); code != 0 {
		t.Fatalf("code = %d", code)
	}
	out.Reset()
	runMain([]string{"help"}, nil, &out, &errBuf)
	if !strings.Contains(out.String(), "Run, list, verify and restore backups") {
		t.Errorf("main usage missing backup line: %q", out.String())
	}
}

type backupAdminClient struct {
	nodev1.NodeAdminClient
	resp *nodev1.RunBackupResponse
	err  error
}

func (c backupAdminClient) RunBackup(context.Context, *nodev1.RunBackupRequest, ...grpc.CallOption) (*nodev1.RunBackupResponse, error) {
	return c.resp, c.err
}

func fakeBackupClient(t *testing.T, c nodev1.NodeAdminClient) {
	t.Helper()
	prevDial, prevClient := dialer, newNodeAdminClient
	t.Cleanup(func() { dialer = prevDial; newNodeAdminClient = prevClient })
	dialer = func(string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///x", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	newNodeAdminClient = func(*grpc.ClientConn) nodev1.NodeAdminClient { return c }
}

func TestBackupRun(t *testing.T) {
	okRun := &statusv1.BackupRun{StartedUnix: 100, FinishedUnix: 112, Ok: true, ChunksUploaded: 3, ChunksSkipped: 1, BytesUploaded: 1258291, Manifest: "manifests/silo-a/2026-10-28T12:00:00.000Z.json"}
	tests := []struct {
		name   string
		client backupAdminClient
		code   int
		stdout []string
		stderr string
	}{
		{
			"success",
			backupAdminClient{resp: &nodev1.RunBackupResponse{NodeId: "silo-a", Run: okRun}},
			0,
			[]string{"Backup finished on node silo-a in 12s.", "Uploaded 3 chunks (1.2 MiB); 1 was already on the target.", "Restore point: manifests/silo-a/2026-10-28T12:00:00.000Z.json"},
			"",
		},
		{
			"many skipped",
			backupAdminClient{resp: &nodev1.RunBackupResponse{NodeId: "silo-a", Run: &statusv1.BackupRun{Ok: true, ChunksUploaded: 1, ChunksSkipped: 10}}},
			0,
			[]string{"Uploaded 1 chunk (0 B); 10 were already"},
			"",
		},
		{
			"run failed",
			backupAdminClient{resp: &nodev1.RunBackupResponse{NodeId: "silo-a", Run: &statusv1.BackupRun{Error: "target unreachable, check credentials"}}},
			1, nil,
			"Backup on node silo-a failed: target unreachable",
		},
		{"deadline", backupAdminClient{err: status.Error(codes.DeadlineExceeded, "late")}, 1, nil, "keeps running on the node"},
		{"precondition", backupAdminClient{err: status.Error(codes.FailedPrecondition, "backup is not configured")}, 1, nil, "backup is not configured"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeBackupClient(t, tt.client)
			var out, errBuf bytes.Buffer
			if code := runBackup([]string{"run", "--server=x", "--wait=5s"}, &out, &errBuf); code != tt.code {
				t.Fatalf("code = %d, want %d (stderr %q)", code, tt.code, errBuf.String())
			}
			for _, w := range tt.stdout {
				if !strings.Contains(out.String(), w) {
					t.Errorf("stdout missing %q: %q", w, out.String())
				}
			}
			if !strings.Contains(errBuf.String(), tt.stderr) {
				t.Errorf("stderr = %q, want %q", errBuf.String(), tt.stderr)
			}
		})
	}
}

func TestBackupRun_DialError(t *testing.T) {
	prev := dialer
	t.Cleanup(func() { dialer = prev })
	dialer = func(string) (*grpc.ClientConn, error) { return nil, errors.New("dial boom") }
	var out, errBuf bytes.Buffer
	if code := runBackup([]string{"run"}, &out, &errBuf); code != 1 || !strings.Contains(errBuf.String(), "could not dial") {
		t.Errorf("code = %d, stderr = %q", code, errBuf.String())
	}
}

var (
	t1 = time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	t2 = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
)

func TestBackupList(t *testing.T) {
	clearBackupEnv(t)
	dir := t.TempDir()
	seedBackup(t, dir, "node-b", t2, fakeChunks{"c1": []byte("aaaa")})
	seedBackup(t, dir, "node-a", t2, fakeChunks{"c1": []byte("aaaa"), "c2": []byte("bb")})
	seedBackup(t, dir, "node-a", t1, fakeChunks{"c1": []byte("aaaa")})

	code, out, errOut := runBk("list", "--target", dir)
	if code != 0 {
		t.Fatalf("code = %d, stderr %q", code, errOut)
	}
	iA1 := strings.Index(out, "node-a  2026-10-27T12:00:00.000Z")
	iA2 := strings.Index(out, "node-a  2026-10-28T12:00:00.000Z")
	iB := strings.Index(out, "node-b  2026-10-28T12:00:00.000Z")
	if iA1 < 0 || iA1 > iA2 || iA2 > iB {
		t.Errorf("rows missing or unsorted:\n%s", out)
	}
	for _, w := range []string{"NODE", "SILO VERSION", "v1.2.3", "Restore one with: siloctl backup restore --target"} {
		if !strings.Contains(out, w) {
			t.Errorf("output missing %q:\n%s", w, out)
		}
	}

	t.Setenv("SILO_BACKUP_TARGET", dir)
	if code, _, _ := runBk("list"); code != 0 {
		t.Errorf("env target: code = %d", code)
	}
}

func TestBackupList_EmptyAndErrors(t *testing.T) {
	clearBackupEnv(t)
	code, out, _ := runBk("list", "--target", t.TempDir())
	if code != 0 || !strings.Contains(out, "No backups were found at") || !strings.Contains(out, "--target") {
		t.Errorf("empty: code = %d, out = %q", code, out)
	}

	// A manifest that is not valid JSON fails the whole listing.
	dir := t.TempDir()
	bad := filepath.Join(dir, "manifests", "node-a")
	if err := os.MkdirAll(bad, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, "2026-10-28T12:00:00.000Z.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, _, errOut := runBk("list", "--target", dir)
	if code != 1 || !strings.Contains(errOut, "could not list backups") {
		t.Errorf("bad manifest: code = %d, stderr = %q", code, errOut)
	}
}

func TestTargetOpen_Options(t *testing.T) {
	prev := openBackupTarget
	t.Cleanup(func() { openBackupTarget = prev })
	var gotURL string
	var gotOpts int
	var openErr error
	openBackupTarget = func(u string, opts ...blobstore.Option) (blobstore.Target, error) {
		gotURL, gotOpts = u, len(opts)
		return nil, openErr
	}

	tests := []struct {
		name     string
		env      map[string]string
		args     []string
		wantCode int
		wantOpts int
		openErr  error
	}{
		{"plain url", nil, []string{"--target=s3://b/p"}, 0, 1, nil},
		{"endpoint adds region and path style", nil, []string{"--target=s3://b/p", "--s3-endpoint=https://s3.example.com"}, 0, 3, nil},
		{"region only", nil, []string{"--target=s3://b/p", "--s3-region=eu-west-1"}, 0, 2, nil},
		{"env target", map[string]string{"SILO_BACKUP_TARGET": "s3://env/p"}, nil, 0, 1, nil},
		{"bad path style env", map[string]string{"SILO_BACKUP_S3_PATH_STYLE": "maybe"}, []string{"--target=s3://b/p"}, 2, 0, nil},
		{"open error", nil, []string{"--target=nope"}, 2, 0, errors.New("bad scheme")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearBackupEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			openErr = tt.openErr
			fs := newBackupFlagSet("x", &bytes.Buffer{})
			tf := addTargetFlags(fs)
			if err := fs.Parse(tt.args); err != nil {
				t.Fatal(err)
			}
			var errBuf bytes.Buffer
			_, code := tf.open(&errBuf)
			// A nil target with no error is fine for the seam; only usage
			// failures set a non-zero code.
			if tt.openErr == nil && tt.wantCode == 0 {
				if code != 0 {
					t.Fatalf("code = %d, stderr %q", code, errBuf.String())
				}
				if gotOpts != tt.wantOpts {
					t.Errorf("options = %d, want %d (url %q)", gotOpts, tt.wantOpts, gotURL)
				}
				return
			}
			if code != tt.wantCode {
				t.Errorf("code = %d, want %d (stderr %q)", code, tt.wantCode, errBuf.String())
			}
		})
	}
}

func TestResolvePathStyle(t *testing.T) {
	tests := []struct {
		name    string
		env     string
		args    []string
		want    bool
		wantErr bool
	}{
		{"no endpoint", "", nil, false, false},
		{"endpoint defaults on", "", []string{"--s3-endpoint=https://s3.example.com"}, true, false},
		{"explicit false beats endpoint", "", []string{"--s3-endpoint=https://s3.example.com", "--s3-path-style=false"}, false, false},
		{"explicit true", "", []string{"--s3-path-style"}, true, false},
		{"env on", "yes", nil, true, false},
		{"env off beats endpoint", "off", []string{"--s3-endpoint=https://s3.example.com"}, false, false},
		{"flag beats env", "1", []string{"--s3-path-style=false"}, false, false},
		{"env invalid", "maybe", nil, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearBackupEnv(t)
			t.Setenv("SILO_BACKUP_S3_PATH_STYLE", tt.env)
			fs := flag.NewFlagSet("x", flag.ContinueOnError)
			tf := addTargetFlags(fs)
			if err := fs.Parse(tt.args); err != nil {
				t.Fatal(err)
			}
			got, err := tf.resolvePathStyle()
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("got (%v, %v), want (%v, err=%v)", got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestParseBoolEnv(t *testing.T) {
	for in, want := range map[string]bool{"1": true, "TRUE": true, " yes ": true, "on": true, "0": false, "false": false, "No": false, "OFF": false} {
		if got, ok := parseBoolEnv(in); !ok || got != want {
			t.Errorf("parseBoolEnv(%q) = %v, %v", in, got, ok)
		}
	}
	if _, ok := parseBoolEnv("x"); ok {
		t.Error("garbage should not parse")
	}
}

func TestBackupVerify(t *testing.T) {
	clearBackupEnv(t)
	dir := t.TempDir()
	seedBackup(t, dir, "node-a", t2, fakeChunks{"c1": []byte("aaaa"), "c2": []byte("bb")})

	code, out, errOut := runBk("verify", "--target", dir, "--node", "node-a")
	if code != 0 || !strings.Contains(out, "All 3 objects referenced by backup 2026-10-28T12:00:00.000Z of node node-a are present with the recorded sizes.") {
		t.Fatalf("ok: code = %d, out = %q, err = %q", code, out, errOut)
	}
	if code, _, _ := runBk("verify", "--target", dir, "--node", "node-a", "--manifest", "2026-10-28T12:00:00.000Z"); code != 0 {
		t.Errorf("explicit manifest: code = %d", code)
	}

	// Missing manifest.
	code, _, errOut = runBk("verify", "--target", dir, "--node", "node-z")
	if code != 1 || !strings.Contains(errOut, "siloctl backup list") {
		t.Errorf("no manifest: code = %d, err = %q", code, errOut)
	}

	// One missing chunk.
	if err := os.Remove(filepath.Join(dir, "chunks", "c1")); err != nil {
		t.Fatal(err)
	}
	code, _, errOut = runBk("verify", "--target", dir, "--node", "node-a")
	if code != 1 || !strings.Contains(errOut, "chunks/c1") || !strings.Contains(errOut, "Restore an older backup") || strings.Contains(errOut, "more") {
		t.Errorf("missing: code = %d, err = %q", code, errOut)
	}
}

func TestBackupVerify_ManyMissingCapped(t *testing.T) {
	clearBackupEnv(t)
	dir := t.TempDir()
	chunks := fakeChunks{}
	for i := 0; i < 60; i++ {
		chunks["c"+string(rune('a'+i/26))+string(rune('a'+i%26))] = []byte("x")
	}
	seedBackup(t, dir, "node-a", t2, chunks)
	if err := os.RemoveAll(filepath.Join(dir, "chunks")); err != nil {
		t.Fatal(err)
	}
	code, _, errOut := runBk("verify", "--target", dir, "--node", "node-a")
	if code != 1 || !strings.Contains(errOut, "has 60 problem objects") || !strings.Contains(errOut, "…and 10 more") {
		t.Errorf("code = %d, err = %q", code, errOut)
	}
}

// failingTarget fails List so Verify's own error path runs.
type failingTarget struct{ blobstore.Target }

func (failingTarget) List(context.Context, string) ([]blobstore.ObjectInfo, error) {
	return nil, errors.New("list boom")
}

func TestBackupVerify_VerifyError(t *testing.T) {
	clearBackupEnv(t)
	dir := t.TempDir()
	seedBackup(t, dir, "node-a", t2, fakeChunks{"c1": []byte("a")})
	prev := openBackupTarget
	t.Cleanup(func() { openBackupTarget = prev })
	openBackupTarget = func(u string, opts ...blobstore.Option) (blobstore.Target, error) {
		inner, err := prev(u, opts...)
		return failingTarget{inner}, err
	}
	code, _, errOut := runBk("verify", "--target", dir, "--node", "node-a")
	if code != 1 || !strings.Contains(errOut, "could not check the backup") {
		t.Errorf("code = %d, err = %q", code, errOut)
	}
}

func TestBackupRestore(t *testing.T) {
	clearBackupEnv(t)
	src := t.TempDir()
	chunks := fakeChunks{}
	for i := 0; i < 20; i++ {
		chunks["chunk-"+string(rune('a'+i))] = []byte("data")
	}
	seedBackup(t, src, "node-a", t2, chunks)

	dest := filepath.Join(t.TempDir(), "restored")
	code, out, errOut := runBk("restore", "--target", src, "--node", "node-a", "--data-dir", dest, "--concurrency=1")
	if code != 0 {
		t.Fatalf("code = %d, stderr %q", code, errOut)
	}
	for _, w := range []string{
		"Restoring node node-a from the backup of 2026-10-28T12:00:00.000Z: 20 chunks, 80 B, made by silo v1.2.3.",
		"10% (2 of 20 chunks)", "50% (10 of 20 chunks)", "100% (20 of 20 chunks)",
		"Restored 20 chunks (80 B)", "SILO_DATA_DIR=" + dest, "SILO_NODE_ID=node-a",
		"SILO_ENCRYPTION_KEY", "node.key", "restore every node from its own backup",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("output missing %q:\n%s", w, out)
		}
	}
	if n := strings.Count(out, "% ("); n > 11 {
		t.Errorf("progress printed %d lines, want at most 11", n)
	}
	for _, f := range []string{"namespace.json", "chunk-a.chunk", "chunk-t.chunk"} {
		if _, err := os.Stat(filepath.Join(dest, f)); err != nil {
			t.Errorf("expected %s in restored dir: %v", f, err)
		}
	}
}

func TestBackupRestore_ProgressSparse(t *testing.T) {
	clearBackupEnv(t)
	src := t.TempDir()
	seedBackup(t, src, "node-a", t2, fakeChunks{"only": []byte("d")})
	code, out, _ := runBk("restore", "--target", src, "--node", "node-a", "--data-dir", filepath.Join(t.TempDir(), "d"))
	if code != 0 || strings.Count(out, "100% (1 of 1 chunks)") != 1 {
		t.Errorf("code = %d, out = %q", code, out)
	}
}

func TestBackupRestore_Failures(t *testing.T) {
	clearBackupEnv(t)
	src := t.TempDir()
	seedBackup(t, src, "node-a", t2, fakeChunks{"c1": []byte("a")})

	t.Run("non-empty directory", func(t *testing.T) {
		dest := t.TempDir()
		keep := filepath.Join(dest, "keep.txt")
		if err := os.WriteFile(keep, []byte("mine"), 0o600); err != nil {
			t.Fatal(err)
		}
		code, _, errOut := runBk("restore", "--target", src, "--node", "node-a", "--data-dir", dest)
		if code != 1 || !strings.Contains(errOut, "Choose a new directory; siloctl never deletes or overwrites files.") {
			t.Errorf("code = %d, err = %q", code, errOut)
		}
		if b, _ := os.ReadFile(keep); string(b) != "mine" {
			t.Errorf("existing file changed: %q", b)
		}
	})
	t.Run("no such backup", func(t *testing.T) {
		code, _, errOut := runBk("restore", "--target", src, "--node", "node-q", "--data-dir", filepath.Join(t.TempDir(), "d"))
		if code != 1 || !strings.Contains(errOut, "siloctl backup list") {
			t.Errorf("code = %d, err = %q", code, errOut)
		}
	})
	t.Run("missing chunk", func(t *testing.T) {
		src2 := t.TempDir()
		seedBackup(t, src2, "node-a", t2, fakeChunks{"c1": []byte("a")})
		if err := os.Remove(filepath.Join(src2, "chunks", "c1")); err != nil {
			t.Fatal(err)
		}
		code, _, errOut := runBk("restore", "--target", src2, "--node", "node-a", "--data-dir", filepath.Join(t.TempDir(), "d"))
		if code != 1 || !strings.Contains(errOut, "restore failed") {
			t.Errorf("code = %d, err = %q", code, errOut)
		}
	})
}

func TestBackupVerify_Deep(t *testing.T) {
	clearBackupEnv(t)
	dir := t.TempDir()
	seedBackup(t, dir, "node-a", t2, fakeChunks{"c1": []byte("aaaa"), "c2": []byte("bb")})

	code, out, errOut := runBk("verify", "--target", dir, "--node", "node-a", "--deep")
	if code != 0 || !strings.Contains(out, "are present and match their recorded checksums.") {
		t.Fatalf("deep ok: code = %d, out = %q, err = %q", code, out, errOut)
	}

	// Same size, different bytes: only a checksum comparison notices.
	if err := os.WriteFile(filepath.Join(dir, "chunks", "c1"), []byte("zzzz"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, _, errOut := runBk("verify", "--target", dir, "--node", "node-a"); code != 0 {
		t.Errorf("default mode should not read contents: code = %d, err = %q", code, errOut)
	}
	code, _, errOut = runBk("verify", "--target", dir, "--node", "node-a", "--deep")
	if code != 1 || !strings.Contains(errOut, "chunks/c1") || !strings.Contains(errOut, "checksum") {
		t.Errorf("deep mismatch: code = %d, err = %q", code, errOut)
	}
}

func TestBackupVerify_SizeMismatch(t *testing.T) {
	clearBackupEnv(t)
	dir := t.TempDir()
	seedBackup(t, dir, "node-a", t2, fakeChunks{"c1": []byte("aaaa")})
	if err := os.WriteFile(filepath.Join(dir, "chunks", "c1"), []byte("aaaaa"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, _, errOut := runBk("verify", "--target", dir, "--node", "node-a")
	if code != 1 || !strings.Contains(errOut, "chunks/c1: size is 5 bytes, the backup recorded 4") {
		t.Errorf("code = %d, err = %q", code, errOut)
	}
}

func TestBackupHelp_MentionsDeep(t *testing.T) {
	clearBackupEnv(t)
	_, out, _ := runBk("help")
	if !strings.Contains(out, "--deep") {
		t.Errorf("help does not mention --deep: %q", out)
	}
}

func TestBackupRestore_TamperedChunk(t *testing.T) {
	clearBackupEnv(t)
	src := t.TempDir()
	seedBackup(t, src, "node-a", t2, fakeChunks{"c1": []byte("aaaa")})
	if err := os.WriteFile(filepath.Join(src, "chunks", "c1"), []byte("zzzz"), 0o600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "d")
	code, _, errOut := runBk("restore", "--target", src, "--node", "node-a", "--data-dir", dest)
	if code != 1 || !strings.Contains(errOut, "restore failed") || !strings.Contains(errOut, "differs from what the backup recorded") {
		t.Errorf("code = %d, err = %q", code, errOut)
	}
	if _, err := os.Stat(filepath.Join(dest, "c1.chunk")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("tampered chunk was written: %v", err)
	}
}

// The free-space check only works on Linux; elsewhere restore prints a note
// and carries on, so the assertions cover both outcomes.
func TestBackupRestore_FreeSpaceCheck(t *testing.T) {
	clearBackupEnv(t)
	src := t.TempDir()
	seedBackup(t, src, "node-a", t2, fakeChunks{"c1": []byte("aaaa")})

	t.Run("notice on platforms without the check", func(t *testing.T) {
		code, out, errOut := runBk("restore", "--target", src, "--node", "node-a", "--data-dir", filepath.Join(t.TempDir(), "d"))
		if code != 0 {
			t.Fatalf("code = %d, err = %q", code, errOut)
		}
		if hasNote := strings.Contains(out, "Note: could not check free space"); hasNote != (runtime.GOOS != "linux") {
			t.Errorf("GOOS=%s, note present = %v, out = %q", runtime.GOOS, hasNote, out)
		}
	})

	t.Run("not enough space", func(t *testing.T) {
		prev := restoreBackup
		t.Cleanup(func() { restoreBackup = prev })
		restoreBackup = func(context.Context, blobstore.Target, *backup.Manifest, string, backup.RestoreOptions) (backup.RestoreResult, error) {
			return backup.RestoreResult{}, fmt.Errorf("%w: /d has 1 bytes free but the backup needs about 9; free up space or restore onto a larger disk (nothing was written)", backup.ErrInsufficientSpace)
		}
		code, _, errOut := runBk("restore", "--target", src, "--node", "node-a", "--data-dir", filepath.Join(t.TempDir(), "d"))
		if code != 1 || !strings.Contains(errOut, "free up space or restore onto a larger disk") || strings.Contains(errOut, "restore failed") {
			t.Errorf("code = %d, err = %q, want the insufficient-space message", code, errOut)
		}
	})
}

func TestTermSafe(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{"escape", "a\x1b[31mred", "a?[31mred"},
		{"bell", "a\ab", "a?b"},
		{"nul", "a\x00b", "a?b"},
		{"delete", "a\x7fb", "a?b"},
		{"c1 control", "a\u009bb", "a?b"},
		{"newline and tab", "a\n\tb", "a??b"},
		{"plain unicode kept", "é – 日本語 ✓", "é – 日本語 ✓"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := termSafe(tt.in); got != tt.want {
				t.Errorf("termSafe(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
