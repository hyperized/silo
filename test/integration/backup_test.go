//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/config"
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

// s3TestImage matches the image deploy/docker-compose.yml pins, so the test
// exercises the same server developers run locally.
const s3TestImage = "rustfs/rustfs:1.0.4@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2"

// bucketSeq keeps bucket names unique within one test binary run.
var bucketSeq atomic.Int64

type fakeChunks map[string][]byte

func (f fakeChunks) List(context.Context) ([]string, error) {
	ids := make([]string, 0, len(f))
	for id := range f {
		ids = append(ids, id)
	}
	return ids, nil
}

func (f fakeChunks) RawChunk(_ context.Context, id string) ([]byte, error) {
	return f[id], nil
}

type fakeExtents struct{}

func (fakeExtents) Volumes() []string { return []string{"vol-1"} }

func (fakeExtents) Snapshot(string) []crdt.MapEntry[uint64, string] {
	return []crdt.MapEntry[uint64, string]{{Key: 0, Value: "chunk-0000", TS: hlc.Timestamp{Wall: 1, Node: "node-a"}}}
}

// s3Endpoint is a reachable S3 server plus the credentials to use it.
type s3Endpoint struct {
	url, accessKey, secretKey string
}

// startS3 returns the server named by SILO_TEST_S3_ENDPOINT, or starts a
// throwaway container. The test is skipped when neither is possible.
func startS3(t *testing.T) s3Endpoint {
	t.Helper()
	if ep := os.Getenv("SILO_TEST_S3_ENDPOINT"); ep != "" {
		return s3Endpoint{url: ep, accessKey: os.Getenv("SILO_TEST_S3_ACCESS_KEY"), secretKey: os.Getenv("SILO_TEST_S3_SECRET_KEY")}
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed: install it, or set SILO_TEST_S3_ENDPOINT (plus SILO_TEST_S3_ACCESS_KEY and SILO_TEST_S3_SECRET_KEY) to use an existing S3 server")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("the docker daemon is not reachable: start it, or set SILO_TEST_S3_ENDPOINT to use an existing S3 server")
	}

	ep := s3Endpoint{accessKey: "silo-test", secretKey: "silo-test-secret-key"}
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", "127.0.0.1::9000",
		"-e", "RUSTFS_ACCESS_KEY="+ep.accessKey, "-e", "RUSTFS_SECRET_KEY="+ep.secretKey,
		s3TestImage).CombinedOutput()
	if err != nil {
		t.Fatalf("could not start %s (%v): %s\nCheck that docker can pull images, or set SILO_TEST_S3_ENDPOINT to use an existing S3 server", s3TestImage, err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })

	portOut, err := exec.Command("docker", "port", id, "9000/tcp").Output()
	if err != nil {
		t.Fatalf("docker port: %v", err)
	}
	// Output is "127.0.0.1:49153" and may repeat for IPv6.
	line, _, _ := strings.Cut(strings.TrimSpace(string(portOut)), "\n")
	ep.url = "http://" + strings.TrimSpace(line)

	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := http.Get(ep.url + "/health") //nolint:noctx // bounded by the deadline loop
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return ep
			}
		}
		if time.Now().After(deadline) {
			logs, _ := exec.Command("docker", "logs", id).CombinedOutput()
			t.Fatalf("S3 server at %s was not healthy within 60s: %s", ep.url, logs)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// newBackupTarget creates a fresh bucket and opens a target in it.
func newBackupTarget(t *testing.T, ep s3Endpoint) blobstore.Target {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", ep.accessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", ep.secretKey)
	t.Setenv("AWS_REGION", "us-east-1")

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatalf("load AWS config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ep.url)
		o.UsePathStyle = true
	})
	bucket := fmt.Sprintf("silo-it-%d-%d", time.Now().Unix(), bucketSeq.Add(1))
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}

	target, err := blobstore.Open("s3://"+bucket+"/it",
		blobstore.WithS3Endpoint(ep.url), blobstore.WithS3Region("us-east-1"), blobstore.WithS3PathStyle(true))
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	return target
}

// TestBackupLifecycle runs export, re-export, verify, restore and prune in
// order against a real S3 server. The steps share state, so they are not
// parallel.
func TestBackupLifecycle(t *testing.T) {
	ep := startS3(t)
	target := newBackupTarget(t, ep)
	ctx := context.Background()

	// More than one ListObjectsV2 page (1000 keys) so the pagination path runs.
	const chunkCount = 1200
	chunks := make(fakeChunks, chunkCount)
	for i := range chunkCount {
		chunks[fmt.Sprintf("chunk-%04d", i)] = fmt.Appendf(nil, "payload-%d", i)
	}
	ns := namespace.New(hlc.New("node-a"))
	wantNS, err := ns.Snapshot()
	if err != nil {
		t.Fatalf("namespace snapshot: %v", err)
	}

	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := base
	exp := backup.NewExporter(chunks, ns, "node-a",
		backup.WithExtentSource(fakeExtents{}),
		backup.WithVersion("it"),
		backup.WithClock(func() time.Time { return clock }))

	first, err := exp.Export(ctx, target)
	if err != nil {
		t.Fatalf("first export: %v", err)
	}
	t.Run("first export uploads everything", func(t *testing.T) {
		if first.Uploaded != chunkCount || first.Skipped != 0 {
			t.Fatalf("uploaded=%d skipped=%d, want %d/0", first.Uploaded, first.Skipped, chunkCount)
		}
		wantKey := backup.ManifestKey("node-a", base.Format("2006-01-02T15:04:05.000Z"))
		if first.Manifest != wantKey {
			t.Fatalf("manifest = %q, want %q", first.Manifest, wantKey)
		}
		for _, name := range []string{wantKey, backup.ManifestKey("node-a", backup.LatestManifest)} {
			if _, err := target.Get(ctx, name, backup.MaxManifestBytes); err != nil {
				t.Fatalf("get %s: %v", name, err)
			}
		}
		objs, err := target.List(ctx, "chunks/")
		if err != nil {
			t.Fatalf("list chunks: %v", err)
		}
		if len(objs) != chunkCount {
			t.Fatalf("listed %d chunks across pages, want %d", len(objs), chunkCount)
		}
	})

	clock = base.Add(time.Minute)
	second, err := exp.Export(ctx, target)
	if err != nil {
		t.Fatalf("second export: %v", err)
	}
	t.Run("second export skips everything", func(t *testing.T) {
		if second.Uploaded != 0 || second.Skipped != chunkCount {
			t.Fatalf("uploaded=%d skipped=%d, want 0/%d", second.Uploaded, second.Skipped, chunkCount)
		}
	})

	m, err := backup.LoadManifest(ctx, target, "node-a", backup.LatestManifest)
	if err != nil {
		t.Fatalf("load latest manifest: %v", err)
	}
	t.Run("verify finds nothing missing", func(t *testing.T) {
		for _, deep := range []bool{false, true} {
			problems, err := backup.Verify(ctx, target, m, backup.VerifyOptions{Deep: deep})
			if err != nil {
				t.Fatalf("verify (deep=%v): %v", deep, err)
			}
			if len(problems) != 0 {
				t.Fatalf("problems (deep=%v): %v", deep, problems[:min(len(problems), 5)])
			}
		}
	})

	t.Run("restore reproduces the source", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "restore")
		res, err := backup.Restore(ctx, target, m, dir, backup.RestoreOptions{})
		if err != nil {
			t.Fatalf("restore: %v", err)
		}
		if res.Chunks != chunkCount || res.Volumes != 1 {
			t.Fatalf("restored chunks=%d volumes=%d, want %d/1", res.Chunks, res.Volumes, chunkCount)
		}
		for id, want := range chunks {
			got, err := os.ReadFile(chunkstore.FilePath(dir, id))
			if err != nil {
				t.Fatalf("read restored chunk %s: %v", id, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("chunk %s differs after restore", id)
			}
		}
		gotNS, err := os.ReadFile(filepath.Join(dir, config.NamespaceFileName))
		if err != nil {
			t.Fatalf("read restored namespace: %v", err)
		}
		if !bytes.Equal(gotNS, wantNS) {
			t.Fatalf("namespace differs after restore")
		}
		if _, err := os.Stat(filepath.Join(dir, config.ExtentsDirName)); err != nil {
			t.Fatalf("extent maps were not restored: %v", err)
		}
	})

	t.Run("prune keeps referenced chunks and honours the grace window", func(t *testing.T) {
		const orphan = "chunks/orphan-chunk"
		if err := target.Put(ctx, orphan, []byte("unreferenced")); err != nil {
			t.Fatalf("put orphan: %v", err)
		}
		grace := backup.PruneGrace(time.Hour)
		opts := backup.PruneOptions{NodeID: "node-a", Retain: 1, Grace: grace, Now: time.Now()}

		res, err := backup.Prune(ctx, target, opts)
		if err != nil || res.Failures != 0 {
			t.Fatalf("prune inside grace: err=%v failures=%d first=%v", err, res.Failures, res.FirstError)
		}
		if res.ManifestsDeleted != 1 {
			t.Fatalf("manifests deleted = %d, want 1", res.ManifestsDeleted)
		}
		oldKey := first.Manifest
		if _, err := target.Get(ctx, oldKey, backup.MaxManifestBytes); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("old manifest still readable after prune: %v", err)
		}
		if _, err := target.Get(ctx, second.Manifest, backup.MaxManifestBytes); err != nil {
			t.Fatalf("newest manifest lost: %v", err)
		}
		if _, err := target.Get(ctx, orphan, 1<<20); err != nil {
			t.Fatalf("orphan deleted inside the grace window: %v", err)
		}

		opts.Now = time.Now().Add(grace + time.Hour)
		if res, err = backup.Prune(ctx, target, opts); err != nil || res.Failures != 0 {
			t.Fatalf("prune after grace: err=%v failures=%d first=%v", err, res.Failures, res.FirstError)
		}
		if _, err := target.Get(ctx, orphan, 1<<20); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("orphan survived past the grace window: %v", err)
		}
		problems, err := backup.Verify(ctx, target, m, backup.VerifyOptions{Deep: true})
		if err != nil || len(problems) != 0 {
			t.Fatalf("referenced objects lost by prune: err=%v problems=%d", err, len(problems))
		}
	})
}

// TestBackupMissingObjects pins down how a real S3 server reports absent keys,
// which the blobstore contract depends on.
func TestBackupMissingObjects(t *testing.T) {
	target := newBackupTarget(t, startS3(t))
	ctx := context.Background()

	t.Run("get reports ErrNotFound", func(t *testing.T) {
		if _, err := target.Get(ctx, "chunks/absent", 1<<20); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("Get of a missing key = %v, want ErrNotFound", err)
		}
	})
	t.Run("delete of a missing key succeeds", func(t *testing.T) {
		if err := target.Delete(ctx, "chunks/absent"); err != nil {
			t.Fatalf("Delete of a missing key = %v, want nil", err)
		}
	})
}

// exportSmall runs one export of a few chunks and returns its manifest.
func exportSmall(t *testing.T, target blobstore.Target, chunks fakeChunks) (*backup.Manifest, backup.Result) {
	t.Helper()
	ctx := context.Background()
	exp := backup.NewExporter(chunks, namespace.New(hlc.New("node-a")), "node-a",
		backup.WithExtentSource(fakeExtents{}), backup.WithVersion("it"))
	res, err := exp.Export(ctx, target)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	m, err := backup.LoadManifest(ctx, target, "node-a", backup.LatestManifest)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	return m, res
}

// TestBackupPutIfAbsent checks that the server honours If-None-Match, which
// the write-once chunk objects depend on.
func TestBackupPutIfAbsent(t *testing.T) {
	target := newBackupTarget(t, startS3(t))
	ctx := context.Background()
	const name = "chunks/write-once"

	created, err := target.PutIfAbsent(ctx, name, []byte("first"))
	if err != nil || !created {
		t.Fatalf("first PutIfAbsent: created=%v err=%v, want true/nil", created, err)
	}
	created, err = target.PutIfAbsent(ctx, name, []byte("second"))
	if err != nil || created {
		t.Fatalf("second PutIfAbsent: created=%v err=%v, want false/nil (the server may ignore If-None-Match)", created, err)
	}
	got, err := target.Get(ctx, name, 1<<20)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "first" {
		t.Fatalf("object holds %q after the second PutIfAbsent, want %q", got, "first")
	}
}

// TestBackupDeepVerifyAndTamper covers checksum verification end to end.
func TestBackupDeepVerifyAndTamper(t *testing.T) {
	target := newBackupTarget(t, startS3(t))
	ctx := context.Background()
	chunks := fakeChunks{"chunk-0000": []byte("aaaaaaaa"), "chunk-0001": []byte("bbbbbbbb")}
	m, _ := exportSmall(t, target, chunks)

	problems, err := backup.Verify(ctx, target, m, backup.VerifyOptions{Deep: true})
	if err != nil || len(problems) != 0 {
		t.Fatalf("deep verify of a good backup: err=%v problems=%v", err, problems)
	}

	if err := target.Put(ctx, "chunks/chunk-0001", []byte("cccccccc")); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	t.Run("default verify misses same-size damage", func(t *testing.T) {
		problems, err := backup.Verify(ctx, target, m, backup.VerifyOptions{})
		if err != nil || len(problems) != 0 {
			t.Fatalf("default verify: err=%v problems=%v, want none", err, problems)
		}
	})
	t.Run("deep verify reports the checksum", func(t *testing.T) {
		problems, err := backup.Verify(ctx, target, m, backup.VerifyOptions{Deep: true})
		if err != nil {
			t.Fatalf("deep verify: %v", err)
		}
		if len(problems) != 1 || problems[0].Name != "chunks/chunk-0001" || !strings.Contains(strings.ToLower(problems[0].Reason), "checksum") {
			t.Fatalf("problems = %+v, want one checksum problem for chunks/chunk-0001", problems)
		}
	})
	t.Run("restore refuses the tampered chunk", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "restore")
		_, err := backup.Restore(ctx, target, m, dir, backup.RestoreOptions{})
		if !errors.Is(err, backup.ErrChecksumMismatch) {
			t.Fatalf("restore err = %v, want ErrChecksumMismatch", err)
		}
		if _, err := os.Stat(chunkstore.FilePath(dir, "chunk-0001")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("tampered chunk was written to the restore dir: %v", err)
		}
	})
}

// TestBackupRunMarkerHoldsBackPrune checks that another node's fresh run
// marker stops chunk deletion, and that removing it lets the prune proceed.
func TestBackupRunMarkerHoldsBackPrune(t *testing.T) {
	target := newBackupTarget(t, startS3(t))
	ctx := context.Background()
	exportSmall(t, target, fakeChunks{"chunk-0000": []byte("aaaaaaaa")})

	const (
		orphan = "chunks/orphan-chunk"
		marker = "runs/other-node/2026-01-01T00:00:00.000Z.json"
	)
	if err := target.Put(ctx, orphan, []byte("unreferenced")); err != nil {
		t.Fatalf("put orphan: %v", err)
	}
	if err := target.Put(ctx, marker, []byte("{}")); err != nil {
		t.Fatalf("put marker: %v", err)
	}
	// Both objects were just written: with Now a minute ahead and a 1ms
	// grace the chunk is old enough to delete and the marker is still
	// younger than the one-hour staleness limit.
	opts := backup.PruneOptions{NodeID: "node-a", Retain: 1, Grace: time.Millisecond, Now: time.Now().Add(time.Minute)}

	res, err := backup.Prune(ctx, target, opts)
	if err != nil {
		t.Fatalf("prune with a marker: %v", err)
	}
	if res.ChunksHeldBack == "" {
		t.Fatalf("ChunksHeldBack is empty, want a reason while another node has a fresh marker")
	}
	if _, err := target.Get(ctx, orphan, 1<<20); err != nil {
		t.Fatalf("orphan deleted despite a fresh marker: %v", err)
	}

	if err := target.Delete(ctx, marker); err != nil {
		t.Fatalf("delete marker: %v", err)
	}
	if res, err = backup.Prune(ctx, target, opts); err != nil || res.ChunksHeldBack != "" {
		t.Fatalf("prune without a marker: err=%v heldBack=%q", err, res.ChunksHeldBack)
	}
	if _, err := target.Get(ctx, orphan, 1<<20); !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("orphan survived the prune: %v", err)
	}
}

// TestBackupReadLimitAndRunMarkers covers the read size cap and marker cleanup.
func TestBackupReadLimitAndRunMarkers(t *testing.T) {
	target := newBackupTarget(t, startS3(t))
	ctx := context.Background()

	t.Run("get above maxBytes returns ErrTooLarge", func(t *testing.T) {
		if err := target.Put(ctx, "chunks/big", bytes.Repeat([]byte("x"), 100)); err != nil {
			t.Fatalf("put: %v", err)
		}
		if _, err := target.Get(ctx, "chunks/big", 10); !errors.Is(err, blobstore.ErrTooLarge) {
			t.Fatalf("Get with a small cap = %v, want ErrTooLarge", err)
		}
		if got, err := target.Get(ctx, "chunks/big", 100); err != nil || len(got) != 100 {
			t.Fatalf("Get at exactly the cap: len=%d err=%v", len(got), err)
		}
	})
	t.Run("export leaves no run markers", func(t *testing.T) {
		exportSmall(t, target, fakeChunks{"chunk-0000": []byte("aaaaaaaa")})
		objs, err := target.List(ctx, "runs/")
		if err != nil {
			t.Fatalf("list runs: %v", err)
		}
		if len(objs) != 0 {
			t.Fatalf("run markers left behind: %v", objs)
		}
	})
}
