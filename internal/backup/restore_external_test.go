package backup_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/extentmap"
	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

func extentObjectKey(stamp, volume string) string {
	return "extents/node-a/" + stamp + "/" + base64.RawURLEncoding.EncodeToString([]byte(volume)) + ".json"
}

func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("file permissions are not enforced for root")
	}
}

// realSources builds a namespace with content and an extent store with two
// volumes, the shapes silod hands the exporter.
func realSources(t *testing.T) (*namespace.Namespace, *extentmap.Store) {
	t.Helper()
	ns := namespace.New(hlc.New("node-a"))
	if _, err := ns.Mkdir("/docs"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if _, err := ns.CreateVolume("/docs/v1", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	store := extentmap.New(quietLogger())
	for i, c := range []string{"c1", "c2"} {
		if err := store.Set("vol-1", uint64(i), c, hlc.Timestamp{Wall: int64(i) + 1, Node: "node-a"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Set("vol-2", 7, "c2", hlc.Timestamp{Wall: 9, Node: "node-a"}); err != nil {
		t.Fatal(err)
	}
	return ns, store
}

func TestRestoreRoundTripLocalTarget(t *testing.T) {
	tgt, err := blobstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ns, store := realSources(t)
	chunks := newChunks(map[string]string{"c1": "first-chunk-bytes", "c2": "second"})
	e := backup.NewExporter(chunks, ns, testNode, backup.WithClock(newClock(t0, 0).Now), backup.WithExtentSource(store))
	export(t, e, tgt)

	ctx := context.Background()
	m, err := backup.LoadManifest(ctx, tgt, testNode, "latest")
	if err != nil {
		t.Fatal(err)
	}
	for _, deep := range []bool{false, true} {
		problems, err := backup.Verify(ctx, tgt, m, backup.VerifyOptions{Deep: deep})
		if err != nil || len(problems) != 0 {
			t.Fatalf("Verify(deep=%v) = %v, %v", deep, problems, err)
		}
	}

	dir := filepath.Join(t.TempDir(), "restored", "data")
	var lastDone, lastTotal int
	res, err := backup.Restore(ctx, tgt, m, dir, backup.RestoreOptions{
		Concurrency: 2,
		Progress:    func(done, total int) { lastDone, lastTotal = done, total },
	})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if res.Chunks != 2 || res.Volumes != 2 || res.Bytes != int64(len("first-chunk-bytes")+len("second")) {
		t.Fatalf("result = %+v", res)
	}
	if lastDone != 2 || lastTotal != 2 {
		t.Fatalf("progress ended at %d/%d", lastDone, lastTotal)
	}

	// Snapshot output order is not stable between calls, so compare with the
	// bytes the exporter stored.
	wantSnap, err := tgt.Get(ctx, m.Namespace.Key, backup.MaxMetadataBytes)
	if err != nil {
		t.Fatal(err)
	}
	gotSnap, err := os.ReadFile(filepath.Join(dir, "namespace.json"))
	if err != nil || !bytes.Equal(gotSnap, wantSnap) {
		t.Fatalf("namespace.json differs (err %v)", err)
	}

	restored, err := extentmap.Open(filepath.Join(dir, "extents"), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	for _, vol := range store.Volumes() {
		if !slices.Equal(restored.Snapshot(vol), store.Snapshot(vol)) {
			t.Fatalf("extent map %s differs: %v vs %v", vol, restored.Snapshot(vol), store.Snapshot(vol))
		}
	}
	if !reflect.DeepEqual(restored.Volumes(), store.Volumes()) {
		t.Fatalf("volumes = %v", restored.Volumes())
	}
	for id, want := range map[string]string{"c1": "first-chunk-bytes", "c2": "second"} {
		got, err := os.ReadFile(chunkstore.FilePath(dir, id))
		if err != nil || string(got) != want {
			t.Fatalf("chunk %s = %q, %v", id, got, err)
		}
	}
}

// goodBackup exports a small backup to a memTarget and returns its manifest.
func goodBackup(t *testing.T) (*memTarget, *backup.Manifest) {
	t.Helper()
	tgt := newMemTarget()
	e := backup.NewExporter(threeChunks(), fakeNS{snap: []byte(`{"version":1}`)}, testNode,
		backup.WithClock(newClock(t0, 0).Now), backup.WithExtentSource(twoVolumes()))
	res := export(t, e, tgt)
	m := decodeManifest(t, tgt.bytesOf(res.Manifest))
	return tgt, &m
}

func TestRestoreDestinations(t *testing.T) {
	ctx := context.Background()
	t.Run("existing empty directory", func(t *testing.T) {
		// A namespace snapshot that namespace.MergeBytes rejects would fail
		// the restore, so use a real one.
		tgt, m := goodBackupWithRealNamespace(t)
		dir := t.TempDir()
		if _, err := backup.Restore(ctx, tgt, m, dir, backup.RestoreOptions{}); err != nil {
			t.Fatalf("Restore: %v", err)
		}
	})
	t.Run("non-empty directory is refused and untouched", func(t *testing.T) {
		tgt, m := goodBackupWithRealNamespace(t)
		dir := t.TempDir()
		keep := filepath.Join(dir, "precious")
		if err := os.WriteFile(keep, []byte("mine"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := backup.Restore(ctx, tgt, m, dir, backup.RestoreOptions{})
		if !errors.Is(err, backup.ErrDataDirNotEmpty) {
			t.Fatalf("err = %v, want ErrDataDirNotEmpty", err)
		}
		entries, _ := os.ReadDir(dir)
		if len(entries) != 1 {
			t.Fatalf("directory changed: %v", entries)
		}
	})
	t.Run("path is a file", func(t *testing.T) {
		tgt, m := goodBackupWithRealNamespace(t)
		f := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(f, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := backup.Restore(ctx, tgt, m, f, backup.RestoreOptions{})
		if err == nil || !strings.Contains(err.Error(), "is a file") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unwritable parent", func(t *testing.T) {
		skipIfRoot(t)
		tgt, m := goodBackupWithRealNamespace(t)
		parent := t.TempDir()
		chmod(t, parent, 0o500)
		_, err := backup.Restore(ctx, tgt, m, filepath.Join(parent, "sub"), backup.RestoreOptions{})
		if err == nil || !strings.Contains(err.Error(), "could not create the restore directory") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("inaccessible parent", func(t *testing.T) {
		skipIfRoot(t)
		tgt, m := goodBackupWithRealNamespace(t)
		parent := t.TempDir()
		chmod(t, parent, 0)
		_, err := backup.Restore(ctx, tgt, m, filepath.Join(parent, "sub"), backup.RestoreOptions{})
		if err == nil || !strings.Contains(err.Error(), "could not inspect") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unreadable directory", func(t *testing.T) {
		skipIfRoot(t)
		tgt, m := goodBackupWithRealNamespace(t)
		dir := t.TempDir()
		chmod(t, dir, 0o300)
		_, err := backup.Restore(ctx, tgt, m, dir, backup.RestoreOptions{})
		if err == nil || !strings.Contains(err.Error(), "could not read the restore directory") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unwritable empty directory", func(t *testing.T) {
		skipIfRoot(t)
		tgt, m := goodBackupWithRealNamespace(t)
		dir := t.TempDir()
		chmod(t, dir, 0o500)
		_, err := backup.Restore(ctx, tgt, m, dir, backup.RestoreOptions{})
		if err == nil || !strings.Contains(err.Error(), "could not write the namespace") {
			t.Fatalf("err = %v", err)
		}
	})
}

func chmod(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o700) })
}

func goodBackupWithRealNamespace(t *testing.T) (*memTarget, *backup.Manifest) {
	t.Helper()
	tgt := newMemTarget()
	ns, _ := realSources(t)
	e := backup.NewExporter(threeChunks(), ns, testNode, backup.WithClock(newClock(t0, 0).Now), backup.WithExtentSource(twoVolumes()))
	res := export(t, e, tgt)
	m := decodeManifest(t, tgt.bytesOf(res.Manifest))
	return tgt, &m
}

// replaceObject stores data under key and updates the manifest's reference to
// it, so the object passes the checksum and fails only on its content.
func replaceObject(tgt *memTarget, m *backup.Manifest, key string, data []byte) {
	tgt.set(key, data, t0)
	size, sum := int64(len(data)), sha256Of(data)
	if m.Namespace.Key == key {
		m.Namespace.Size, m.Namespace.SHA256 = size, sum
	}
	for i := range m.Extents {
		if m.Extents[i].Key == key {
			m.Extents[i].Size, m.Extents[i].SHA256 = size, sum
		}
	}
	for i := range m.Chunks {
		if "chunks/"+m.Chunks[i].ID == key {
			m.Chunks[i].Size, m.Chunks[i].SHA256 = size, sum
		}
	}
}

func TestRestoreDamagedBackups(t *testing.T) {
	boom := errors.New("boom")
	volKey := extentObjectKey(stamp0, "vol-2")
	nsKey := "namespace/node-a/" + stamp0 + ".json"
	cases := []struct {
		name   string
		damage func(tgt *memTarget, m *backup.Manifest)
		want   string
		wantIs error
	}{
		{"missing namespace", func(tgt *memTarget, _ *backup.Manifest) { tgt.remove(nsKey) }, "restore the namespace snapshot", blobstore.ErrNotFound},
		{"undecodable namespace", func(tgt *memTarget, m *backup.Manifest) { replaceObject(tgt, m, nsKey, []byte("not json")) }, "is damaged", nil},
		{"missing extent", func(tgt *memTarget, _ *backup.Manifest) { tgt.remove(volKey) }, "restore the extent map", blobstore.ErrNotFound},
		{"undecodable extent", func(tgt *memTarget, m *backup.Manifest) { replaceObject(tgt, m, volKey, []byte("{oops")) }, "is damaged", nil},
		{"extent for another volume", func(tgt *memTarget, m *backup.Manifest) {
			replaceObject(tgt, m, volKey, []byte(`{"volume_id":"other","entries":[]}`))
		}, "does not match the volume", nil},
		{"extent without volume id", func(tgt *memTarget, m *backup.Manifest) {
			replaceObject(tgt, m, volKey, []byte(`{"volume_id":"","entries":[]}`))
		}, "does not match the volume", nil},
		{"missing chunk", func(tgt *memTarget, _ *backup.Manifest) { tgt.remove("chunks/c2") }, "restore chunk c2", blobstore.ErrNotFound},
		{"emptied chunk", func(tgt *memTarget, _ *backup.Manifest) { tgt.set("chunks/c2", nil, t0) }, "restore chunk c2", backup.ErrChecksumMismatch},
		{"chunk get error", func(tgt *memTarget, _ *backup.Manifest) { tgt.failGet("chunks/c3", boom) }, "restore chunk c3", boom},
		{"namespace get error", func(tgt *memTarget, _ *backup.Manifest) { tgt.failGet(nsKey, boom) }, "restore the namespace snapshot", boom},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tgt, m := goodBackupWithRealNamespace(t)
			c.damage(tgt, m)
			dir := filepath.Join(t.TempDir(), "r")
			_, err := backup.Restore(context.Background(), tgt, m, dir, backup.RestoreOptions{Concurrency: 1})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want it to contain %q", err, c.want)
			}
			if c.wantIs != nil && !errors.Is(err, c.wantIs) {
				t.Fatalf("err = %v, want it to wrap %v", err, c.wantIs)
			}
			if strings.Contains(c.want, "chunk") && !strings.Contains(err.Error(), "delete "+dir) {
				t.Fatalf("chunk failure does not tell the operator to delete the directory: %v", err)
			}
		})
	}
}

func TestRestoreRejectsContentThatDiffersFromTheManifest(t *testing.T) {
	ctx := context.Background()
	nsKey := "namespace/node-a/" + stamp0 + ".json"
	run := func(t *testing.T, damage func(tgt *memTarget, m *backup.Manifest)) (string, error) {
		t.Helper()
		tgt, m := goodBackupWithRealNamespace(t)
		damage(tgt, m)
		dir := filepath.Join(t.TempDir(), "r")
		_, err := backup.Restore(ctx, tgt, m, dir, backup.RestoreOptions{Concurrency: 1})
		return dir, err
	}
	sameSizeAs := func(tgt *memTarget, key string) []byte {
		return bytes.Repeat([]byte("X"), len(tgt.bytesOf(key)))
	}

	t.Run("namespace", func(t *testing.T) {
		dir, err := run(t, func(tgt *memTarget, _ *backup.Manifest) { tgt.set(nsKey, sameSizeAs(tgt, nsKey), t0) })
		if !errors.Is(err, backup.ErrChecksumMismatch) {
			t.Fatalf("err = %v, want ErrChecksumMismatch", err)
		}
		if _, statErr := os.Stat(filepath.Join(dir, "namespace.json")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("mismatching namespace was written: %v", statErr)
		}
	})
	t.Run("namespace of another size", func(t *testing.T) {
		_, err := run(t, func(tgt *memTarget, _ *backup.Manifest) { tgt.set(nsKey, []byte("{}"), t0) })
		if !errors.Is(err, backup.ErrChecksumMismatch) {
			t.Fatalf("err = %v, want ErrChecksumMismatch", err)
		}
	})
	t.Run("extent", func(t *testing.T) {
		var first string
		dir, err := run(t, func(tgt *memTarget, m *backup.Manifest) {
			first = m.Extents[0].Key
			tgt.set(first, sameSizeAs(tgt, first), t0)
		})
		if !errors.Is(err, backup.ErrChecksumMismatch) || !strings.Contains(err.Error(), first) {
			t.Fatalf("err = %v, want ErrChecksumMismatch naming %s", err, first)
		}
		entries, readErr := os.ReadDir(filepath.Join(dir, "extents"))
		if readErr != nil || len(entries) != 0 {
			t.Fatalf("mismatching extent map was written: %v (%v)", entries, readErr)
		}
	})
	t.Run("chunk", func(t *testing.T) {
		dir, err := run(t, func(tgt *memTarget, _ *backup.Manifest) { tgt.set("chunks/c2", sameSizeAs(tgt, "chunks/c2"), t0) })
		if !errors.Is(err, backup.ErrChecksumMismatch) || !strings.Contains(err.Error(), "chunk c2") {
			t.Fatalf("err = %v, want ErrChecksumMismatch for chunk c2", err)
		}
		if _, statErr := os.Stat(chunkstore.FilePath(dir, "c2")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("mismatching chunk was written: %v", statErr)
		}
	})
	t.Run("chunk of another size", func(t *testing.T) {
		dir, err := run(t, func(tgt *memTarget, _ *backup.Manifest) { tgt.set("chunks/c2", []byte("longer than before"), t0) })
		if !errors.Is(err, backup.ErrChecksumMismatch) {
			t.Fatalf("err = %v, want ErrChecksumMismatch", err)
		}
		if _, statErr := os.Stat(chunkstore.FilePath(dir, "c2")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("mismatching chunk was written: %v", statErr)
		}
	})
	t.Run("oversized chunk", func(t *testing.T) {
		dir, err := run(t, func(tgt *memTarget, _ *backup.Manifest) {
			tgt.set("chunks/c2", make([]byte, backup.MaxChunkBytes+1), t0)
		})
		if !errors.Is(err, blobstore.ErrTooLarge) {
			t.Fatalf("err = %v, want ErrTooLarge", err)
		}
		if _, statErr := os.Stat(chunkstore.FilePath(dir, "c2")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("oversized chunk was written: %v", statErr)
		}
	})
}

func TestRestoreWriteFailures(t *testing.T) {
	skipIfRoot(t)
	t.Run("extent store cannot be opened", func(t *testing.T) {
		tgt, m := goodBackupWithRealNamespace(t)
		dir := filepath.Join(t.TempDir(), "r")
		tgt.setGetHook(func(name string) {
			if strings.HasPrefix(name, "namespace/") {
				if err := os.WriteFile(filepath.Join(dir, "extents"), nil, 0o600); err != nil {
					t.Error(err)
				}
			}
		})
		_, err := backup.Restore(context.Background(), tgt, m, dir, backup.RestoreOptions{})
		if err == nil || !strings.Contains(err.Error(), "could not prepare the extent maps") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("extent map cannot be saved", func(t *testing.T) {
		tgt, m := goodBackupWithRealNamespace(t)
		dir := filepath.Join(t.TempDir(), "r")
		tgt.setGetHook(func(name string) {
			if strings.HasPrefix(name, "extents/") {
				chmod(t, filepath.Join(dir, "extents"), 0o500)
			}
		})
		_, err := backup.Restore(context.Background(), tgt, m, dir, backup.RestoreOptions{})
		if err == nil || !strings.Contains(err.Error(), "could not write the extent map") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("chunk cannot be saved", func(t *testing.T) {
		tgt, m := goodBackupWithRealNamespace(t)
		dir := filepath.Join(t.TempDir(), "r")
		tgt.setGetHook(func(name string) {
			if strings.HasPrefix(name, "chunks/") {
				chmod(t, dir, 0o500)
			}
		})
		_, err := backup.Restore(context.Background(), tgt, m, dir, backup.RestoreOptions{Concurrency: 1})
		if err == nil || !strings.Contains(err.Error(), "could not write chunk") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestRestoreRefusesInvalidManifestBeforeWriting(t *testing.T) {
	tgt, m := goodBackup(t)
	m.Chunks = []backup.ChunkRef{{ID: "../../etc/passwd", Size: 1, SHA256: validSum}}
	dir := filepath.Join(t.TempDir(), "r")
	_, err := backup.Restore(context.Background(), tgt, m, dir, backup.RestoreOptions{})
	if !errors.Is(err, backup.ErrInvalidManifest) {
		t.Fatalf("err = %v", err)
	}
	if _, statErr := os.Stat(dir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("restore directory was created: %v", statErr)
	}
}

func TestRestoreWithoutExtents(t *testing.T) {
	tgt := newMemTarget()
	ns, _ := realSources(t)
	res := export(t, backup.NewExporter(threeChunks(), ns, testNode, backup.WithClock(newClock(t0, 0).Now)), tgt)
	m := decodeManifest(t, tgt.bytesOf(res.Manifest))
	dir := filepath.Join(t.TempDir(), "r")
	rr, err := backup.Restore(context.Background(), tgt, &m, dir, backup.RestoreOptions{})
	if err != nil || rr.Volumes != 0 || rr.Chunks != 3 {
		t.Fatalf("res = %+v err = %v", rr, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "extents")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("extents directory should not exist: %v", err)
	}
}

func TestVerify(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	names := func(ps []backup.Problem) []string {
		out := make([]string, len(ps))
		for i, p := range ps {
			out[i] = p.Name
		}
		return out
	}
	reasons := func(ps []backup.Problem) map[string]string {
		out := map[string]string{}
		for _, p := range ps {
			out[p.Name] = p.Reason
		}
		return out
	}
	for _, deep := range []bool{false, true} {
		opts := backup.VerifyOptions{Deep: deep, Concurrency: 2}
		t.Run(fmt.Sprintf("deep=%v", deep), func(t *testing.T) {
			t.Run("complete backup", func(t *testing.T) {
				tgt, m := goodBackup(t)
				problems, err := backup.Verify(ctx, tgt, m, opts)
				if err != nil || len(problems) != 0 {
					t.Fatalf("problems = %v err = %v", problems, err)
				}
			})
			t.Run("reports missing and mismatched sizes, sorted", func(t *testing.T) {
				tgt, m := goodBackup(t)
				tgt.remove(m.Namespace.Key)
				tgt.remove(m.Extents[0].Key)
				tgt.remove("chunks/c3")
				tgt.set("chunks/c1", nil, t0)
				tgt.set("chunks/c2", []byte("bbbbbb"), t0)
				problems, err := backup.Verify(ctx, tgt, m, opts)
				if err != nil {
					t.Fatal(err)
				}
				want := []string{"chunks/c1", "chunks/c2", "chunks/c3", m.Extents[0].Key, m.Namespace.Key}
				slices.Sort(want)
				if !reflect.DeepEqual(names(problems), want) {
					t.Fatalf("problems = %v, want names %v", problems, want)
				}
				r := reasons(problems)
				if r["chunks/c3"] != "missing" || r[m.Namespace.Key] != "missing" {
					t.Fatalf("reasons = %v", r)
				}
				if !strings.Contains(r["chunks/c1"], "size is 0 bytes, the backup recorded 3") || !strings.Contains(r["chunks/c2"], "size is 6 bytes, the backup recorded 2") {
					t.Fatalf("reasons = %v", r)
				}
			})
			t.Run("list errors propagate", func(t *testing.T) {
				for _, key := range []string{"chunks/", "namespace/node-a/", "extents/node-a/" + stamp0 + "/"} {
					tgt, m := goodBackup(t)
					tgt.failList(key, boom)
					if _, err := backup.Verify(ctx, tgt, m, opts); !errors.Is(err, boom) {
						t.Fatalf("list %s: err = %v", key, err)
					}
				}
			})
			t.Run("invalid manifest", func(t *testing.T) {
				tgt, m := goodBackup(t)
				m.Format = 0
				if _, err := backup.Verify(ctx, tgt, m, opts); !errors.Is(err, backup.ErrInvalidManifest) {
					t.Fatalf("err = %v", err)
				}
			})
		})
	}

	t.Run("default mode does not download", func(t *testing.T) {
		tgt, m := goodBackup(t)
		tgt.resetGets()
		tgt.set("chunks/c1", []byte("zzz"), t0) // same size, other content
		problems, err := backup.Verify(ctx, tgt, m, backup.VerifyOptions{})
		if err != nil || len(problems) != 0 {
			t.Fatalf("problems = %v err = %v", problems, err)
		}
		if n := tgt.getCount(""); n != 0 {
			t.Fatalf("default mode downloaded %d objects", n)
		}
	})

	deep := backup.VerifyOptions{Deep: true}
	t.Run("deep reports a checksum mismatch", func(t *testing.T) {
		tgt, m := goodBackup(t)
		tgt.set("chunks/c1", []byte("zzz"), t0)
		tgt.set(m.Namespace.Key, bytes.Repeat([]byte("x"), int(m.Namespace.Size)), t0)
		problems, err := backup.Verify(ctx, tgt, m, deep)
		if err != nil {
			t.Fatal(err)
		}
		r := reasons(problems)
		if len(problems) != 2 || !strings.Contains(r["chunks/c1"], "checksum mismatch") || !strings.Contains(r[m.Namespace.Key], "checksum mismatch") {
			t.Fatalf("problems = %v", problems)
		}
	})
	t.Run("deep reports an object that vanishes after the listing", func(t *testing.T) {
		tgt, m := goodBackup(t)
		tgt.setGetHook(func(name string) {
			if name == "chunks/c2" {
				tgt.remove(name)
			}
		})
		problems, err := backup.Verify(ctx, tgt, m, deep)
		if err != nil || len(problems) != 1 || problems[0] != (backup.Problem{Name: "chunks/c2", Reason: "missing"}) {
			t.Fatalf("problems = %v err = %v", problems, err)
		}
	})
	t.Run("deep reports an oversized object", func(t *testing.T) {
		tgt, m := goodBackup(t)
		tgt.failGet("chunks/c3", fmt.Errorf("%w: planted", blobstore.ErrTooLarge))
		problems, err := backup.Verify(ctx, tgt, m, deep)
		if err != nil || len(problems) != 1 || problems[0].Name != "chunks/c3" || !strings.Contains(problems[0].Reason, "larger than silo ever writes") {
			t.Fatalf("problems = %v err = %v", problems, err)
		}
	})
	t.Run("deep returns other download errors", func(t *testing.T) {
		tgt, m := goodBackup(t)
		tgt.failGet("chunks/c1", boom)
		problems, err := backup.Verify(ctx, tgt, m, deep)
		if !errors.Is(err, boom) || problems != nil {
			t.Fatalf("problems = %v err = %v", problems, err)
		}
	})
}

// A trimmed extent is backed up as an unmap entry and restored as one: it must
// not come back mapped, and its released chunk need not be in the backup.
func TestRestoreRoundTripsUnmappedExtent(t *testing.T) {
	tgt, err := blobstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ns, store := realSources(t)
	// Extent 1 of vol-1 was bound to c2 and then trimmed.
	if err := store.Set("vol-1", 1, extentmap.Unmapped, hlc.Timestamp{Wall: 20, Node: "node-a"}); err != nil {
		t.Fatal(err)
	}
	chunks := newChunks(map[string]string{"c1": "first-chunk-bytes", "c2": "second"})
	e := backup.NewExporter(chunks, ns, testNode, backup.WithClock(newClock(t0, 0).Now), backup.WithExtentSource(store))
	export(t, e, tgt)

	ctx := context.Background()
	m, err := backup.LoadManifest(ctx, tgt, testNode, "latest")
	if err != nil {
		t.Fatal(err)
	}
	if problems, err := backup.Verify(ctx, tgt, m, backup.VerifyOptions{Deep: true}); err != nil || len(problems) != 0 {
		t.Fatalf("Verify = %v, %v", problems, err)
	}
	dir := filepath.Join(t.TempDir(), "data")
	if _, err := backup.Restore(ctx, tgt, m, dir, backup.RestoreOptions{}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	restored, err := extentmap.Open(filepath.Join(dir, "extents"), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(restored.Snapshot("vol-1"), store.Snapshot("vol-1")) {
		t.Fatalf("vol-1 restored as %v, want %v", restored.Snapshot("vol-1"), store.Snapshot("vol-1"))
	}
	if id, ok := restored.Get("vol-1", 1); ok {
		t.Errorf("the trimmed extent came back mapped to %q", id)
	}
	if got := restored.Len("vol-1"); got != 1 {
		t.Errorf("restored Len = %d, want 1 mapped extent", got)
	}
}
