package blobstore_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/hyperized/silo/internal/blobstore"
)

func mustOpen(t *testing.T, url string, opts ...blobstore.Option) blobstore.Target {
	t.Helper()
	tgt, err := blobstore.Open(url, opts...)
	if err != nil {
		t.Fatalf("Open(%q): %v", url, err)
	}
	return tgt
}

func TestOpen_Routing(t *testing.T) {
	cases := []struct {
		name     string
		url      string
		wantName string
	}{
		{"local path", "/var/backups/silo", "file:///var/backups/silo"},
		{"file url", "file:///data/bk", "file:///data/bk"},
		{"s3 with prefix", "s3://my-bucket/silo/prod", "s3://my-bucket/silo/prod"},
		{"s3 without prefix", "s3://my-bucket", "s3://my-bucket/"},
		{"s3 trailing slash", "s3://my-bucket/silo/", "s3://my-bucket/silo"},
		{"gs", "gs://my-bucket/silo", "gs://my-bucket/silo"},
		{"gs without prefix", "gs://my-bucket", "gs://my-bucket/"},
		{"az with prefix", "az://acct/container/silo", "az://acct/container/silo"},
		{"az without prefix", "az://acct/container", "az://acct/container/"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := mustOpen(t, c.url).Name(); got != c.wantName {
				t.Errorf("Name() = %q, want %q", got, c.wantName)
			}
		})
	}
}

func TestOpen_S3OptionsDoNotLeakIntoName(t *testing.T) {
	tgt := mustOpen(t, "s3://bucket/prefix",
		blobstore.WithS3Endpoint("https://minio.example.com:9000"),
		blobstore.WithS3Region("eu-west-1"),
		blobstore.WithS3PathStyle(true),
	)
	if got, want := tgt.Name(), "s3://bucket/prefix"; got != want {
		t.Errorf("Name() = %q, want %q", got, want)
	}
	if strings.Contains(tgt.Name(), "minio") {
		t.Errorf("Name() leaks the endpoint: %q", tgt.Name())
	}
}

func TestOpen_Errors(t *testing.T) {
	cases := []struct {
		name string
		url  string
	}{
		{"empty", ""},
		{"unsupported scheme", "ftp://h/p"},
		{"unparsable", "://bad"},
		{"az without container", "az://acct"},
		{"s3 without bucket", "s3://"},
		{"gs without bucket", "gs://"},
		{"credentials in url", "s3://user:pw@bucket/p"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tgt, err := blobstore.Open(c.url)
			if err == nil {
				t.Fatalf("Open(%q) = %v, want an error", c.url, tgt)
			}
			if strings.Contains(err.Error(), "pw") {
				t.Errorf("error leaks the password: %v", err)
			}
		})
	}
}

func TestValidateName(t *testing.T) {
	valid := []string{"a", "chunks/abc", "manifests/n/2026-10-28T12:00:00.000Z.json"}
	for _, n := range valid {
		t.Run("valid "+n, func(t *testing.T) {
			if err := blobstore.ValidateName(n); err != nil {
				t.Errorf("ValidateName(%q) = %v", n, err)
			}
		})
	}
	invalid := map[string]string{
		"empty":        "",
		"absolute":     "/abs",
		"double slash": "a//b",
		"dot segment":  "a/./b",
		"parent":       "../x",
		"trailing ..":  "a/..",
		"backslash":    "a\\b",
		"nul":          "a\x00b",
		"newline":      "a\nb",
		"delete":       "a\x7fb",
	}
	for label, n := range invalid {
		t.Run("invalid "+label, func(t *testing.T) {
			if err := blobstore.ValidateName(n); !errors.Is(err, blobstore.ErrInvalidName) {
				t.Errorf("ValidateName(%q) = %v, want ErrInvalidName", n, err)
			}
		})
	}
}

func TestValidateS3Endpoint(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		wantErr  bool
	}{
		{"http with port", "http://minio:9000", false},
		{"https with port", "https://minio.example.com:9000", false},
		{"https no port", "https://s3.example.com", false},
		{"bad scheme", "ftp://minio:9000", true},
		{"no scheme", "minio:9000", true},
		{"missing host", "https://", true},
		{"garbage", "://\x7f", true},
		{"credentials", "https://user:s3cret@minio:9000", true},
		{"query", "https://minio:9000?x=1", true},
		{"fragment", "https://minio:9000#frag", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := blobstore.ValidateS3Endpoint(c.endpoint)
			if (err != nil) != c.wantErr {
				t.Fatalf("ValidateS3Endpoint(%q) = %v, wantErr %v", c.endpoint, err, c.wantErr)
			}
			if err != nil && strings.Contains(err.Error(), "s3cret") {
				t.Errorf("error leaks the password: %v", err)
			}
		})
	}
}

func TestLocal_PutCreatesNestedDirs(t *testing.T) {
	dir := t.TempDir()
	tgt := mustOpen(t, dir)
	if !strings.HasPrefix(tgt.Name(), "file://") {
		t.Errorf("name = %q", tgt.Name())
	}
	if err := tgt.Put(context.Background(), "chunks/abc.chunk", []byte("ciphertext")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "chunks", "abc.chunk"))
	if err != nil || string(got) != "ciphertext" {
		t.Errorf("stored object = (%q, %v)", got, err)
	}
}

func TestLocal_RoundTripOverwriteDelete(t *testing.T) {
	ctx := context.Background()
	tgt := mustOpen(t, t.TempDir())

	if err := tgt.Put(ctx, "a/b", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if got, err := tgt.Get(ctx, "a/b", maxGet); err != nil || string(got) != "one" {
		t.Fatalf("Get = (%q, %v)", got, err)
	}
	if err := tgt.Put(ctx, "a/b", []byte("two-longer")); err != nil {
		t.Fatal(err)
	}
	if got, err := tgt.Get(ctx, "a/b", maxGet); err != nil || string(got) != "two-longer" {
		t.Fatalf("Get after overwrite = (%q, %v)", got, err)
	}
	if err := tgt.Delete(ctx, "a/b"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := tgt.Get(ctx, "a/b", maxGet); !errors.Is(err, blobstore.ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", err)
	}
	if err := tgt.Delete(ctx, "a/b"); err != nil {
		t.Errorf("second Delete should be a no-op, got %v", err)
	}
}

func TestLocal_GetMissing(t *testing.T) {
	tgt := mustOpen(t, t.TempDir())
	_, err := tgt.Get(context.Background(), "nope/x", maxGet)
	if !errors.Is(err, blobstore.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestLocal_List(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	tgt := mustOpen(t, dir)
	objects := map[string]string{
		"chunks/b":                 "bb",
		"chunks/a":                 "a",
		"chunksX/z":                "zzzz",
		"manifests/n/2.json":       "22",
		"manifests/n/1.json":       "1",
		"manifests/other/3.json":   "333",
		"top":                      "t",
		"manifests/n/pending.json": "p",
	}
	for n, d := range objects {
		if err := tgt.Put(ctx, n, []byte(d)); err != nil {
			t.Fatal(err)
		}
	}
	// Leftovers that are not objects: a temp file, an empty directory and a symlink.
	if err := os.WriteFile(filepath.Join(dir, "chunks", "half.tmp"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "chunks", "emptydir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "top"), filepath.Join(dir, "chunks", "link")); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		prefix string
		want   []string
	}{
		{"all", "", []string{"chunks/a", "chunks/b", "chunksX/z", "manifests/n/1.json", "manifests/n/2.json", "manifests/n/pending.json", "manifests/other/3.json", "top"}},
		{"chunks dir", "chunks/", []string{"chunks/a", "chunks/b"}},
		{"nested dir", "manifests/n/", []string{"manifests/n/1.json", "manifests/n/2.json", "manifests/n/pending.json"}},
		{"partial name", "manifests/n/p", []string{"manifests/n/pending.json"}},
		{"bare prefix", "chunks", []string{"chunks/a", "chunks/b", "chunksX/z"}},
		{"missing dir", "nothing/", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := tgt.List(ctx, c.prefix)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			var names []string
			for _, o := range got {
				names = append(names, o.Name)
				if o.Size != int64(len(objects[o.Name])) {
					t.Errorf("%s size = %d, want %d", o.Name, o.Size, len(objects[o.Name]))
				}
				if o.ModTime.IsZero() {
					t.Errorf("%s has a zero ModTime", o.Name)
				}
			}
			if !sort.StringsAreSorted(names) {
				t.Errorf("names not sorted: %v", names)
			}
			if strings.Join(names, ",") != strings.Join(c.want, ",") {
				t.Errorf("names = %v, want %v", names, c.want)
			}
		})
	}
}

func TestLocal_InvalidNames(t *testing.T) {
	ctx := context.Background()
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	tgt := mustOpen(t, root)

	for _, n := range []string{"", "../escape", "/abs", "a//b", "a\\b", "a/../../escape"} {
		t.Run(fmt.Sprintf("%q", n), func(t *testing.T) {
			if err := tgt.Put(ctx, n, []byte("x")); !errors.Is(err, blobstore.ErrInvalidName) {
				t.Errorf("Put = %v", err)
			}
			if _, err := tgt.Get(ctx, n, maxGet); !errors.Is(err, blobstore.ErrInvalidName) {
				t.Errorf("Get = %v", err)
			}
			if err := tgt.Delete(ctx, n); !errors.Is(err, blobstore.ErrInvalidName) {
				t.Errorf("Delete = %v", err)
			}
			if n == "" {
				return // an empty List prefix means "everything"
			}
			if _, err := tgt.List(ctx, n); !errors.Is(err, blobstore.ErrInvalidName) {
				t.Errorf("List = %v", err)
			}
		})
	}

	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "root" {
		t.Errorf("parent directory changed: %v", entries)
	}
	inRoot, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(inRoot) != 0 {
		t.Errorf("root should be empty, has %v", inRoot)
	}
}

func TestLocal_ErrorBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("put under a regular file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "afile"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := mustOpen(t, dir).Put(ctx, "afile/sub", []byte("y")); err == nil {
			t.Error("Put under a file should fail")
		}
	})

	t.Run("put onto an existing directory", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "d"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := mustOpen(t, dir).Put(ctx, "d", []byte("y")); err == nil {
			t.Error("Put onto a directory should fail")
		}
	})

	t.Run("get a directory", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "d"), 0o700); err != nil {
			t.Fatal(err)
		}
		_, err := mustOpen(t, dir).Get(ctx, "d", maxGet)
		if err == nil || errors.Is(err, blobstore.ErrNotFound) {
			t.Errorf("err = %v, want a non-NotFound read error", err)
		}
	})

	t.Run("get an unreadable file", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root ignores file permissions")
		}
		dir := t.TempDir()
		f := filepath.Join(dir, "locked")
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(f, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(f, 0o600) })
		_, err := mustOpen(t, dir).Get(ctx, "locked", maxGet)
		if err == nil || errors.Is(err, blobstore.ErrNotFound) {
			t.Errorf("err = %v, want a non-NotFound open error", err)
		}
	})

	t.Run("delete a non-empty directory", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "d", "child"), 0o700); err != nil {
			t.Fatal(err)
		}
		err := mustOpen(t, dir).Delete(ctx, "d")
		if err == nil || errors.Is(err, os.ErrNotExist) {
			t.Errorf("err = %v, want a non-NotExist error", err)
		}
	})

	t.Run("list unreadable directory", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root ignores directory permissions")
		}
		dir := t.TempDir()
		tgt := mustOpen(t, dir)
		if err := tgt.Put(ctx, "locked/x", []byte("x")); err != nil {
			t.Fatal(err)
		}
		locked := filepath.Join(dir, "locked")
		if err := os.Chmod(locked, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
		for _, prefix := range []string{"", "locked/"} {
			if _, err := tgt.List(ctx, prefix); err == nil {
				t.Errorf("List(%q) should fail on an unreadable directory", prefix)
			}
		}
	})
}

func TestLocal_Concurrent(t *testing.T) {
	ctx := context.Background()
	tgt := mustOpen(t, t.TempDir())
	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n*3)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("chunks/%02d", i)
			want := strings.Repeat("x", i+1)
			if err := tgt.Put(ctx, name, []byte(want)); err != nil {
				errs <- err
				return
			}
			got, err := tgt.Get(ctx, name, maxGet)
			if err != nil {
				errs <- err
				return
			}
			if string(got) != want {
				errs <- fmt.Errorf("Get(%s) = %q, want %q", name, got, want)
			}
			if _, err := tgt.List(ctx, "chunks/"); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	got, err := tgt.List(ctx, "chunks/")
	if err != nil || len(got) != n {
		t.Errorf("List = (%d objects, %v), want %d", len(got), err, n)
	}
}

const maxGet = 1 << 20

func TestLocal_GetLimit(t *testing.T) {
	ctx := context.Background()
	tgt := mustOpen(t, t.TempDir())
	if err := tgt.Put(ctx, "o", []byte("12345")); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		max     int64
		wantErr bool
	}{
		{"exactly at the limit", 5, false},
		{"one byte over", 4, true},
		{"well above", 100, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := tgt.Get(ctx, "o", c.max)
			if c.wantErr {
				if !errors.Is(err, blobstore.ErrTooLarge) {
					t.Fatalf("err = %v, want ErrTooLarge", err)
				}
				return
			}
			if err != nil || string(got) != "12345" {
				t.Errorf("Get = (%q, %v)", got, err)
			}
		})
	}
}

func TestLocal_PutIfAbsent(t *testing.T) {
	ctx := context.Background()

	t.Run("first creates and second leaves the original", func(t *testing.T) {
		dir := t.TempDir()
		tgt := mustOpen(t, dir)
		created, err := tgt.PutIfAbsent(ctx, "chunks/a", []byte("first"))
		if err != nil || !created {
			t.Fatalf("first = (%v, %v)", created, err)
		}
		created, err = tgt.PutIfAbsent(ctx, "chunks/a", []byte("second"))
		if err != nil || created {
			t.Fatalf("second = (%v, %v)", created, err)
		}
		got, err := tgt.Get(ctx, "chunks/a", maxGet)
		if err != nil || string(got) != "first" {
			t.Errorf("content = (%q, %v)", got, err)
		}
		entries, err := os.ReadDir(filepath.Join(dir, "chunks"))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Errorf("leftover files: %v", entries)
		}
	})

	t.Run("invalid name", func(t *testing.T) {
		_, err := mustOpen(t, t.TempDir()).PutIfAbsent(ctx, "../x", []byte("x"))
		if !errors.Is(err, blobstore.ErrInvalidName) {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("parent is a file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "afile"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		created, err := mustOpen(t, dir).PutIfAbsent(ctx, "afile/sub", []byte("y"))
		if err == nil || created {
			t.Errorf("= (%v, %v), want an error", created, err)
		}
	})

	t.Run("unwritable directory", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root ignores directory permissions")
		}
		dir := t.TempDir()
		ro := filepath.Join(dir, "ro")
		if err := os.Mkdir(ro, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(ro, 0o700) })
		created, err := mustOpen(t, dir).PutIfAbsent(ctx, "ro/x", []byte("y"))
		if err == nil || created {
			t.Errorf("= (%v, %v), want an error", created, err)
		}
	})

	t.Run("link fails for another reason than the name being taken", func(t *testing.T) {
		dir := t.TempDir()
		// A name past the 255-byte file name limit: the temp file is created
		// fine, the hard link is refused with ENAMETOOLONG.
		created, err := mustOpen(t, dir).PutIfAbsent(ctx, strings.Repeat("x", 300), []byte("y"))
		if err == nil || created {
			t.Fatalf("= (%v, %v), want an error", created, err)
		}
		entries, rerr := os.ReadDir(dir)
		if rerr != nil {
			t.Fatal(rerr)
		}
		if len(entries) != 0 {
			t.Errorf("temp file left behind: %v", entries)
		}
	})

	t.Run("concurrent writers of one name create it once", func(t *testing.T) {
		dir := t.TempDir()
		tgt := mustOpen(t, dir)
		const n = 8
		var wg sync.WaitGroup
		var mu sync.Mutex
		wins := 0
		errs := make(chan error, n)
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				created, err := tgt.PutIfAbsent(ctx, "chunks/same", []byte(fmt.Sprintf("writer-%d", i)))
				if err != nil {
					errs <- err
					return
				}
				if created {
					mu.Lock()
					wins++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		if wins != 1 {
			t.Errorf("%d writers reported created, want exactly 1", wins)
		}
		entries, err := os.ReadDir(filepath.Join(dir, "chunks"))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Errorf("leftover files: %v", entries)
		}
	})
}
