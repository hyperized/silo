package blobstore_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/blobstore"
)

func TestLocalTarget_Put(t *testing.T) {
	dir := t.TempDir()
	tgt, err := blobstore.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !strings.HasPrefix(tgt.Name(), "file://") {
		t.Errorf("name = %q", tgt.Name())
	}
	// A nested object name creates parent directories.
	if err := tgt.Put(context.Background(), "chunks/abc.chunk", []byte("ciphertext")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "chunks", "abc.chunk"))
	if err != nil || string(got) != "ciphertext" {
		t.Errorf("stored object = (%q, %v)", got, err)
	}

	// Writing under a path whose parent is a file fails.
	blocker := filepath.Join(dir, "afile")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := tgt.Put(context.Background(), "afile/sub", []byte("y")); err == nil {
		t.Error("Put under a file should fail")
	}
}

func TestOpen_Routing(t *testing.T) {
	cases := []struct {
		url      string
		wantName string
	}{
		{"/var/backups/silo", "file:///var/backups/silo"},
		{"file:///data/bk", "file:///data/bk"},
		{"s3://my-bucket/silo/prod", "s3://my-bucket/silo/prod"},
		{"gs://my-bucket/silo", "gs://my-bucket/silo"},
		{"az://acct/container/silo", "az://acct/container/silo"},
	}
	for _, c := range cases {
		tgt, err := blobstore.Open(c.url)
		if err != nil {
			t.Errorf("Open(%q): %v", c.url, err)
			continue
		}
		if tgt.Name() != c.wantName {
			t.Errorf("Open(%q).Name() = %q, want %q", c.url, tgt.Name(), c.wantName)
		}
	}
}

func TestOpen_Errors(t *testing.T) {
	bad := []string{"", "ftp://host/path", "://bad", "az://acct"}
	for _, u := range bad {
		if _, err := blobstore.Open(u); err == nil {
			t.Errorf("Open(%q) should error", u)
		}
	}
}
