package atomicfile_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperized/silo/internal/atomicfile"
)

func TestWrite_CreatesAndReplaces(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	for _, content := range []string{"first", "second, longer than the first", "3"} {
		if err := atomicfile.Write(path, []byte(content), 0o600); err != nil {
			t.Fatalf("Write(%q): %v", content, err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read back: %v", err)
		}
		if string(got) != content {
			t.Errorf("content = %q, want %q", got, content)
		}
	}
	if _, err := os.Stat(path + atomicfile.TempSuffix); !os.IsNotExist(err) {
		t.Errorf("temp file left behind after a successful write: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("perm = %v, want 0600", fi.Mode().Perm())
	}
}

func TestWrite_MissingDirectoryFailsAndLeavesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "state.json")
	if err := atomicfile.Write(path, []byte("x"), 0o600); err == nil {
		t.Fatal("Write into a missing directory should fail")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("target should not exist, stat err = %v", err)
	}
}

// A failed rename keeps the old content intact and cleans up the temp file.
func TestWrite_FailedRenameKeepsOldContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// A non-empty directory at the target makes the rename fail on every
	// platform without needing permissions games.
	if err := os.MkdirAll(filepath.Join(path, "occupied"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := atomicfile.Write(path, []byte("new"), 0o600); err == nil {
		t.Fatal("Write over a non-empty directory should fail")
	}
	if _, err := os.Stat(path + atomicfile.TempSuffix); !os.IsNotExist(err) {
		t.Errorf("temp file left behind after a failed rename: %v", err)
	}
	if fi, err := os.Stat(path); err != nil || !fi.IsDir() {
		t.Errorf("the original entry should be untouched: %v", err)
	}
}
