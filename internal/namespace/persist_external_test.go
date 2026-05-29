package namespace_test

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestNamespace_PersistRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "namespace.json")

	first, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := first.Mkdir("/docs"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if _, err := first.Touch("/docs/readme"); err != nil {
		t.Fatalf("Touch: %v", err)
	}

	// A second Open of the same file recovers the state written by the first.
	second, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := names(mustList(t, second, "/")); len(got) != 1 || got[0] != "docs" {
		t.Fatalf("reopened root = %v, want [docs]", got)
	}
	if got := names(mustList(t, second, "/docs")); len(got) != 1 || got[0] != "readme" {
		t.Fatalf("reopened /docs = %v, want [readme]", got)
	}

	// A removal also persists across a reopen.
	if err := second.Remove("/docs/readme"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	third, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("third open: %v", err)
	}
	if got := names(mustList(t, third, "/docs")); len(got) != 0 {
		t.Errorf("removal did not persist: /docs = %v", got)
	}
}

func TestNamespace_OpenFreshPath(t *testing.T) {
	ns, err := namespace.Open(hlc.New("a"), filepath.Join(t.TempDir(), "absent.json"), discardLogger())
	if err != nil {
		t.Fatalf("Open of a missing file should succeed: %v", err)
	}
	if got := names(mustList(t, ns, "/")); len(got) != 0 {
		t.Errorf("fresh namespace should be empty, got %v", got)
	}
}

func TestNamespace_OpenCorruptFileRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "namespace.json")
	if err := os.WriteFile(path, []byte("not json at all"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	// A corrupt file is logged and ignored, not fatal — the node boots empty
	// and re-learns from peers.
	ns, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("corrupt file should not fail Open: %v", err)
	}
	if got := names(mustList(t, ns, "/")); len(got) != 0 {
		t.Errorf("namespace should start empty after a corrupt load, got %v", got)
	}
}

func TestNamespace_OpenReadError(t *testing.T) {
	// A directory at the state path makes ReadFile fail with a non-not-exist
	// error, which must fail Open.
	dir := t.TempDir()
	if _, err := namespace.Open(hlc.New("a"), dir, discardLogger()); err == nil {
		t.Fatal("Open should fail when the state path is unreadable")
	}
}

func TestNamespace_OpenEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "namespace.json")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	ns, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("Open of an empty file: %v", err)
	}
	if got := names(mustList(t, ns, "/")); len(got) != 0 {
		t.Errorf("empty-file namespace should be empty, got %v", got)
	}
}

func TestNamespace_PersistWriteErrorIsLoggedNotFatal(t *testing.T) {
	// Point the state file inside a directory that does not exist: the load
	// sees "not found" (fine), but the persist on mutation cannot write.
	path := filepath.Join(t.TempDir(), "missing-dir", "namespace.json")
	ns, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// The mutation still succeeds in memory even though persistence fails.
	if _, err := ns.Mkdir("/x"); err != nil {
		t.Fatalf("Mkdir should succeed despite a persist failure: %v", err)
	}
	if got := names(mustList(t, ns, "/")); len(got) != 1 || got[0] != "x" {
		t.Errorf("in-memory state should hold despite persist failure, got %v", got)
	}
}

func TestNamespace_OpenNilLoggerDefaults(t *testing.T) {
	// A nil logger must not panic — Open falls back to slog.Default.
	if _, err := namespace.Open(hlc.New("a"), filepath.Join(t.TempDir(), "ns.json"), nil); err != nil {
		t.Fatalf("Open with nil logger: %v", err)
	}
}
