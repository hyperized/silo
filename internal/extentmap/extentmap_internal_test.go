package extentmap

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/hlc"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// New(nil) must not panic and must default the logger.
func TestNew_NilLoggerDefaults(t *testing.T) {
	s := New(nil)
	if s.logger == nil {
		t.Fatal("New(nil) should default the logger")
	}
	s.Set("v", 0, "c", hlc.Timestamp{Wall: 1}) // in-memory, no dir: persist is a no-op
	if id, ok := s.Get("v", 0); !ok || id != "c" {
		t.Errorf("in-memory Set/Get failed: (%q,%v)", id, ok)
	}
}

// A marshal failure during persist is logged and swallowed, not fatal, and
// leaves no file behind.
func TestPersist_MarshalErrorIsSwallowed(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, quiet())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	orig := marshal
	marshal = func(any) ([]byte, error) { return nil, errors.New("boom") }
	defer func() { marshal = orig }()

	s.Set("vol", 0, "c0", hlc.Timestamp{Wall: 1}) // persist marshals -> error path
	// Nothing should have been written for vol.
	if _, statErr := os.Stat(s.filename("vol")); !os.IsNotExist(statErr) {
		t.Errorf("a marshal failure should leave no file, stat err = %v", statErr)
	}
	// In-memory state still applied.
	if id, ok := s.Get("vol", 0); !ok || id != "c0" {
		t.Errorf("in-memory state should still apply: (%q,%v)", id, ok)
	}
}

// A write failure during persist (read-only data dir) is logged and swallowed.
func TestPersist_WriteErrorIsSwallowed(t *testing.T) {
	skipIfRoot(t)
	dir := t.TempDir()
	s, err := Open(dir, quiet())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := os.Chmod(dir, 0o500); err != nil { // read+exec, no write
		t.Fatalf("chmod: %v", err)
	}
	defer func() { _ = os.Chmod(dir, 0o700) }() // restore so TempDir cleanup works

	s.Set("vol", 0, "c0", hlc.Timestamp{Wall: 1}) // write into a read-only dir -> error path, swallowed
	if id, ok := s.Get("vol", 0); !ok || id != "c0" {
		t.Errorf("in-memory state should still apply despite a write failure: (%q,%v)", id, ok)
	}
}

// Open surfaces a directory-read failure.
func TestOpen_ReadDirError(t *testing.T) {
	skipIfRoot(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	defer func() { _ = os.Chmod(dir, 0o700) }()

	if _, err := Open(dir, quiet()); err == nil {
		t.Error("Open should fail when the data dir cannot be read")
	}
}

// An unreadable per-volume file is logged and skipped, not fatal.
func TestLoad_UnreadableFileIsSkipped(t *testing.T) {
	skipIfRoot(t)
	dir := t.TempDir()
	bad := filepath.Join(dir, "x"+fileSuffix)
	if err := os.WriteFile(bad, []byte(`{"volume_id":"x"}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Chmod(bad, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	defer func() { _ = os.Chmod(bad, 0o600) }()

	s, err := Open(dir, quiet())
	if err != nil {
		t.Fatalf("Open should not fail on one unreadable file: %v", err)
	}
	if len(s.Volumes()) != 0 {
		t.Errorf("unreadable file should be skipped, got volumes %v", s.Volumes())
	}
}

// Delete surfaces a file-removal failure (a read-only data dir).
func TestDelete_RemoveErrorIsReturned(t *testing.T) {
	skipIfRoot(t)
	dir := t.TempDir()
	s, err := Open(dir, quiet())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.Set("v", 0, "c", hlc.Timestamp{Wall: 1})
	if err := os.Chmod(dir, 0o500); err != nil { // read+exec, no write -> Remove fails
		t.Fatalf("chmod: %v", err)
	}
	defer func() { _ = os.Chmod(dir, 0o700) }()

	if err := s.Delete("v"); err == nil {
		t.Error("Delete should surface a file-remove failure")
	}
}

// A stat failure while judging a map's age is collected, not fatal, and reaps
// nothing.
func TestReap_StatErrorIsCollected(t *testing.T) {
	skipIfRoot(t)
	dir := t.TempDir()
	s, err := Open(dir, quiet())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.Set("v", 0, "c", hlc.Timestamp{Wall: 1})
	if err := os.Chmod(dir, 0o000); err != nil { // no traverse -> stat fails with EACCES
		t.Fatalf("chmod: %v", err)
	}
	defer func() { _ = os.Chmod(dir, 0o700) }()

	reaped, err := s.Reap(map[string]struct{}{}, time.Now())
	if err == nil {
		t.Error("a stat failure should be reported")
	}
	if len(reaped) != 0 {
		t.Errorf("nothing should be reaped on a stat failure, got %v", reaped)
	}
}

// A removal failure during a reap is collected, not fatal.
func TestReap_DeleteErrorIsCollected(t *testing.T) {
	skipIfRoot(t)
	dir := t.TempDir()
	s, err := Open(dir, quiet())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.Set("v", 0, "c", hlc.Timestamp{Wall: 1})
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(s.filename("v"), past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if err := os.Chmod(dir, 0o500); err != nil { // stat ok (exec), remove fails (no write)
		t.Fatalf("chmod: %v", err)
	}
	defer func() { _ = os.Chmod(dir, 0o700) }()

	reaped, err := s.Reap(map[string]struct{}{}, time.Now().Add(-time.Hour))
	if err == nil {
		t.Error("a remove failure during reap should be reported")
	}
	if len(reaped) != 0 {
		t.Errorf("a failed reap should report nothing reaped, got %v", reaped)
	}
}

func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("permission-based test is meaningless as root")
	}
}
