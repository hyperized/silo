// Package atomicfile replaces a small file so that a crash or power loss at any
// point leaves either the old content or the new content on disk, never a torn
// mix of the two.
//
// The sequence is the usual one: write a temp file in the same directory, fsync
// it, rename it over the target, then fsync the directory. Renaming alone is not
// enough. Without the file fsync the rename can reach the disk before the data
// does, and a crash leaves a correctly named but empty or truncated file. That is
// how extent maps came back as "unexpected end of JSON input" after a hard reset.
// Without the directory fsync the rename itself can be lost on recovery.
package atomicfile

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// TempSuffix is appended to the target path to name the temp file. A file with
// this suffix found at startup is the remains of a write that never finished
// and can be removed.
const TempSuffix = ".tmp"

// syncCloser is the part of *os.File that Write uses, so tests can inject
// failures into the write, fsync and close steps.
type syncCloser interface {
	io.Writer
	Sync() error
	Close() error
}

// File-system seams. Tests swap them to reach every error branch; production
// code never reassigns them.
var (
	openFile = func(path string, perm os.FileMode) (syncCloser, error) {
		// The caller owns path; this package only appends TempSuffix to it.
		return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm) // #nosec G304
	}
	openDir = func(dir string) (syncCloser, error) {
		return os.Open(dir) // #nosec G304
	}
	osRename = os.Rename
	osRemove = os.Remove
)

// Write replaces the file at path with data. When it returns nil the new content
// is durable: a crash after that point reads back data, and a crash at any
// earlier point reads back the previous content (or no file if there was none),
// never a partial write. perm applies when the file is created.
//
// On failure the temp file is removed and the error names the step that failed.
// A failure while syncing the directory comes after the rename, so the new
// content may already be visible but is not guaranteed to survive a crash.
//
// Write is safe to call concurrently for different paths. Concurrent writes to
// the same path share one temp file name and must be serialised by the caller.
func Write(path string, data []byte, perm os.FileMode) error {
	tmp := path + TempSuffix
	f, err := openFile(tmp, perm)
	if err != nil {
		return fmt.Errorf("atomicfile: could not create %s (%w)", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = osRemove(tmp)
		return fmt.Errorf("atomicfile: could not write %s (%w)", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = osRemove(tmp)
		return fmt.Errorf("atomicfile: could not flush %s to disk (%w)", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = osRemove(tmp)
		return fmt.Errorf("atomicfile: could not close %s (%w)", tmp, err)
	}
	if err := osRename(tmp, path); err != nil {
		_ = osRemove(tmp)
		return fmt.Errorf("atomicfile: could not move %s into place (%w)", path, err)
	}
	return syncDir(filepath.Dir(path))
}

// syncDir makes the rename in dir durable.
func syncDir(dir string) error {
	d, err := openDir(dir)
	if err != nil {
		return fmt.Errorf("atomicfile: could not open directory %s to flush it (%w)", dir, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("atomicfile: could not flush directory %s to disk (%w)", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("atomicfile: could not close directory %s (%w)", dir, err)
	}
	return nil
}
