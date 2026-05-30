package blobstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// Local is a Target backed by the local filesystem: objects become files under
// a root directory. Useful for a single-host backup to an attached disk, and
// the test backend for the backup logic.
type Local struct{ root string }

func newLocal(root string) *Local { return &Local{root: root} }

// Put writes data to root/name, creating parent directories.
func (l *Local) Put(_ context.Context, name string, data []byte) error {
	p := filepath.Join(l.root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return fmt.Errorf("blobstore: could not create the backup directory for %s (%w)", p, err)
	}
	if err := os.WriteFile(p, data, 0o600); err != nil {
		return fmt.Errorf("blobstore: could not write the backup object %s (%w)", p, err)
	}
	return nil
}

// Name returns the local root as a file:// URL.
func (l *Local) Name() string { return "file://" + l.root }
