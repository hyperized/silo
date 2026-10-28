package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hyperized/silo/internal/atomicfile"
)

// Local is a Target backed by the local filesystem: objects become files under
// a root directory. Useful for a single-host backup to an attached disk, and
// the test backend for the backup logic.
//
// Local is safe for concurrent use for different names. Two concurrent Puts of
// the same name share a temp file and must not overlap; the backup exporter
// never does that.
type Local struct{ root string }

func newLocal(root string) *Local { return &Local{root: root} }

// path maps a validated object name to its file. ValidateName has already
// rejected "..", absolute names and backslashes, so the result stays under root.
func (l *Local) path(name string) string {
	return filepath.Join(l.root, filepath.FromSlash(name))
}

// Put writes data to root/name, creating parent directories. The write goes
// through a temp file and a rename, so a crash or a concurrent List never sees
// half an object.
func (l *Local) Put(_ context.Context, name string, data []byte) error {
	if err := ValidateName(name); err != nil {
		return err
	}
	p := l.path(name)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return fmt.Errorf("blobstore: could not create the backup directory for %s (%w); check that the backup directory exists and silod may write to it", p, err)
	}
	if err := atomicfile.Write(p, data, 0o600); err != nil {
		return fmt.Errorf("blobstore: could not write the backup object %s (%w); check free space and permissions on the backup directory", p, err)
	}
	return nil
}

// PutIfAbsent writes root/name unless it exists. The content goes to a temp
// file first and is hard-linked into place, because link fails atomically when
// the name is taken while rename would silently replace it.
func (l *Local) PutIfAbsent(_ context.Context, name string, data []byte) (bool, error) {
	if err := ValidateName(name); err != nil {
		return false, err
	}
	p := l.path(name)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return false, fmt.Errorf("blobstore: could not create the backup directory for %s (%w); check that the backup directory exists and silod may write to it", p, err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".put-*"+atomicfile.TempSuffix)
	if err != nil {
		return false, fmt.Errorf("blobstore: could not write the backup object %s (%w); check free space and permissions on the backup directory", p, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	_, werr := tmp.Write(data)
	serr := tmp.Sync()
	cerr := tmp.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		// Not reachable in tests: a freshly created temp file cannot be
		// made to fail its write or sync without a filesystem fault.
		return false, fmt.Errorf("blobstore: could not write the backup object %s (%w); check free space and permissions on the backup directory", p, err)
	}
	if err := os.Link(tmp.Name(), p); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, nil
		}
		return false, fmt.Errorf("blobstore: could not write the backup object %s (%w); the backup directory must be on a filesystem that supports hard links", p, err)
	}
	return true, nil
}

// Get reads root/name, refusing files larger than maxBytes.
func (l *Local) Get(_ context.Context, name string, maxBytes int64) ([]byte, error) {
	if err := ValidateName(name); err != nil {
		return nil, err
	}
	f, err := os.Open(l.path(name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s in %s", ErrNotFound, name, l.Name())
	}
	if err != nil {
		return nil, fmt.Errorf("blobstore: could not read the backup object %s (%w); check permissions on the backup directory", name, err)
	}
	defer func() { _ = f.Close() }()
	return readCapped(f, maxBytes, name, l.Name())
}

// List walks the directory that holds prefix and returns the regular files
// whose names start with it. Leftover temp files from an interrupted Put are
// not objects and are skipped.
func (l *Local) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	if err := validatePrefix(prefix); err != nil {
		return nil, err
	}
	// Walk only the deepest directory the prefix pins down: listing
	// "chunks/" must not stat every manifest too.
	dir := ""
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		dir = prefix[:i]
	}
	start := l.root
	if dir != "" {
		start = l.path(dir)
	}
	var out []ObjectInfo
	err := filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == start && errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll
			}
			return err
		}
		if !d.Type().IsRegular() || strings.HasSuffix(p, atomicfile.TempSuffix) {
			return nil
		}
		rel, err := filepath.Rel(l.root, p)
		if err != nil {
			// Not reachable in tests: WalkDir only yields paths under start,
			// which is under root, so Rel cannot fail.
			return err
		}
		name := filepath.ToSlash(rel)
		if !strings.HasPrefix(name, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			// Not reachable in tests: only fails when the file vanishes
			// between the directory read and the stat.
			return err
		}
		out = append(out, ObjectInfo{Name: name, Size: info.Size(), ModTime: info.ModTime()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("blobstore: could not list %q in %s (%w); check that the backup directory is readable", prefix, l.Name(), err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Delete removes root/name. A missing file is not an error.
func (l *Local) Delete(_ context.Context, name string) error {
	if err := ValidateName(name); err != nil {
		return err
	}
	if err := os.Remove(l.path(name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("blobstore: could not delete the backup object %s (%w); check permissions on the backup directory", name, err)
	}
	return nil
}

// Name returns the local root as a file:// URL.
func (l *Local) Name() string { return "file://" + l.root }
