package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hyperized/silo/internal/crypto"
)

const (
	chunkExt = ".chunk"
	tmpExt   = ".chunk.tmp"
)

// FileStore stores each chunk as a single encrypted-envelope file inside
// one directory. A flat layout is fine while chunk counts are small;
// sharding by id prefix will replace it once a single directory becomes
// inefficient for the kernel's dirent cache.
type FileStore struct {
	root   string
	cipher *crypto.Cipher
	guard  *diskGuard
}

// diskGuard is the hard-watermark write fence. usage reports the backing
// filesystem's available and total bytes; Put refuses a write that would push
// usage at or past hard. Injected as an Option so the store stays decoupled from
// the diskusage package and tests can drive the fence deterministically.
type diskGuard struct {
	hard  float64
	usage func() (availableBytes, capacityBytes int64, err error)
}

// Option configures a FileStore. Follows the functional-options pattern so the
// disk guard (and future knobs) are opt-in without churning NewFileStore's
// signature.
type Option func(*FileStore)

// WithDiskGuard refuses writes once the data filesystem reaches the hard
// watermark (a fraction in (0,1]). usage returns the filesystem's available and
// total bytes — wire it to diskusage.Measure(dataDir). A measurement error
// fails open (the write is attempted) so a transient statfs hiccup cannot wedge
// all writes; the absolute backstop is then the filesystem's own ENOSPC.
func WithDiskGuard(hard float64, usage func() (availableBytes, capacityBytes int64, err error)) Option {
	return func(s *FileStore) {
		if hard > 0 && usage != nil {
			s.guard = &diskGuard{hard: hard, usage: usage}
		}
	}
}

// NewFileStore creates the data directory if it does not exist and
// fails fast on misconfiguration (missing cipher, non-writable root)
// so the operator gets a startup error instead of a runtime failure on
// the first Put.
func NewFileStore(root string, cipher *crypto.Cipher, opts ...Option) (*FileStore, error) {
	if cipher == nil {
		return nil, errors.New("silo: chunkstore needs a non-nil *crypto.Cipher; build one with crypto.NewCipher(cfg.EncryptionKey)")
	}
	if root == "" {
		return nil, errors.New("silo: chunkstore needs a non-empty data directory; set SILO_DATA_DIR to a writable path, e.g. /var/lib/silo")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("could not create the chunk data directory %q (%w); check the path is on a writable filesystem and silod has permission", root, err)
	}
	s := &FileStore{root: root, cipher: cipher}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

func (s *FileStore) path(id string) string {
	return filepath.Join(s.root, id+chunkExt)
}

// Put validates the id, encrypts the chunk, and writes it atomically
// (temp file + rename + dir fsync) so a crash never leaves a torn chunk.
func (s *FileStore) Put(_ context.Context, id string, data []byte) (Info, error) {
	if err := ValidateID(id); err != nil {
		return Info{}, err
	}
	envelope, err := s.cipher.EncryptChunk(data)
	if err != nil {
		return Info{}, fmt.Errorf("could not encrypt chunk %q for storage (%w); see silod logs for details", id, err)
	}
	if err := s.checkSpace(int64(len(envelope))); err != nil {
		return Info{}, err
	}

	final := s.path(id)
	tmp := filepath.Join(s.root, id+tmpExt)
	if err := writeAtomic(tmp, final, envelope, s.root); err != nil {
		return Info{}, fmt.Errorf("could not write chunk %q to disk (%w); check %s has free space and silod can write to it", id, err, s.root)
	}

	return Info{
		ID:          id,
		PlainBytes:  int64(len(data)),
		StoredBytes: int64(len(envelope)),
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// checkSpace enforces the hard disk watermark before a write. It refuses the
// chunk (ErrNoSpace) when storing it would push the data filesystem to or past
// the hard fraction. With no guard configured, or when the measurement fails,
// it allows the write — the filesystem's own ENOSPC is the ultimate backstop.
func (s *FileStore) checkSpace(envelopeBytes int64) error {
	if s.guard == nil {
		return nil
	}
	avail, capacity, err := s.guard.usage()
	if err != nil || capacity <= 0 {
		return nil // fail open: never let a statfs hiccup wedge all writes
	}
	// Reserve = the bytes that must stay free to keep usage below hard.
	reserve := int64(float64(capacity) * (1 - s.guard.hard))
	if avail-envelopeBytes < reserve {
		return fmt.Errorf("could not store chunk: %w (available %d B, reserve %d B of %d B capacity)", ErrNoSpace, avail, reserve, capacity)
	}
	return nil
}

// RawChunk returns the chunk's on-disk encrypted envelope as-is, without
// decrypting it. Backups use this so the exported copy stays AES-GCM encrypted
// at rest under the cluster key, exactly like the live chunk.
func (s *FileStore) RawChunk(_ context.Context, id string) ([]byte, error) {
	if err := ValidateID(id); err != nil {
		return nil, err
	}
	// id passed ValidateID (ASCII allowlist, no separators), so the joined path
	// cannot escape s.root.
	data, err := os.ReadFile(s.path(id)) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("could not read chunk %q for backup (%w)", id, err)
	}
	return data, nil
}

// Get returns the decrypted chunk. A missing chunk maps to ErrNotFound
// so callers can branch with errors.Is rather than parsing messages.
func (s *FileStore) Get(_ context.Context, id string) ([]byte, Info, error) {
	if err := ValidateID(id); err != nil {
		return nil, Info{}, err
	}
	// id passed ValidateID above (ASCII allowlist, no separators), so the
	// joined path cannot escape s.root — not attacker-controlled traversal.
	path := s.path(id)
	envelope, err := os.ReadFile(path) // #nosec G304
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, Info{}, ErrNotFound
		}
		return nil, Info{}, fmt.Errorf("could not read chunk %q (%w); check %s permissions and disk health", id, err, path)
	}
	plaintext, err := s.cipher.DecryptChunk(envelope)
	if err != nil {
		return nil, Info{}, fmt.Errorf("could not decrypt chunk %q (%w); see internal/crypto error for next steps", id, err)
	}

	info := Info{
		ID:          id,
		PlainBytes:  int64(len(plaintext)),
		StoredBytes: int64(len(envelope)),
	}
	if stat, err := os.Stat(path); err == nil {
		info.CreatedAt = stat.ModTime().UTC()
	}
	return plaintext, info, nil
}

// Delete removes the chunk file and fsyncs the directory so the removal
// survives a crash. A missing chunk maps to ErrNotFound.
func (s *FileStore) Delete(_ context.Context, id string) error {
	if err := ValidateID(id); err != nil {
		return err
	}
	if err := os.Remove(s.path(id)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("could not delete chunk %q (%w); check filesystem permissions on %s", id, err, s.root)
	}
	return fsyncDir(s.root)
}

// Stat returns chunk metadata from the filesystem without reading or
// decrypting the payload. A missing chunk maps to ErrNotFound.
func (s *FileStore) Stat(_ context.Context, id string) (Info, error) {
	if err := ValidateID(id); err != nil {
		return Info{}, err
	}
	stat, err := os.Stat(s.path(id))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Info{}, ErrNotFound
		}
		return Info{}, fmt.Errorf("could not stat chunk %q (%w); check %s is accessible", id, err, s.root)
	}
	return Info{
		ID:          id,
		PlainBytes:  stat.Size() - int64(crypto.OverheadBytes),
		StoredBytes: stat.Size(),
		CreatedAt:   stat.ModTime().UTC(),
	}, nil
}

// List returns the id of every fully-written chunk in the store. In-flight
// temp files (id.chunk.tmp) end in .tmp, not .chunk, so they are skipped —
// the scrubber must never treat a half-written chunk as a replica.
func (s *FileStore) List(_ context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, fmt.Errorf("could not list chunks in %s (%w); check the data directory is readable", s.root, err)
	}
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, chunkExt) {
			continue
		}
		ids = append(ids, strings.TrimSuffix(name, chunkExt))
	}
	return ids, nil
}

// syncCloser is the subset of *os.File writeAtomic needs. Pulled out so
// tests can swap openExclusiveFile to return a fake that fails Write,
// Sync, or Close on demand — without those seams the post-OpenFile error
// paths are unreachable from a unit test and silently rot.
type syncCloser interface {
	io.Writer
	Sync() error
	Close() error
}

// File-system seams. Swap in tests; never mutate from production code.
// Keeping them at package scope (rather than threading through a struct)
// matches the osHostname pattern in internal/config.
var (
	openExclusiveFile = func(path string, mode os.FileMode) (syncCloser, error) {
		// O_EXCL avoids racing with a concurrent Put of the same id; the
		// second caller fails fast rather than corrupting the first one's
		// tmp file. path derives from a ValidateID'd chunk id under s.root.
		return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_EXCL, mode) // #nosec G304
	}
	osRename = os.Rename
	osRemove = os.Remove
)

// writeAtomic gives crash consistency: write to a tmp file, fsync,
// rename, fsync the directory. A power loss at any step leaves either
// the previous chunk (if any) or no chunk, never a half-written one.
// The fsync on the directory is the step everyone forgets — without it,
// the rename can be undone by the journal on recovery.
func writeAtomic(tmp, final string, data []byte, dir string) error {
	f, err := openExclusiveFile(tmp, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = osRemove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = osRemove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = osRemove(tmp)
		return err
	}
	if err := osRename(tmp, final); err != nil {
		_ = osRemove(tmp)
		return err
	}
	return fsyncDir(dir)
}

// fsyncDir flushes the directory entry to make rename/unlink durable.
// Most "the file was there a moment ago" bugs after a crash come from
// skipping this step.
func fsyncDir(dir string) error {
	// dir is the store root from operator config, not request input.
	d, err := os.Open(dir) // #nosec G304
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
