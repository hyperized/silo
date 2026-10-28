package backup

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"

	"github.com/hyperized/silo/internal/atomicfile"
	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/config"
	"github.com/hyperized/silo/internal/diskusage"
	"github.com/hyperized/silo/internal/extentmap"
	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

// ErrDataDirNotEmpty is returned by Restore when the destination already holds
// files. Restore never deletes or overwrites anything.
var ErrDataDirNotEmpty = errors.New("backup: the restore directory is not empty")

// ErrInsufficientSpace is returned by Restore when the destination filesystem
// has less free space than the backup needs.
var ErrInsufficientSpace = errors.New("backup: not enough free space for the restore")

// spaceMargin is extra free space required on top of the backup's size, for
// the extent-map store's own files and filesystem overhead.
const spaceMargin int64 = 64 << 20

// measureDisk is swapped in tests; production uses statfs where available.
var measureDisk = diskusage.Measure

// RestoreOptions tunes Restore. The zero value is usable.
type RestoreOptions struct {
	// Concurrency bounds parallel chunk downloads; values below one use
	// DefaultConcurrency.
	Concurrency int
	// Progress, when set, is called after each chunk is written with the
	// number done so far and the total. Calls are serialised.
	Progress func(done, total int)
	// Logger receives the extent-map store's own diagnostics. Nil discards
	// them.
	Logger *slog.Logger
	// Notice, when set, receives messages worth showing the operator that
	// are not errors, such as a skipped free-space check.
	Notice func(msg string)
}

// RestoreResult summarises a restore.
type RestoreResult struct {
	Chunks  int
	Bytes   int64
	Volumes int
}

// Restore writes the state described by m into dir using the layout silod
// reads at boot: the namespace replica, the extent maps, and every chunk
// envelope, still encrypted. dir must not exist or must be empty.
//
// Chunk ids are assigned by the writer (writer id, epoch and counter), not
// derived from content, so a downloaded chunk cannot be checked against its
// id. Instead every object is checked against the size and SHA-256 the
// manifest recorded before anything is written, so the directory never holds
// content that differs from what the backup run stored. The AES-GCM tag inside
// each envelope is verified the first time silod reads the chunk with the
// cluster key.
//
// When Restore fails part-way, dir is left as it is; delete it before trying
// again.
func Restore(ctx context.Context, target blobstore.Target, m *Manifest, dir string, opts RestoreOptions) (RestoreResult, error) {
	if err := m.Validate(); err != nil {
		return RestoreResult{}, err
	}
	if err := prepareRestoreDir(dir); err != nil {
		return RestoreResult{}, err
	}
	if opts.Concurrency < 1 {
		opts.Concurrency = DefaultConcurrency
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Notice == nil {
		opts.Notice = func(string) {}
	}
	if err := checkFreeSpace(dir, m.TotalBytes()+spaceMargin, opts.Notice); err != nil {
		return RestoreResult{}, err
	}

	if err := restoreNamespace(ctx, target, m, dir); err != nil {
		return RestoreResult{}, err
	}
	vols, err := restoreExtents(ctx, target, m, dir, opts.Logger)
	if err != nil {
		return RestoreResult{}, err
	}
	res, err := restoreChunks(ctx, target, m, dir, opts)
	res.Volumes = vols
	return res, err
}

// prepareRestoreDir creates dir, or accepts it if it is an empty directory.
func prepareRestoreDir(dir string) error {
	info, err := os.Stat(dir)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("backup: could not create the restore directory %s (%w); check that its parent exists and is writable", dir, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("backup: could not inspect the restore directory %s (%w); check its permissions", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("backup: %s is a file, not a directory; choose a new directory path for --data-dir", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("backup: could not read the restore directory %s (%w); check its permissions", dir, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("%w: %s already holds %d entries; restore into a new or empty directory (nothing was changed)", ErrDataDirNotEmpty, dir, len(entries))
	}
	return nil
}

// checkFreeSpace refuses a restore that cannot fit. Where the filesystem
// cannot be measured the check is skipped with a notice rather than blocking
// the restore.
func checkFreeSpace(dir string, need int64, notice func(string)) error {
	u, err := measureDisk(dir)
	if err != nil {
		notice(fmt.Sprintf("could not check free space in %s (%v); make sure it has at least %d bytes free", dir, err, need))
		return nil
	}
	if u.AvailableBytes < need {
		return fmt.Errorf("%w: %s has %d bytes free but the backup needs about %d; free up space or restore onto a larger disk (nothing was written)", ErrInsufficientSpace, dir, u.AvailableBytes, need)
	}
	return nil
}

// fetchVerified downloads key and checks it against the recorded size and
// checksum.
func fetchVerified(ctx context.Context, target blobstore.Target, key string, size int64, sum string, maxBytes int64) ([]byte, error) {
	data, err := target.Get(ctx, key, maxBytes)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != size || sha256Hex(data) != sum {
		return nil, fmt.Errorf("%w: %s on %s differs from what the backup recorded (it was changed or damaged after the backup ran)", ErrChecksumMismatch, key, target.Name())
	}
	return data, nil
}

// restoreNamespace downloads the snapshot, checks that it decodes, and writes
// it where silod loads its namespace replica. silod ignores an undecodable
// namespace file at boot, so a damaged snapshot must be caught here instead.
func restoreNamespace(ctx context.Context, target blobstore.Target, m *Manifest, dir string) error {
	ns := m.Namespace
	snap, err := fetchVerified(ctx, target, ns.Key, ns.Size, ns.SHA256, MaxMetadataBytes)
	if err != nil {
		return fmt.Errorf("backup: could not restore the namespace snapshot %s (%w); run 'siloctl backup verify --deep' to check the backup, or restore an older one", ns.Key, err)
	}
	if err := namespace.New(hlc.New(m.NodeID)).MergeBytes(snap); err != nil {
		return fmt.Errorf("backup: the namespace snapshot %s is damaged (%w); restore an older backup", ns.Key, err)
	}
	if err := atomicfile.Write(filepath.Join(dir, config.NamespaceFileName), snap, 0o600); err != nil {
		return fmt.Errorf("backup: could not write the namespace into %s (%w); check free space", dir, err)
	}
	return nil
}

// restoreExtents downloads each extent map and merges it into an extent-map
// store opened on the restored data directory, so the files on disk are
// written by the same code silod uses.
func restoreExtents(ctx context.Context, target blobstore.Target, m *Manifest, dir string, logger *slog.Logger) (int, error) {
	if len(m.Extents) == 0 {
		return 0, nil
	}
	store, err := extentmap.Open(filepath.Join(dir, config.ExtentsDirName), logger)
	if err != nil {
		return 0, fmt.Errorf("backup: could not prepare the extent maps in %s (%w)", dir, err)
	}
	for _, ref := range m.Extents {
		key := ref.Key
		raw, err := fetchVerified(ctx, target, key, ref.Size, ref.SHA256, MaxMetadataBytes)
		if err != nil {
			return 0, fmt.Errorf("backup: could not restore the extent map %s (%w); run 'siloctl backup verify --deep' to check the backup, or restore an older one", key, err)
		}
		var eb extentBackup
		if err := json.Unmarshal(raw, &eb); err != nil {
			return 0, fmt.Errorf("backup: the extent map %s is damaged (%w); restore an older backup", key, err)
		}
		// The object name encodes the volume id; a mismatch means the object
		// was not written by this run.
		if eb.VolumeID == "" || path.Base(key) != base64.RawURLEncoding.EncodeToString([]byte(eb.VolumeID))+".json" {
			return 0, fmt.Errorf("backup: the extent map %s does not match the volume it names; restore an older backup", key)
		}
		if err := store.Merge(eb.VolumeID, eb.Entries); err != nil {
			return 0, fmt.Errorf("backup: could not write the extent map of volume %s into %s (%w); check free space", eb.VolumeID, dir, err)
		}
	}
	return len(m.Extents), nil
}

// restoreChunks downloads every chunk the manifest lists in parallel.
func restoreChunks(ctx context.Context, target blobstore.Target, m *Manifest, dir string, opts RestoreOptions) (RestoreResult, error) {
	var (
		mu  sync.Mutex
		res RestoreResult
	)
	refs := chunkRefs(m)
	total := len(m.Chunks)
	err := forEach(ctx, opts.Concurrency, chunkIDs(m), func(ctx context.Context, id string) error {
		ref := refs[id]
		data, err := fetchVerified(ctx, target, chunkKey(id), ref.Size, ref.SHA256, MaxChunkBytes)
		if err != nil {
			return fmt.Errorf("backup: could not restore chunk %s (%w); run 'siloctl backup verify --deep' to check the backup, or restore an older one", id, err)
		}
		if err := atomicfile.Write(chunkstore.FilePath(dir, id), data, 0o600); err != nil {
			return fmt.Errorf("backup: could not write chunk %s into %s (%w); check free space", id, dir, err)
		}
		mu.Lock()
		defer mu.Unlock()
		res.Chunks++
		res.Bytes += int64(len(data))
		if opts.Progress != nil {
			opts.Progress(res.Chunks, total)
		}
		return nil
	})
	if err != nil {
		return res, fmt.Errorf("%w; the restore stopped part-way, delete %s before trying again", err, dir)
	}
	return res, nil
}

func chunkIDs(m *Manifest) []string {
	ids := make([]string, len(m.Chunks))
	for i, c := range m.Chunks {
		ids[i] = c.ID
	}
	return ids
}

func chunkRefs(m *Manifest) map[string]ChunkRef {
	out := make(map[string]ChunkRef, len(m.Chunks))
	for _, c := range m.Chunks {
		out[c.ID] = c
	}
	return out
}

// VerifyOptions tunes Verify.
type VerifyOptions struct {
	// Deep downloads every object and checks its SHA-256. Without it only
	// presence and size are checked, which costs a few listings.
	Deep bool
	// Concurrency bounds parallel downloads for Deep; values below one use
	// DefaultConcurrency.
	Concurrency int
}

// Problem is one object that would make a restore fail.
type Problem struct {
	Name   string
	Reason string
}

// Verify checks every object m references and returns the problems found,
// sorted by name. By default it compares the target's listing with the sizes
// the manifest recorded; with Deep it also downloads each object and compares
// its SHA-256.
func Verify(ctx context.Context, target blobstore.Target, m *Manifest, opts VerifyOptions) ([]Problem, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if opts.Concurrency < 1 {
		opts.Concurrency = DefaultConcurrency
	}
	sizes, err := listSizes(ctx, target, chunksPrefix, namespaceDir+"/"+m.NodeID+"/", extentsDir+"/"+m.NodeID+"/"+m.Stamp()+"/")
	if err != nil {
		return nil, err
	}
	type want struct {
		size int64
		sum  string
		max  int64
	}
	wanted := map[string]want{m.Namespace.Key: {m.Namespace.Size, m.Namespace.SHA256, MaxMetadataBytes}}
	for _, e := range m.Extents {
		wanted[e.Key] = want{e.Size, e.SHA256, MaxMetadataBytes}
	}
	for _, c := range m.Chunks {
		wanted[chunkKey(c.ID)] = want{c.Size, c.SHA256, MaxChunkBytes}
	}

	var (
		mu       sync.Mutex
		problems []Problem
		deep     []string
	)
	for key, w := range wanted {
		got, ok := sizes[key]
		switch {
		case !ok:
			problems = append(problems, Problem{key, "missing"})
		case got != w.size:
			problems = append(problems, Problem{key, fmt.Sprintf("size is %d bytes, the backup recorded %d", got, w.size)})
		case opts.Deep:
			deep = append(deep, key)
		}
	}
	sort.Strings(deep)
	err = forEach(ctx, opts.Concurrency, deep, func(ctx context.Context, key string) error {
		w := wanted[key]
		_, err := fetchVerified(ctx, target, key, w.size, w.sum, w.max)
		if err == nil {
			return nil
		}
		reason := ""
		switch {
		case errors.Is(err, ErrChecksumMismatch):
			reason = "content differs from the backup (checksum mismatch)"
		case errors.Is(err, blobstore.ErrNotFound):
			reason = "missing"
		case errors.Is(err, blobstore.ErrTooLarge):
			reason = "larger than silo ever writes"
		default:
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		problems = append(problems, Problem{key, reason})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(problems, func(i, j int) bool { return problems[i].Name < problems[j].Name })
	return problems, nil
}

// listSizes maps every object name under each prefix to its size.
func listSizes(ctx context.Context, target blobstore.Target, prefixes ...string) (map[string]int64, error) {
	out := map[string]int64{}
	for _, p := range prefixes {
		objs, err := target.List(ctx, p)
		if err != nil {
			return nil, fmt.Errorf("backup: could not list %s on %s (%w)", p, target.Name(), err)
		}
		for _, o := range objs {
			out[o.Name] = o.Size
		}
	}
	return out, nil
}
