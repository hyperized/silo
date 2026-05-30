package silofuse

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hyperized/silo/pkg/fuse"
)

// fakeBackend is an in-memory path-keyed filesystem implementing Backend.
type fakeBackend struct {
	dirs  map[string]bool
	files map[string][]byte
	fail  map[string]error // path -> error to return from any op touching it
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{dirs: map[string]bool{"/": true}, files: map[string][]byte{}, fail: map[string]error{}}
}

func (b *fakeBackend) errFor(path string) error { return b.fail[path] }

func (b *fakeBackend) Mkdir(_ context.Context, path string) error {
	if err := b.errFor(path); err != nil {
		return err
	}
	if b.dirs[path] || b.files[path] != nil {
		return ErrExists
	}
	b.dirs[path] = true
	return nil
}

func (b *fakeBackend) Touch(_ context.Context, path string) error {
	if err := b.errFor(path); err != nil {
		return err
	}
	if _, ok := b.files[path]; !ok {
		b.files[path] = []byte{}
	}
	return nil
}

func (b *fakeBackend) Remove(_ context.Context, path string) error {
	if err := b.errFor(path); err != nil {
		return err
	}
	if !b.dirs[path] {
		if _, ok := b.files[path]; !ok {
			return ErrNotFound
		}
	}
	delete(b.dirs, path)
	delete(b.files, path)
	return nil
}

func (b *fakeBackend) List(_ context.Context, path string) ([]DirItem, error) {
	if err := b.errFor(path); err != nil {
		return nil, err
	}
	prefix := path
	if prefix != "/" {
		prefix += "/"
	}
	var out []DirItem
	seen := map[string]bool{}
	add := func(full string, isDir bool) {
		if !strHasPrefix(full, prefix) || full == path {
			return
		}
		rest := full[len(prefix):]
		if rest == "" || containsSlash(rest) || seen[rest] {
			return
		}
		seen[rest] = true
		out = append(out, DirItem{Name: rest, IsDir: isDir})
	}
	for d := range b.dirs {
		add(d, true)
	}
	for f := range b.files {
		add(f, false)
	}
	return out, nil
}

func (b *fakeBackend) ReadFile(_ context.Context, path string) ([]byte, error) {
	if err := b.errFor(path); err != nil {
		return nil, err
	}
	data, ok := b.files[path]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

func (b *fakeBackend) WriteFile(_ context.Context, path string, data []byte) error {
	if err := b.errFor(path); err != nil {
		return err
	}
	b.files[path] = append([]byte(nil), data...)
	return nil
}

func (b *fakeBackend) FileSize(_ context.Context, path string) (int64, error) {
	if err := b.errFor(path); err != nil {
		return 0, err
	}
	d, ok := b.files[path]
	if !ok {
		return 0, ErrNotFound
	}
	return int64(len(d)), nil
}

func strHasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }
func containsSlash(s string) bool {
	for i := range s {
		if s[i] == '/' {
			return true
		}
	}
	return false
}

func newFS(t *testing.T) (*SiloFS, *fakeBackend) {
	t.Helper()
	b := newFakeBackend()
	return New(context.Background(), b), b
}

func TestSiloFS_FileLifecycle(t *testing.T) {
	fs, _ := newFS(t)
	root := fuse.RootNodeID

	// Create a file, write to it, commit on release, read it back.
	id, _, fh, errno := fs.Create(root, "hello.txt", 0, 0o644)
	if errno != fuse.OK {
		t.Fatalf("Create errno = %d", errno)
	}
	if n, e := fs.Write(id, fh, 0, []byte("hello world")); e != fuse.OK || n != 11 {
		t.Fatalf("Write = (%d, %d)", n, e)
	}
	// Getattr reflects the dirty in-memory size before commit.
	if a, e := fs.Getattr(id); e != fuse.OK || a.Size != 11 {
		t.Fatalf("dirty getattr size = %d (errno %d), want 11", a.Size, e)
	}
	if e := fs.Flush(id, fh); e != fuse.OK {
		t.Fatalf("Flush errno = %d", e)
	}
	if e := fs.Release(id, fh); e != fuse.OK {
		t.Fatalf("Release errno = %d", e)
	}

	// Lookup + Open + Read the committed content.
	lid, attr, e := fs.Lookup(root, "hello.txt")
	if e != fuse.OK || lid != id || attr.Size != 11 {
		t.Fatalf("Lookup = (%d, size %d, errno %d)", lid, attr.Size, e)
	}
	rfh, e := fs.Open(id, 0)
	if e != fuse.OK {
		t.Fatalf("Open errno = %d", e)
	}
	data, e := fs.Read(id, rfh, 0, 100)
	if e != fuse.OK || string(data) != "hello world" {
		t.Fatalf("Read = (%q, %d)", data, e)
	}
	if d, _ := fs.Read(id, rfh, 100, 10); len(d) != 0 {
		t.Errorf("read past EOF = %q", d)
	}
	_ = fs.Release(id, rfh)
}

func TestSiloFS_DirLifecycle(t *testing.T) {
	fs, _ := newFS(t)
	root := fuse.RootNodeID

	did, attr, errno := fs.Mkdir(root, "sub", 0o755)
	if errno != fuse.OK || attr.Mode&fuse.ModeDir == 0 {
		t.Fatalf("Mkdir = (%d, mode %o)", errno, attr.Mode)
	}
	if a, e := fs.Getattr(did); e != fuse.OK || a.Mode&fuse.ModeDir == 0 {
		t.Errorf("dir getattr mode = %o", a.Mode)
	}
	if _, e := fs.Opendir(did, 0); e != fuse.OK {
		t.Errorf("Opendir errno = %d", e)
	}

	// A file inside the subdir, then readdir the subdir and root.
	fid, _, fh, _ := fs.Create(did, "f", 0, 0o644)
	_ = fs.Release(fid, fh)
	ents, e := fs.ReadDir(did)
	if e != fuse.OK || len(ents) != 1 || ents[0].Name != "f" {
		t.Fatalf("readdir sub = (%v, %d)", ents, e)
	}
	rootEnts, _ := fs.ReadDir(root)
	if len(rootEnts) != 1 || rootEnts[0].Name != "sub" || rootEnts[0].Mode&fuse.ModeDir == 0 {
		t.Errorf("readdir root = %v", rootEnts)
	}

	// Remove the file then the now-empty dir.
	if e := fs.Unlink(did, "f"); e != fuse.OK {
		t.Errorf("Unlink errno = %d", e)
	}
	if e := fs.Rmdir(root, "sub"); e != fuse.OK {
		t.Errorf("Rmdir errno = %d", e)
	}
	if _, _, e := fs.Lookup(root, "sub"); e != fuse.ENOENT {
		t.Errorf("lookup removed dir errno = %d, want ENOENT", e)
	}
}

func TestSiloFS_SetattrTruncate(t *testing.T) {
	fs, _ := newFS(t)
	id, _, fh, _ := fs.Create(fuse.RootNodeID, "f", 0, 0o644)
	_, _ = fs.Write(id, fh, 0, []byte("hello world"))

	attr, errno := fs.Setattr(id, fuse.SetattrIn{Valid: fuse.SetattrSize, Size: 5})
	if errno != fuse.OK || attr.Size != 5 {
		t.Fatalf("truncate = (size %d, errno %d), want 5", attr.Size, errno)
	}
	if d, _ := fs.Read(id, fh, 0, 100); string(d) != "hello" {
		t.Errorf("after truncate read = %q, want hello", d)
	}
}

func TestSiloFS_Errors(t *testing.T) {
	fs, b := newFS(t)
	root := fuse.RootNodeID

	// Missing parent / node ids.
	if _, _, e := fs.Lookup(999, "x"); e != fuse.ENOENT {
		t.Errorf("lookup bad parent = %d", e)
	}
	if _, e := fs.Getattr(999); e != fuse.ENOENT {
		t.Errorf("getattr missing = %d", e)
	}
	if _, _, _, e := fs.Create(999, "x", 0, 0); e != fuse.ENOTDIR {
		t.Errorf("create under non-dir = %d", e)
	}
	if _, e := fs.Open(999, 0); e != fuse.ENOENT {
		t.Errorf("open missing = %d", e)
	}
	if _, e := fs.Opendir(999, 0); e != fuse.ENOENT {
		t.Errorf("opendir missing = %d", e)
	}
	if _, e := fs.ReadDir(999); e != fuse.ENOENT {
		t.Errorf("readdir missing = %d", e)
	}
	if _, e := fs.Setattr(999, fuse.SetattrIn{}); e != fuse.ENOENT {
		t.Errorf("setattr missing = %d", e)
	}
	if _, e := fs.Read(0, 999, 0, 1); e != fuse.EINVAL {
		t.Errorf("read bad fh = %d", e)
	}
	if _, e := fs.Write(0, 999, 0, []byte("x")); e != fuse.EINVAL {
		t.Errorf("write bad fh = %d", e)
	}
	if e := fs.Flush(0, 999); e != fuse.OK {
		t.Errorf("flush unknown fh should be a no-op OK, got %d", e)
	}

	// Backend failures map through.
	b.fail["/boom"] = status.Error(codes.AlreadyExists, "exists")
	if _, _, e := fs.Mkdir(root, "boom", 0o755); e != fuse.EEXIST {
		t.Errorf("mkdir EEXIST mapping = %d", e)
	}
	b.fail["/io"] = errors.New("disk on fire")
	if _, _, _, e := fs.Create(root, "io", 0, 0o644); e != fuse.EIO {
		t.Errorf("create EIO mapping = %d", e)
	}
}

func TestSiloFS_BackendErrorPaths(t *testing.T) {
	fs, b := newFS(t)
	root := fuse.RootNodeID
	boom := errors.New("backend down")

	// Lookup when listing the parent fails.
	b.fail["/"] = boom
	if _, _, e := fs.Lookup(root, "x"); e != fuse.EIO {
		t.Errorf("lookup with failing list = %d, want EIO", e)
	}
	if _, e := fs.ReadDir(root); e != fuse.EIO {
		t.Errorf("readdir with failing list = %d, want EIO", e)
	}
	delete(b.fail, "/")

	// A file that lists but whose FileSize fails.
	fid, _, fh, _ := fs.Create(root, "f", 0, 0o644)
	_ = fs.Release(fid, fh)
	b.fail["/f"] = boom
	if _, _, e := fs.Lookup(root, "f"); e != fuse.EIO {
		t.Errorf("lookup with failing filesize = %d, want EIO", e)
	}
	if _, e := fs.Open(fid, 0); e != fuse.EIO {
		t.Errorf("open with failing read = %d, want EIO", e)
	}
	delete(b.fail, "/f")

	// Mkdir / Unlink / Open under a file node, and an unopened path.
	if _, _, e := fs.Mkdir(fid, "x", 0o755); e != fuse.ENOTDIR {
		t.Errorf("mkdir under a file = %d, want ENOTDIR", e)
	}
	if e := fs.Unlink(fid, "x"); e != fuse.ENOTDIR {
		t.Errorf("unlink under a file = %d, want ENOTDIR", e)
	}
	if _, e := fs.Opendir(fid, 0); e != fuse.ENOTDIR {
		t.Errorf("opendir on a file = %d, want ENOTDIR", e)
	}

	// Remove failing in the backend.
	b.fail["/f"] = boom
	if e := fs.Unlink(root, "f"); e != fuse.EIO {
		t.Errorf("unlink with failing remove = %d, want EIO", e)
	}
	delete(b.fail, "/f")

	// Commit failure: a dirty handle whose WriteFile errors on release.
	wid, _, wfh, _ := fs.Create(root, "w", 0, 0o644)
	_, _ = fs.Write(wid, wfh, 0, []byte("data"))
	b.fail["/w"] = boom
	if e := fs.Release(wid, wfh); e != fuse.EIO {
		t.Errorf("release with failing write = %d, want EIO", e)
	}
}

func TestMapErr(t *testing.T) {
	cases := map[error]fuse.Errno{
		nil:                                      fuse.OK,
		ErrNotFound:                              fuse.ENOENT,
		ErrExists:                                fuse.EEXIST,
		status.Error(codes.NotFound, "x"):        fuse.ENOENT,
		status.Error(codes.AlreadyExists, "x"):   fuse.EEXIST,
		status.Error(codes.InvalidArgument, "x"): fuse.EINVAL,
		errors.New("other"):                      fuse.EIO,
	}
	for err, want := range cases {
		if got := mapErr(err); got != want {
			t.Errorf("mapErr(%v) = %d, want %d", err, got, want)
		}
	}
}
