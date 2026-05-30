// Package silofuse bridges silo's distributed store to the pkg/fuse protocol
// library: SiloFS implements fuse.Filesystem on top of the CRDT namespace and
// the writer/reader SDKs, so a silo cluster can be mounted as an ordinary
// directory tree. Coherence is close-to-open (NFS-style): a file's bytes are
// loaded into memory on open and written back as silo chunks on close, which
// matches silo's writer-owned-chunk model and keeps the hot path coordination-
// free.
package silofuse

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hyperized/silo/pkg/fuse"
)

// DirItem is one child in a directory listing from the Backend.
type DirItem struct {
	Name  string
	IsDir bool
}

// ErrNotFound and ErrExists let a Backend signal the common cases without a
// gRPC status; SiloFS also recognises the equivalent gRPC codes.
var (
	ErrNotFound = errors.New("silofuse: not found")
	ErrExists   = errors.New("silofuse: already exists")
)

// Backend is the slice of silo SiloFS drives, in path terms. The gRPC-backed
// implementation (NewGRPCBackend) wraps the namespace client and the
// writer/reader SDKs; tests supply an in-memory fake. Every method is given an
// absolute slash path ("/", "/a/b").
type Backend interface {
	Mkdir(ctx context.Context, path string) error
	Touch(ctx context.Context, path string) error
	Remove(ctx context.Context, path string) error
	List(ctx context.Context, path string) ([]DirItem, error)
	ReadFile(ctx context.Context, path string) ([]byte, error)
	WriteFile(ctx context.Context, path string, data []byte) error
	FileSize(ctx context.Context, path string) (int64, error)
}

// SiloFS implements fuse.Filesystem over a Backend. It keeps the inode table the
// kernel works in (uint64 node ids) mapped to silo paths, and an open-file table
// holding each handle's in-memory buffer until close.
type SiloFS struct {
	ctx     context.Context //nolint:containedctx // bound to the mount session so SiloFS can satisfy fuse.Filesystem
	backend Backend

	mu       sync.Mutex
	byNode   map[uint64]*inode
	byPath   map[string]uint64
	nextNode uint64

	nextFh uint64
	open   map[uint64]*handle
}

type inode struct {
	path  string
	isDir bool
}

type handle struct {
	path  string
	buf   []byte
	dirty bool
}

// New builds a SiloFS over backend. ctx governs every backend call (bind it to
// the mount's lifetime).
func New(ctx context.Context, backend Backend) *SiloFS {
	fs := &SiloFS{
		ctx:      ctx,
		backend:  backend,
		byNode:   map[uint64]*inode{fuse.RootNodeID: {path: "/", isDir: true}},
		byPath:   map[string]uint64{"/": fuse.RootNodeID},
		nextNode: fuse.RootNodeID + 1,
		nextFh:   1,
		open:     map[uint64]*handle{},
	}
	return fs
}

// childPath joins a parent path and a child name into an absolute path.
func childPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

// intern returns the node id for path (allocating one on first sight) and keeps
// the bidirectional map current. Must hold mu.
func (fs *SiloFS) intern(path string, isDir bool) uint64 {
	if id, ok := fs.byPath[path]; ok {
		fs.byNode[id].isDir = isDir
		return id
	}
	id := fs.nextNode
	fs.nextNode++
	fs.byNode[id] = &inode{path: path, isDir: isDir}
	fs.byPath[path] = id
	return id
}

// forget drops a node from the table (after unlink/rmdir). Must hold mu.
func (fs *SiloFS) forget(path string) {
	if id, ok := fs.byPath[path]; ok {
		delete(fs.byNode, id)
		delete(fs.byPath, path)
	}
}

func dirAttr(ino uint64) fuse.Attr {
	return fuse.Attr{Ino: ino, Mode: fuse.ModeDir | 0o755, Nlink: 2}
}

func fileAttr(ino uint64, size uint64) fuse.Attr {
	return fuse.Attr{Ino: ino, Mode: fuse.ModeReg | 0o644, Nlink: 1, Size: size, Blocks: (size + 511) / 512}
}

// Lookup resolves name in the parent directory by listing the parent and
// matching the child.
func (fs *SiloFS) Lookup(parent uint64, name string) (uint64, fuse.Attr, fuse.Errno) {
	fs.mu.Lock()
	pnode, ok := fs.byNode[parent]
	fs.mu.Unlock()
	if !ok {
		return 0, fuse.Attr{}, fuse.ENOENT
	}
	items, err := fs.backend.List(fs.ctx, pnode.path)
	if err != nil {
		return 0, fuse.Attr{}, mapErr(err)
	}
	for _, it := range items {
		if it.Name != name {
			continue
		}
		path := childPath(pnode.path, name)
		fs.mu.Lock()
		id := fs.intern(path, it.IsDir)
		fs.mu.Unlock()
		attr, errno := fs.attrFor(id, path, it.IsDir)
		return id, attr, errno
	}
	return 0, fuse.Attr{}, fuse.ENOENT
}

// attrFor builds the attributes for a node, fetching a file's size from the
// backend.
func (fs *SiloFS) attrFor(ino uint64, path string, isDir bool) (fuse.Attr, fuse.Errno) {
	if isDir {
		return dirAttr(ino), fuse.OK
	}
	size, err := fs.backend.FileSize(fs.ctx, path)
	if err != nil {
		return fuse.Attr{}, mapErr(err)
	}
	return fileAttr(ino, uint64(size)), fuse.OK //nolint:gosec // file size is non-negative
}

// Forget drops the kernel's references to a node; SiloFS holds no per-node
// state, so it is a no-op.
func (fs *SiloFS) Forget(uint64, uint64) {}

// Getattr returns a node's attributes.
func (fs *SiloFS) Getattr(nodeID uint64) (fuse.Attr, fuse.Errno) {
	fs.mu.Lock()
	n, ok := fs.byNode[nodeID]
	fs.mu.Unlock()
	if !ok {
		return fuse.Attr{}, fuse.ENOENT
	}
	// A dirty open handle has the authoritative size in memory.
	if size, dirty := fs.openSize(n.path); dirty {
		return fileAttr(nodeID, uint64(size)), fuse.OK //nolint:gosec // buffer length is non-negative
	}
	return fs.attrFor(nodeID, n.path, n.isDir)
}

// openSize reports the in-memory size of a dirty handle for path, if any.
func (fs *SiloFS) openSize(path string) (int, bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for _, h := range fs.open {
		if h.path == path && h.dirty {
			return len(h.buf), true
		}
	}
	return 0, false
}

// Setattr supports truncation of an open file (the SETATTR the kernel sends for
// truncate(2)/ftruncate(2)); other changes are accepted as no-ops.
func (fs *SiloFS) Setattr(nodeID uint64, in fuse.SetattrIn) (fuse.Attr, fuse.Errno) {
	fs.mu.Lock()
	n, ok := fs.byNode[nodeID]
	if !ok {
		fs.mu.Unlock()
		return fuse.Attr{}, fuse.ENOENT
	}
	if in.Valid&fuse.SetattrSize != 0 {
		for _, h := range fs.open {
			if h.path == n.path {
				h.buf = resize(h.buf, int(in.Size)) //nolint:gosec // truncate size is bounded by the file
				h.dirty = true
			}
		}
	}
	fs.mu.Unlock()
	// Getattr reflects a truncated open handle's new in-memory size.
	return fs.Getattr(nodeID)
}

// Mkdir creates a directory.
func (fs *SiloFS) Mkdir(parent uint64, name string, _ uint32) (uint64, fuse.Attr, fuse.Errno) {
	path, errno := fs.parentChild(parent, name)
	if errno != fuse.OK {
		return 0, fuse.Attr{}, errno
	}
	if err := fs.backend.Mkdir(fs.ctx, path); err != nil {
		return 0, fuse.Attr{}, mapErr(err)
	}
	fs.mu.Lock()
	id := fs.intern(path, true)
	fs.mu.Unlock()
	return id, dirAttr(id), fuse.OK
}

// Create makes an empty file and opens it for writing.
func (fs *SiloFS) Create(parent uint64, name string, _ uint32, _ uint32) (uint64, fuse.Attr, uint64, fuse.Errno) {
	path, errno := fs.parentChild(parent, name)
	if errno != fuse.OK {
		return 0, fuse.Attr{}, 0, errno
	}
	if err := fs.backend.Touch(fs.ctx, path); err != nil {
		return 0, fuse.Attr{}, 0, mapErr(err)
	}
	fs.mu.Lock()
	id := fs.intern(path, false)
	fh := fs.newHandle(path, nil)
	fs.mu.Unlock()
	return id, fileAttr(id, 0), fh, fuse.OK
}

// parentChild resolves a (parent node, name) into a child path.
func (fs *SiloFS) parentChild(parent uint64, name string) (string, fuse.Errno) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	pnode, ok := fs.byNode[parent]
	if !ok || !pnode.isDir {
		return "", fuse.ENOTDIR
	}
	return childPath(pnode.path, name), fuse.OK
}

// newHandle registers an open file handle. Must hold mu.
func (fs *SiloFS) newHandle(path string, buf []byte) uint64 {
	fh := fs.nextFh
	fs.nextFh++
	fs.open[fh] = &handle{path: path, buf: buf}
	return fh
}

// Rmdir removes an empty directory.
func (fs *SiloFS) Rmdir(parent uint64, name string) fuse.Errno { return fs.remove(parent, name) }

// Unlink removes a file.
func (fs *SiloFS) Unlink(parent uint64, name string) fuse.Errno { return fs.remove(parent, name) }

func (fs *SiloFS) remove(parent uint64, name string) fuse.Errno {
	path, errno := fs.parentChild(parent, name)
	if errno != fuse.OK {
		return errno
	}
	if err := fs.backend.Remove(fs.ctx, path); err != nil {
		return mapErr(err)
	}
	fs.mu.Lock()
	fs.forget(path)
	fs.mu.Unlock()
	return fuse.OK
}

// Open loads the file's current contents into a handle buffer (close-to-open).
func (fs *SiloFS) Open(nodeID uint64, _ uint32) (uint64, fuse.Errno) {
	fs.mu.Lock()
	n, ok := fs.byNode[nodeID]
	fs.mu.Unlock()
	if !ok {
		return 0, fuse.ENOENT
	}
	data, err := fs.backend.ReadFile(fs.ctx, n.path)
	if err != nil {
		return 0, mapErr(err)
	}
	fs.mu.Lock()
	fh := fs.newHandle(n.path, data)
	fs.mu.Unlock()
	return fh, fuse.OK
}

// Opendir validates that the node is a directory.
func (fs *SiloFS) Opendir(nodeID uint64, _ uint32) (uint64, fuse.Errno) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, ok := fs.byNode[nodeID]
	if !ok {
		return 0, fuse.ENOENT
	}
	if !n.isDir {
		return 0, fuse.ENOTDIR
	}
	return nodeID, fuse.OK
}

// Read serves bytes from the handle's in-memory buffer.
func (fs *SiloFS) Read(_, fh, offset uint64, size uint32) ([]byte, fuse.Errno) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	h, ok := fs.open[fh]
	if !ok {
		return nil, fuse.EINVAL
	}
	if offset >= uint64(len(h.buf)) {
		return nil, fuse.OK
	}
	end := offset + uint64(size)
	if end > uint64(len(h.buf)) {
		end = uint64(len(h.buf))
	}
	out := make([]byte, end-offset)
	copy(out, h.buf[offset:end])
	return out, fuse.OK
}

// Write stores bytes into the handle's buffer; the file is committed on close.
func (fs *SiloFS) Write(_, fh, offset uint64, data []byte) (uint32, fuse.Errno) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	h, ok := fs.open[fh]
	if !ok {
		return 0, fuse.EINVAL //nolint:gosec // size/offset from the request is non-negative
	}
	end := int(offset) + len(data) //nolint:gosec // write offset is bounded by the request
	if end > len(h.buf) {
		h.buf = resize(h.buf, end)
	}
	copy(h.buf[offset:], data)
	h.dirty = true
	return uint32(len(data)), fuse.OK //nolint:gosec // write length is bounded by the request
}

// Flush commits a dirty handle (called on close(2)) without dropping it.
func (fs *SiloFS) Flush(_, fh uint64) fuse.Errno {
	return fs.commit(fh, false)
}

// Release commits a dirty handle and drops it (last close).
func (fs *SiloFS) Release(_, fh uint64) fuse.Errno {
	return fs.commit(fh, true)
}

// commit writes a dirty handle's buffer back to silo. With drop, the handle is
// also removed from the open table.
func (fs *SiloFS) commit(fh uint64, drop bool) fuse.Errno {
	fs.mu.Lock()
	h, ok := fs.open[fh]
	if !ok {
		fs.mu.Unlock()
		return fuse.OK // already released; nothing to do
	}
	path, buf, dirty := h.path, h.buf, h.dirty
	if drop {
		delete(fs.open, fh)
	} else {
		h.dirty = false
	}
	fs.mu.Unlock()

	if !dirty {
		return fuse.OK
	}
	if err := fs.backend.WriteFile(fs.ctx, path, buf); err != nil {
		return mapErr(err)
	}
	return fuse.OK
}

// ReadDir lists a directory's children.
func (fs *SiloFS) ReadDir(nodeID uint64) ([]fuse.DirEntry, fuse.Errno) {
	fs.mu.Lock()
	n, ok := fs.byNode[nodeID]
	fs.mu.Unlock()
	if !ok {
		return nil, fuse.ENOENT
	}
	items, err := fs.backend.List(fs.ctx, n.path)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]fuse.DirEntry, 0, len(items))
	for _, it := range items {
		path := childPath(n.path, it.Name)
		fs.mu.Lock()
		id := fs.intern(path, it.IsDir)
		fs.mu.Unlock()
		mode := uint32(fuse.ModeReg)
		if it.IsDir {
			mode = fuse.ModeDir
		}
		out = append(out, fuse.DirEntry{Ino: id, Name: it.Name, Mode: mode})
	}
	return out, fuse.OK
}

// mapErr translates a backend error into a FUSE errno, recognising both the
// package sentinels and the equivalent gRPC status codes from silod.
func mapErr(err error) fuse.Errno {
	switch {
	case err == nil:
		return fuse.OK
	case errors.Is(err, ErrNotFound), status.Code(err) == codes.NotFound:
		return fuse.ENOENT
	case errors.Is(err, ErrExists), status.Code(err) == codes.AlreadyExists:
		return fuse.EEXIST
	case status.Code(err) == codes.InvalidArgument:
		return fuse.EINVAL
	default:
		return fuse.EIO
	}
}

// resize grows or shrinks a byte slice to n, zero-filling growth.
func resize(b []byte, n int) []byte {
	if n <= len(b) {
		return b[:n]
	}
	return append(b, make([]byte, n-len(b))...)
}

// Compile-time check that SiloFS satisfies the FUSE backend contract.
var _ fuse.Filesystem = (*SiloFS)(nil)
