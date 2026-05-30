package fuse

// Errno is a POSIX error number returned by Filesystem operations. Zero (OK)
// means success; the session negates it into the fuse_out_header error field
// the kernel expects.
type Errno int32

// The errno values the core opcodes use (from <errno.h>).
const (
	OK        Errno = 0
	EPERM     Errno = 1
	ENOENT    Errno = 2
	EIO       Errno = 5
	EEXIST    Errno = 17
	ENOTDIR   Errno = 20
	EISDIR    Errno = 21
	EINVAL    Errno = 22
	ENOSPC    Errno = 28
	ENOSYS    Errno = 38
	ENOTEMPTY Errno = 39
)

// RootNodeID is the inode number the kernel uses for the mount root. Every
// Filesystem must resolve lookups under it.
const RootNodeID uint64 = 1

// Filesystem is the backend a Session drives. Methods map one-to-one onto the
// core FUSE opcodes; an implementation returns an Errno (OK on success). Node
// ids are the inode numbers the kernel tracks (RootNodeID for the mount root);
// file handles are opaque values the implementation chooses at Open/Create time
// and the kernel echoes back on read/write/release.
//
// Implementations need not be safe for concurrent use unless they negotiate it:
// a Session dispatches requests one at a time by default.
type Filesystem interface {
	// Lookup resolves name within the parent directory.
	Lookup(parent uint64, name string) (nodeID uint64, attr Attr, errno Errno)
	// Forget drops nlookup references the kernel previously took on a node.
	Forget(nodeID uint64, nlookup uint64)
	// Getattr returns a node's attributes.
	Getattr(nodeID uint64) (Attr, Errno)
	// Setattr applies a truncation and/or chmod and returns the new attributes.
	Setattr(nodeID uint64, in SetattrIn) (Attr, Errno)

	// Mkdir creates a directory; Rmdir removes an empty one.
	Mkdir(parent uint64, name string, mode uint32) (nodeID uint64, attr Attr, errno Errno)
	Rmdir(parent uint64, name string) Errno
	// Unlink removes a file.
	Unlink(parent uint64, name string) Errno

	// Create makes and opens a new file, returning its node id and a handle.
	Create(parent uint64, name string, flags, mode uint32) (nodeID uint64, attr Attr, fh uint64, errno Errno)
	// Open opens an existing file, returning a handle.
	Open(nodeID uint64, flags uint32) (fh uint64, errno Errno)
	// Read returns up to size bytes at offset; a short read signals EOF.
	Read(nodeID, fh, offset uint64, size uint32) ([]byte, Errno)
	// Write stores data at offset and returns how many bytes were written.
	Write(nodeID, fh, offset uint64, data []byte) (uint32, Errno)
	// Flush is called on close(2); Release when the last handle is dropped.
	Flush(nodeID, fh uint64) Errno
	Release(nodeID, fh uint64) Errno

	// Opendir opens a directory for reading; ReadDir lists its children.
	Opendir(nodeID uint64, flags uint32) (fh uint64, errno Errno)
	ReadDir(nodeID uint64) ([]DirEntry, Errno)
}
