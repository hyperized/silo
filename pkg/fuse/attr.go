package fuse

// File type bits (from <sys/stat.h> S_IF*), used in Attr.Mode and dirent types.
const (
	ModeMask = 0o170000
	ModeDir  = 0o040000
	ModeReg  = 0o100000
	ModeLink = 0o120000
)

// Attr is a file's metadata, the friendly form of fuse_attr. Times are seconds
// since the epoch with a nanosecond remainder; Mode includes the S_IF* type
// bits OR'd with the permission bits.
type Attr struct {
	Ino    uint64
	Size   uint64
	Blocks uint64
	Atime  uint64
	Mtime  uint64
	Ctime  uint64
	Mode   uint32
	Nlink  uint32
	UID    uint32
	GID    uint32
	Rdev   uint32
}

// attrSize is the wire size of fuse_attr: 88 bytes.
const attrSize = 88

// appendAttr writes a fuse_attr onto e.
func (e *le) attr(a Attr) *le {
	return e.u64(a.Ino).u64(a.Size).u64(a.Blocks).
		u64(a.Atime).u64(a.Mtime).u64(a.Ctime).
		u32(0).u32(0).u32(0). // atimensec, mtimensec, ctimensec
		u32(a.Mode).u32(a.Nlink).u32(a.UID).u32(a.GID).
		u32(a.Rdev).u32(blksize).u32(0) // blksize, padding
}

const blksize = 4096

// EntryOut encodes a fuse_entry_out reply (LOOKUP / CREATE / MKDIR): the new
// node id and its attributes. The validity timeouts are left at zero — silo is
// close-to-open coherent, so the kernel revalidates on each open.
func EntryOut(nodeID uint64, a Attr) []byte {
	return newLE(128).
		u64(nodeID).u64(0). // nodeid, generation
		u64(0).u64(0).      // entry_valid, attr_valid (seconds)
		u32(0).u32(0).      // entry_valid_nsec, attr_valid_nsec
		attr(a).
		bytes()
}

// AttrOut encodes a fuse_attr_out reply (GETATTR / SETATTR).
func AttrOut(a Attr) []byte {
	return newLE(attrSize + 16).
		u64(0).u32(0).u32(0). // attr_valid, attr_valid_nsec, dummy
		attr(a).
		bytes()
}

// OpenOut encodes a fuse_open_out reply (OPEN / OPENDIR / CREATE). fh is the
// file handle the kernel echoes on subsequent read/write/release.
func OpenOut(fh uint64, flags uint32) []byte {
	return newLE(16).u64(fh).u32(flags).u32(0).bytes()
}

// WriteOut encodes a fuse_write_out reply: how many bytes were written.
func WriteOut(n uint32) []byte {
	return newLE(8).u32(n).u32(0).bytes()
}

// CreateOut encodes the fuse_entry_out + fuse_open_out pair a CREATE reply
// carries (the kernel both looks up and opens the new file).
func CreateOut(nodeID uint64, a Attr, fh uint64, openFlags uint32) []byte {
	out := EntryOut(nodeID, a)
	return append(out, OpenOut(fh, openFlags)...)
}
