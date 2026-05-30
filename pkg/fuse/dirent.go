package fuse

// DirEntry is one child in a directory listing, the friendly form of
// fuse_dirent. Mode carries the S_IF* type bits so the kernel can fill d_type.
type DirEntry struct {
	Ino  uint64
	Name string
	Mode uint32
}

// direntHeader is the fixed part of fuse_dirent before the name: ino(8) off(8)
// namelen(4) type(4).
const direntHeader = 24

// packDirents serialises entries[offset:] into the fuse_dirent stream READDIR
// returns, stopping before it would exceed maxSize. Each entry's offset is its
// 1-based index in the full listing, so the kernel resumes by passing the last
// offset it consumed. The kernel requires every record padded to an 8-byte
// boundary.
func packDirents(entries []DirEntry, offset uint64, maxSize int) []byte {
	e := newLE(maxSize)
	for i := int(offset); i < len(entries); i++ { //nolint:gosec // kernel-bounded dir offset
		de := entries[i]
		recLen := direntHeader + len(de.Name)
		padded := (recLen + 7) &^ 7
		if len(e.bytes())+padded > maxSize {
			break
		}
		typ := (de.Mode & ModeMask) >> 12
		e.u64(de.Ino).
			u64(uint64(i) + 1).        //nolint:gosec // next offset; entry index is small
			u32(uint32(len(de.Name))). //nolint:gosec // name length is bounded by the request
			u32(typ).
			raw([]byte(de.Name)).
			zero(padded - recLen)
	}
	return e.bytes()
}
