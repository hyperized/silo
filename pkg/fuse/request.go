package fuse

import (
	"encoding/binary"
	"fmt"
)

// The decoders below parse the request bodies (everything after the in-header)
// the core opcodes carry. Each is bounds-checked: a short body is a protocol
// error rather than a panic, since the bytes come straight off the kernel
// channel. Only the fields the dispatcher uses are surfaced.

// InitIn is fuse_init_in: the kernel's protocol version and feature flags.
type InitIn struct {
	Major        uint32
	Minor        uint32
	MaxReadahead uint32
	Flags        uint32
}

func decodeInitIn(b []byte) (InitIn, error) {
	if len(b) < 16 {
		return InitIn{}, fmt.Errorf("fuse: INIT body is %d bytes, need >= 16", len(b))
	}
	return InitIn{
		Major:        binary.LittleEndian.Uint32(b[0:4]),
		Minor:        binary.LittleEndian.Uint32(b[4:8]),
		MaxReadahead: binary.LittleEndian.Uint32(b[8:12]),
		Flags:        binary.LittleEndian.Uint32(b[12:16]),
	}, nil
}

// ReadIn is fuse_read_in, shared by READ and READDIR.
type ReadIn struct {
	Fh     uint64
	Offset uint64
	Size   uint32
}

func decodeReadIn(b []byte) (ReadIn, error) {
	if len(b) < 24 {
		return ReadIn{}, fmt.Errorf("fuse: READ body is %d bytes, need >= 24", len(b))
	}
	return ReadIn{
		Fh:     binary.LittleEndian.Uint64(b[0:8]),
		Offset: binary.LittleEndian.Uint64(b[8:16]),
		Size:   binary.LittleEndian.Uint32(b[16:20]),
	}, nil
}

// WriteIn is fuse_write_in plus the payload that follows its 40-byte header.
type WriteIn struct {
	Fh     uint64
	Offset uint64
	Data   []byte
}

func decodeWriteIn(b []byte) (WriteIn, error) {
	const hdr = 40
	if len(b) < hdr {
		return WriteIn{}, fmt.Errorf("fuse: WRITE header is %d bytes, need >= %d", len(b), hdr)
	}
	size := binary.LittleEndian.Uint32(b[16:20])
	if len(b)-hdr < int(size) {
		return WriteIn{}, fmt.Errorf("fuse: WRITE payload is %d bytes, header claims %d", len(b)-hdr, size)
	}
	return WriteIn{
		Fh:     binary.LittleEndian.Uint64(b[0:8]),
		Offset: binary.LittleEndian.Uint64(b[8:16]),
		Data:   b[hdr : hdr+int(size)],
	}, nil
}

// OpenIn is fuse_open_in (the open flags; OPENDIR shares the layout).
type OpenIn struct{ Flags uint32 }

func decodeOpenIn(b []byte) (OpenIn, error) {
	if len(b) < 8 {
		return OpenIn{}, fmt.Errorf("fuse: OPEN body is %d bytes, need >= 8", len(b))
	}
	return OpenIn{Flags: binary.LittleEndian.Uint32(b[0:4])}, nil
}

// ReleaseIn is fuse_release_in (the file handle to close).
type ReleaseIn struct{ Fh uint64 }

func decodeReleaseIn(b []byte) (ReleaseIn, error) {
	if len(b) < 8 {
		return ReleaseIn{}, fmt.Errorf("fuse: RELEASE body is %d bytes, need >= 8", len(b))
	}
	return ReleaseIn{Fh: binary.LittleEndian.Uint64(b[0:8])}, nil
}

// MkdirIn is fuse_mkdir_in: the mode plus the directory name that follows.
type MkdirIn struct {
	Mode uint32
	Name string
}

func decodeMkdirIn(b []byte) (MkdirIn, error) {
	if len(b) < 8 {
		return MkdirIn{}, fmt.Errorf("fuse: MKDIR body is %d bytes, need >= 8", len(b))
	}
	name, err := cstr(b[8:])
	if err != nil {
		return MkdirIn{}, err
	}
	return MkdirIn{Mode: binary.LittleEndian.Uint32(b[0:4]), Name: name}, nil
}

// CreateIn is fuse_create_in: open flags, mode, and the new file's name.
type CreateIn struct {
	Flags uint32
	Mode  uint32
	Name  string
}

func decodeCreateIn(b []byte) (CreateIn, error) {
	const hdr = 16
	if len(b) < hdr {
		return CreateIn{}, fmt.Errorf("fuse: CREATE body is %d bytes, need >= %d", len(b), hdr)
	}
	name, err := cstr(b[hdr:])
	if err != nil {
		return CreateIn{}, err
	}
	return CreateIn{
		Flags: binary.LittleEndian.Uint32(b[0:4]),
		Mode:  binary.LittleEndian.Uint32(b[4:8]),
		Name:  name,
	}, nil
}

// ForgetIn is fuse_forget_in: how many lookups to drop for the node.
type ForgetIn struct{ Nlookup uint64 }

func decodeForgetIn(b []byte) (ForgetIn, error) {
	if len(b) < 8 {
		return ForgetIn{}, fmt.Errorf("fuse: FORGET body is %d bytes, need >= 8", len(b))
	}
	return ForgetIn{Nlookup: binary.LittleEndian.Uint64(b[0:8])}, nil
}

// SetattrValid bits (fuse_setattr_in.valid) for the fields silo honours.
const (
	SetattrMode = 1 << 0
	SetattrSize = 1 << 3
)

// SetattrIn is the subset of fuse_setattr_in silo acts on: a truncation (size)
// and a chmod (mode), each gated by its Valid bit.
type SetattrIn struct {
	Valid uint32
	Size  uint64
	Mode  uint32
}

func decodeSetattrIn(b []byte) (SetattrIn, error) {
	// fuse_setattr_in: valid(4) pad(4) fh(8) size(8) lock_owner(8) atime(8)
	// mtime(8) ctime(8) atimensec(4) mtimensec(4) ctimensec(4) mode(4) ...
	if len(b) < 56 {
		return SetattrIn{}, fmt.Errorf("fuse: SETATTR body is %d bytes, need >= 56", len(b))
	}
	return SetattrIn{
		Valid: binary.LittleEndian.Uint32(b[0:4]),
		Size:  binary.LittleEndian.Uint64(b[16:24]),
		Mode:  binary.LittleEndian.Uint32(b[52:56]),
	}, nil
}

// cstr reads a NUL-terminated name from the front of b (the form LOOKUP,
// UNLINK, and RMDIR carry, and the trailing name on MKDIR/CREATE).
func cstr(b []byte) (string, error) {
	for i, c := range b {
		if c == 0 {
			return string(b[:i]), nil //nolint:gosec // size was bounds-checked against the buffer above
		}
	}
	return "", fmt.Errorf("fuse: name is not NUL-terminated")
}
