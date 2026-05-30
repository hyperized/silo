// Package fuse is a from-scratch implementation of the Linux FUSE protocol on
// the standard library — no cgo, no libfuse, no third-party FUSE module. It
// decodes the kernel's requests from the /dev/fuse character device, dispatches
// them to a Filesystem implementation, and encodes the replies.
//
// It is published as a standalone library (github.com/hyperized/silo/pkg/fuse):
// silo mounts its distributed store through it (see cmd/silo-fuse), but nothing
// here depends on silo, so it is usable as a pure-Go FUSE library on its own.
//
// The wire layer (this file) is split from the OS layer (mount, /dev/fuse) so
// the protocol can be exercised over an in-memory pipe in tests, with no kernel
// involved. Struct layouts follow linux/fuse.h; all integers are little-endian.
package fuse

import (
	"encoding/binary"
	"fmt"
)

// Protocol version this library implements. The kernel sends its own version in
// INIT; the session negotiates down to min(kernel, library).
const (
	KernelVersion      = 7
	KernelMinorVersion = 31
)

// Opcode is a FUSE request opcode (linux/fuse.h `enum fuse_opcode`).
type Opcode uint32

// The opcodes this library handles. Values match linux/fuse.h exactly — the
// kernel keys on them, so they are a hard external contract.
const (
	OpLookup      Opcode = 1
	OpForget      Opcode = 2
	OpGetattr     Opcode = 3
	OpSetattr     Opcode = 4
	OpReadlink    Opcode = 5
	OpSymlink     Opcode = 6
	OpMknod       Opcode = 8
	OpMkdir       Opcode = 9
	OpUnlink      Opcode = 10
	OpRmdir       Opcode = 11
	OpRename      Opcode = 12
	OpLink        Opcode = 13
	OpOpen        Opcode = 14
	OpRead        Opcode = 15
	OpWrite       Opcode = 16
	OpStatfs      Opcode = 17
	OpRelease     Opcode = 18
	OpFsync       Opcode = 20
	OpFlush       Opcode = 25
	OpInit        Opcode = 26
	OpOpendir     Opcode = 27
	OpReaddir     Opcode = 28
	OpReleasedir  Opcode = 29
	OpFsyncdir    Opcode = 30
	OpCreate      Opcode = 35
	OpDestroy     Opcode = 38
	OpReaddirplus Opcode = 44
)

// String renders an opcode for logs.
func (o Opcode) String() string {
	if name, ok := opcodeNames[o]; ok {
		return name
	}
	return fmt.Sprintf("Opcode(%d)", uint32(o))
}

var opcodeNames = map[Opcode]string{
	OpLookup: "LOOKUP", OpForget: "FORGET", OpGetattr: "GETATTR", OpSetattr: "SETATTR",
	OpReadlink: "READLINK", OpSymlink: "SYMLINK", OpMknod: "MKNOD", OpMkdir: "MKDIR",
	OpUnlink: "UNLINK", OpRmdir: "RMDIR", OpRename: "RENAME", OpLink: "LINK",
	OpOpen: "OPEN", OpRead: "READ", OpWrite: "WRITE", OpStatfs: "STATFS",
	OpRelease: "RELEASE", OpFsync: "FSYNC", OpFlush: "FLUSH", OpInit: "INIT",
	OpOpendir: "OPENDIR", OpReaddir: "READDIR", OpReleasedir: "RELEASEDIR",
	OpFsyncdir: "FSYNCDIR", OpCreate: "CREATE", OpDestroy: "DESTROY", OpReaddirplus: "READDIRPLUS",
}

// InHeaderSize is the wire size of fuse_in_header (linux/fuse.h): 40 bytes.
const InHeaderSize = 40

// InHeader is the fixed prefix on every request the kernel sends.
type InHeader struct {
	Len    uint32 // total request length including this header
	Opcode Opcode
	Unique uint64 // request id; the reply echoes it
	Nodeid uint64 // the inode the request targets
	UID    uint32
	GID    uint32
	PID    uint32
	// 4 bytes padding on the wire
}

// DecodeInHeader parses a fuse_in_header from the front of b.
func DecodeInHeader(b []byte) (InHeader, error) {
	if len(b) < InHeaderSize {
		return InHeader{}, fmt.Errorf("fuse: in-header is %d bytes, need %d", len(b), InHeaderSize)
	}
	return InHeader{
		Len:    binary.LittleEndian.Uint32(b[0:4]),
		Opcode: Opcode(binary.LittleEndian.Uint32(b[4:8])),
		Unique: binary.LittleEndian.Uint64(b[8:16]),
		Nodeid: binary.LittleEndian.Uint64(b[16:24]),
		UID:    binary.LittleEndian.Uint32(b[24:28]),
		GID:    binary.LittleEndian.Uint32(b[28:32]),
		PID:    binary.LittleEndian.Uint32(b[32:36]),
	}, nil
}

// OutHeaderSize is the wire size of fuse_out_header: 16 bytes.
const OutHeaderSize = 16

// encodeOutHeader writes a fuse_out_header for a reply of total length len,
// error code errno (0 on success, a negative errno per the FUSE convention),
// echoing the request's unique id.
func encodeOutHeader(dst []byte, length uint32, errno int32, unique uint64) {
	binary.LittleEndian.PutUint32(dst[0:4], length)
	binary.LittleEndian.PutUint32(dst[4:8], uint32(errno)) //nolint:gosec // errno fits a uint32 by the FUSE convention
	binary.LittleEndian.PutUint64(dst[8:16], unique)
}

// le is a tiny helper for building little-endian reply bodies.
type le struct{ b []byte }

func newLE(n int) *le          { return &le{b: make([]byte, 0, n)} }
func (e *le) u32(v uint32) *le { e.b = binary.LittleEndian.AppendUint32(e.b, v); return e }
func (e *le) u64(v uint64) *le { e.b = binary.LittleEndian.AppendUint64(e.b, v); return e }
func (e *le) raw(p []byte) *le { e.b = append(e.b, p...); return e }
func (e *le) zero(n int) *le   { e.b = append(e.b, make([]byte, n)...); return e }
func (e *le) bytes() []byte    { return e.b }
