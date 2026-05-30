package fuse

import "testing"

func TestOpcodeString(t *testing.T) {
	if OpLookup.String() != "LOOKUP" {
		t.Errorf("LOOKUP string = %q", OpLookup.String())
	}
	if got := Opcode(9999).String(); got != "Opcode(9999)" {
		t.Errorf("unknown opcode string = %q", got)
	}
}

func TestDecodeShortBodies(t *testing.T) {
	short := []byte{0, 0}
	if _, err := decodeInitIn(short); err == nil {
		t.Error("decodeInitIn short")
	}
	if _, err := decodeReadIn(short); err == nil {
		t.Error("decodeReadIn short")
	}
	if _, err := decodeWriteIn(short); err == nil {
		t.Error("decodeWriteIn short")
	}
	if _, err := decodeWriteIn(make([]byte, 40)); err != nil {
		t.Error("decodeWriteIn 40-byte header with no payload should be valid (size 0)")
	}
	if _, err := decodeOpenIn(short); err == nil {
		t.Error("decodeOpenIn short")
	}
	if _, err := decodeReleaseIn(short); err == nil {
		t.Error("decodeReleaseIn short")
	}
	if _, err := decodeMkdirIn(short); err == nil {
		t.Error("decodeMkdirIn short")
	}
	if _, err := decodeMkdirIn([]byte{0, 0, 0, 0, 0, 0, 0, 0, 'x'}); err == nil {
		t.Error("decodeMkdirIn unterminated name")
	}
	if _, err := decodeCreateIn(short); err == nil {
		t.Error("decodeCreateIn short")
	}
	if _, err := decodeCreateIn(make([]byte, 16)); err == nil {
		t.Error("decodeCreateIn missing name")
	}
	if _, err := decodeForgetIn(short); err == nil {
		t.Error("decodeForgetIn short")
	}
	if _, err := decodeSetattrIn(short); err == nil {
		t.Error("decodeSetattrIn short")
	}
}

func TestDecodeWriteInPayloadShort(t *testing.T) {
	// A 40-byte header claiming a 10-byte payload that is not present.
	b := make([]byte, 40)
	b[16] = 10 // size = 10
	if _, err := decodeWriteIn(b); err == nil {
		t.Error("decodeWriteIn should reject a header claiming more payload than present")
	}
}

func TestDecodeInHeaderShort(t *testing.T) {
	if _, err := DecodeInHeader([]byte{1, 2, 3}); err == nil {
		t.Error("DecodeInHeader should reject a short buffer")
	}
}

func TestCstr(t *testing.T) {
	if s, err := cstr([]byte("name\x00extra")); err != nil || s != "name" {
		t.Errorf("cstr = (%q, %v)", s, err)
	}
	if _, err := cstr([]byte("no-nul")); err == nil {
		t.Error("cstr without NUL should error")
	}
}

func TestPackDirents_Pagination(t *testing.T) {
	entries := []DirEntry{
		{Ino: 2, Name: "alpha", Mode: ModeReg},
		{Ino: 3, Name: "bravo", Mode: ModeDir},
		{Ino: 4, Name: "charlie", Mode: ModeReg},
	}
	// A tiny buffer fits only the first record, so packing stops early.
	out := packDirents(entries, 0, 40)
	if len(out) == 0 || len(out) > 40 {
		t.Errorf("packed %d bytes into a 40-byte budget", len(out))
	}
	// Starting at offset 1 skips the first entry.
	full := packDirents(entries, 1, 4096)
	if len(full) == 0 {
		t.Error("expected entries from offset 1")
	}
	// Offset past the end yields nothing.
	if len(packDirents(entries, 3, 4096)) != 0 {
		t.Error("offset past the end should pack nothing")
	}
}

func TestMemFS_EdgeErrnos(t *testing.T) {
	m := NewMemFS()
	// Seed a file and a dir under root.
	fileID, _, _, _ := m.Create(RootNodeID, "f", 0, 0o644)
	dirID, _, _ := m.Mkdir(RootNodeID, "d", 0o755)
	_, _, _, _ = m.Create(dirID, "child", 0, 0o644) // make d non-empty

	cases := []struct {
		name string
		got  Errno
		want Errno
	}{
		{"lookup-in-file", lookupErr(m, fileID, "x"), ENOTDIR},
		{"lookup-missing", lookupErr(m, RootNodeID, "nope"), ENOENT},
		{"getattr-missing", getattrErr(m, 999), ENOENT},
		{"open-missing", openErr(m, 999), ENOENT},
		{"opendir-on-file", opendirErr(m, fileID), ENOTDIR},
		{"opendir-missing", opendirErr(m, 999), ENOENT},
		{"readdir-on-file", readdirErr(m, fileID), ENOTDIR},
		{"readdir-missing", readdirErr(m, 999), ENOENT},
		{"read-missing", readErr(m, 999), ENOENT},
		{"write-missing", writeErr(m, 999), ENOENT},
		{"write-to-dir", writeErr(m, dirID), EISDIR},
		{"rmdir-nonempty", m.Rmdir(RootNodeID, "d"), ENOTEMPTY},
		{"rmdir-on-file", m.Rmdir(RootNodeID, "f"), ENOTDIR},
		{"unlink-a-dir", m.Unlink(RootNodeID, "d"), EISDIR},
		{"unlink-missing", m.Unlink(RootNodeID, "gone"), ENOENT},
		{"remove-bad-parent", m.Unlink(999, "x"), ENOTDIR},
		{"create-bad-parent", createErr(m, 999, "x"), ENOTDIR},
		{"setattr-missing", setattrErr(m, 999), ENOENT},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s: errno = %d, want %d", tc.name, tc.got, tc.want)
		}
	}

	// Forget is a no-op; Setattr chmod adjusts the permission bits.
	m.Forget(fileID, 1)
	attr, errno := m.Setattr(fileID, SetattrIn{Valid: SetattrMode, Mode: 0o600})
	if errno != OK || attr.Mode&0o777 != 0o600 {
		t.Errorf("chmod = (%o, %d), want 0600 OK", attr.Mode&0o777, errno)
	}
}

func lookupErr(m *MemFS, p uint64, n string) Errno { _, _, e := m.Lookup(p, n); return e }
func getattrErr(m *MemFS, id uint64) Errno         { _, e := m.Getattr(id); return e }
func openErr(m *MemFS, id uint64) Errno            { _, e := m.Open(id, 0); return e }
func opendirErr(m *MemFS, id uint64) Errno         { _, e := m.Opendir(id, 0); return e }
func readdirErr(m *MemFS, id uint64) Errno         { _, e := m.ReadDir(id); return e }
func readErr(m *MemFS, id uint64) Errno            { _, e := m.Read(id, 0, 0, 1); return e }
func writeErr(m *MemFS, id uint64) Errno           { _, e := m.Write(id, 0, 0, []byte("x")); return e }
func createErr(m *MemFS, p uint64, n string) Errno { _, _, _, e := m.Create(p, n, 0, 0); return e }
func setattrErr(m *MemFS, id uint64) Errno         { _, e := m.Setattr(id, SetattrIn{}); return e }
