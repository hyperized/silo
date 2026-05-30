package fuse_test

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/hyperized/silo/pkg/fuse"
)

// capConn is an in-memory Conn: it replays queued requests and captures the
// replies, so the whole protocol runs with no kernel.
type capConn struct {
	reqs [][]byte
	resp [][]byte
	i    int
}

func (c *capConn) ReadRequest() ([]byte, error) {
	if c.i >= len(c.reqs) {
		return nil, io.EOF
	}
	r := c.reqs[c.i]
	c.i++
	return r, nil
}

func (c *capConn) WriteResponse(b []byte) error {
	c.resp = append(c.resp, append([]byte(nil), b...))
	return nil
}

func (c *capConn) Close() error { return nil }

func req(op fuse.Opcode, nodeid, unique uint64, body []byte) []byte {
	h := make([]byte, fuse.InHeaderSize)
	binary.LittleEndian.PutUint32(h[0:4], uint32(fuse.InHeaderSize+len(body)))
	binary.LittleEndian.PutUint32(h[4:8], uint32(op))
	binary.LittleEndian.PutUint64(h[8:16], unique)
	binary.LittleEndian.PutUint64(h[16:24], nodeid)
	return append(h, body...)
}

func parseResp(t *testing.T, b []byte) (int32, []byte) {
	t.Helper()
	if len(b) < fuse.OutHeaderSize {
		t.Fatalf("reply too short: %d bytes", len(b))
	}
	l := binary.LittleEndian.Uint32(b[0:4])
	errno := int32(binary.LittleEndian.Uint32(b[4:8]))
	return errno, b[fuse.OutHeaderSize:l]
}

// body builders
func initBody() []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint32(b[0:4], 7)  // major
	binary.LittleEndian.PutUint32(b[4:8], 31) // minor
	return b
}
func name(s string) []byte { return append([]byte(s), 0) }
func mkdirBody(mode uint32, n string) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b[0:4], mode)
	return append(b, name(n)...)
}

func createBody(mode uint32, n string) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint32(b[4:8], mode) // flags, mode, umask, pad
	return append(b, name(n)...)
}
func openBody() []byte { return make([]byte, 8) }
func readBody(fh, off uint64, size uint32) []byte {
	b := make([]byte, 40)
	binary.LittleEndian.PutUint64(b[0:8], fh)
	binary.LittleEndian.PutUint64(b[8:16], off)
	binary.LittleEndian.PutUint32(b[16:20], size)
	return b
}

func writeBody(fh, off uint64, data []byte) []byte {
	b := make([]byte, 40)
	binary.LittleEndian.PutUint64(b[0:8], fh)
	binary.LittleEndian.PutUint64(b[8:16], off)
	binary.LittleEndian.PutUint32(b[16:20], uint32(len(data)))
	return append(b, data...)
}

func fhBody(fh uint64) []byte {
	b := make([]byte, 24)
	binary.LittleEndian.PutUint64(b[0:8], fh)
	return b
}

func setattrBody(valid uint32, size uint64, mode uint32) []byte {
	b := make([]byte, 56)
	binary.LittleEndian.PutUint32(b[0:4], valid)
	binary.LittleEndian.PutUint64(b[16:24], size)
	binary.LittleEndian.PutUint32(b[52:56], mode)
	return b
}

// run drives a list of requests through a fresh session and returns the replies.
func run(t *testing.T, reqs ...[]byte) [][]byte {
	t.Helper()
	conn := &capConn{reqs: reqs}
	if err := fuse.NewSession(conn, fuse.NewMemFS()).Serve(); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	return conn.resp
}

func TestSession_Init(t *testing.T) {
	resp := run(t, req(fuse.OpInit, 0, 1, initBody()))
	errno, body := parseResp(t, resp[0])
	if errno != 0 {
		t.Fatalf("INIT errno = %d", errno)
	}
	if major := binary.LittleEndian.Uint32(body[0:4]); major != 7 {
		t.Errorf("negotiated major = %d, want 7", major)
	}
	if minor := binary.LittleEndian.Uint32(body[4:8]); minor != 31 {
		t.Errorf("negotiated minor = %d, want 31", minor)
	}
}

func TestSession_FileLifecycle(t *testing.T) {
	root := fuse.RootNodeID
	const fooID = 2 // first node MemFS allocates
	resp := run(t,
		req(fuse.OpLookup, root, 1, name("foo")),                          // 0: ENOENT
		req(fuse.OpCreate, root, 2, createBody(0o644, "foo")),             // 1: entry+open, id 2
		req(fuse.OpWrite, fooID, 3, writeBody(fooID, 0, []byte("hello"))), // 2
		req(fuse.OpGetattr, fooID, 4, nil),                                // 3
		req(fuse.OpRead, fooID, 5, readBody(fooID, 0, 100)),               // 4
		req(fuse.OpRead, fooID, 6, readBody(fooID, 100, 100)),             // 5: past EOF
		req(fuse.OpLookup, root, 7, name("foo")),                          // 6: now resolves
	)
	if errno, _ := parseResp(t, resp[0]); errno != -int32(fuse.ENOENT) {
		t.Fatalf("lookup missing errno = %d, want -ENOENT", errno)
	}
	errno, createOut := parseResp(t, resp[1])
	if errno != 0 || binary.LittleEndian.Uint64(createOut[0:8]) != fooID {
		t.Fatalf("create = (%d, node %d), want (0, %d)", errno, binary.LittleEndian.Uint64(createOut[0:8]), fooID)
	}
	if _, w := parseResp(t, resp[2]); binary.LittleEndian.Uint32(w[0:4]) != 5 {
		t.Errorf("write returned %d bytes, want 5", binary.LittleEndian.Uint32(w[0:4]))
	}
	// attr.size: fuse_attr_out preamble is 16 bytes, then fuse_attr.size at +8.
	if _, a := parseResp(t, resp[3]); binary.LittleEndian.Uint64(a[24:32]) != 5 {
		t.Errorf("getattr size = %d, want 5", binary.LittleEndian.Uint64(a[24:32]))
	}
	if _, r := parseResp(t, resp[4]); string(r) != "hello" {
		t.Errorf("read = %q, want hello", r)
	}
	if _, r := parseResp(t, resp[5]); len(r) != 0 {
		t.Errorf("read past EOF = %q, want empty", r)
	}
	if _, e := parseResp(t, resp[6]); binary.LittleEndian.Uint64(e[0:8]) != fooID {
		t.Errorf("lookup node = %d, want %d", binary.LittleEndian.Uint64(e[0:8]), fooID)
	}
}

func TestSession_DirLifecycleAndReaddir(t *testing.T) {
	root := fuse.RootNodeID
	resp := run(t,
		req(fuse.OpMkdir, root, 1, mkdirBody(0o755, "dir")),
		req(fuse.OpCreate, root, 2, createBody(0o644, "file")),
		req(fuse.OpOpendir, root, 3, openBody()),
		req(fuse.OpReaddir, root, 4, readBody(0, 0, 4096)),
		req(fuse.OpReleasedir, root, 5, fhBody(root)),
	)
	if errno, _ := parseResp(t, resp[0]); errno != 0 {
		t.Fatalf("mkdir errno = %d", errno)
	}
	if errno, _ := parseResp(t, resp[2]); errno != 0 {
		t.Fatalf("opendir errno = %d", errno)
	}
	_, dirents := parseResp(t, resp[3])
	names := parseDirents(dirents)
	if !names["dir"] || !names["file"] {
		t.Errorf("readdir names = %v, want dir+file", names)
	}
	if errno, _ := parseResp(t, resp[4]); errno != 0 {
		t.Errorf("releasedir errno = %d", errno)
	}
}

func TestSession_OpenFlushReleaseSetattrUnlink(t *testing.T) {
	root := fuse.RootNodeID
	const id = 2
	resp := run(t,
		req(fuse.OpCreate, root, 1, createBody(0o644, "f")),                     // 0: id 2
		req(fuse.OpWrite, id, 2, writeBody(id, 0, []byte("hello"))),             // 1
		req(fuse.OpOpen, id, 3, openBody()),                                     // 2
		req(fuse.OpFlush, id, 4, fhBody(id)),                                    // 3
		req(fuse.OpRelease, id, 5, fhBody(id)),                                  // 4
		req(fuse.OpSetattr, id, 6, setattrBody(uint32(fuse.SetattrSize), 2, 0)), // 5: truncate to 2
		req(fuse.OpRead, id, 7, readBody(id, 0, 100)),                           // 6
		req(fuse.OpUnlink, root, 8, name("f")),                                  // 7
		req(fuse.OpLookup, root, 9, name("f")),                                  // 8
	)
	for _, i := range []int{0, 1, 2, 3, 4, 5, 7} {
		if errno, _ := parseResp(t, resp[i]); errno != 0 {
			t.Errorf("resp[%d] errno = %d, want 0", i, errno)
		}
	}
	if _, r := parseResp(t, resp[6]); string(r) != "he" {
		t.Errorf("read after truncate = %q, want he", r)
	}
	if errno, _ := parseResp(t, resp[8]); errno != -int32(fuse.ENOENT) {
		t.Errorf("lookup after unlink errno = %d, want -ENOENT", errno)
	}
}

func TestSession_Errors(t *testing.T) {
	root := fuse.RootNodeID
	const dirID = 2 // mkdir "d" allocates id 2
	resp := run(t,
		req(fuse.OpMkdir, root, 1, mkdirBody(0o755, "d")),     // 0: OK, id 2
		req(fuse.OpCreate, root, 2, createBody(0o644, "dup")), // 1: OK
		req(fuse.OpCreate, root, 3, createBody(0o644, "dup")), // 2: EEXIST
		req(fuse.OpRead, dirID, 4, readBody(dirID, 0, 10)),    // 3: EISDIR
		req(fuse.OpLookup, dirID, 5, name("x")),               // 4: ENOENT in empty dir
		req(fuse.OpRmdir, root, 6, name("d")),                 // 5: OK (empty)
		req(99, root, 7, nil),                                 // 6: unknown opcode -> ENOSYS
		req(fuse.OpRead, root, 8, []byte{0, 0}),               // 7: short READ body -> EINVAL
		req(fuse.OpDestroy, 0, 9, nil),                        // 8: OK
	)
	checks := []int32{0, 0, -int32(fuse.EEXIST), -int32(fuse.EISDIR), -int32(fuse.ENOENT), 0, -int32(fuse.ENOSYS), -int32(fuse.EINVAL), 0}
	for i, want := range checks {
		if errno, _ := parseResp(t, resp[i]); errno != want {
			t.Errorf("resp[%d] errno = %d, want %d", i, errno, want)
		}
	}
}

func TestSession_ForgetHasNoReplyAndMalformedDropped(t *testing.T) {
	root := fuse.RootNodeID
	forget := make([]byte, 8) // nlookup
	resp := run(t,
		req(fuse.OpForget, root, 1, forget), // no reply
		req(fuse.OpGetattr, root, 2, nil),   // a real reply follows
	)
	if len(resp) != 1 {
		t.Fatalf("got %d replies, want 1 (FORGET is silent)", len(resp))
	}

	// A truncated header is dropped without a reply or a crash.
	short := []byte{1, 2, 3}
	resp = run(t, short, req(fuse.OpGetattr, root, 3, nil))
	if len(resp) != 1 {
		t.Fatalf("got %d replies, want 1 (malformed dropped)", len(resp))
	}
}

func TestSession_MalformedBodiesAreEINVAL(t *testing.T) {
	root := fuse.RootNodeID
	short := []byte{0, 0}           // too short for any fixed body
	noNUL := []byte("unterminated") // a name with no NUL
	einval := -int32(fuse.EINVAL)

	cases := []struct {
		op   fuse.Opcode
		body []byte
	}{
		{fuse.OpInit, short},
		{fuse.OpSetattr, short},
		{fuse.OpLookup, noNUL},
		{fuse.OpMkdir, short},
		{fuse.OpCreate, short},
		{fuse.OpOpen, short},
		{fuse.OpOpendir, short},
		{fuse.OpRead, short},
		{fuse.OpReaddir, short},
		{fuse.OpWrite, short},
		{fuse.OpFlush, short},
		{fuse.OpRelease, short},
		{fuse.OpRmdir, noNUL},
		{fuse.OpUnlink, noNUL},
	}
	reqs := make([][]byte, len(cases))
	for i, c := range cases {
		reqs[i] = req(c.op, root, uint64(i+1), c.body)
	}
	resp := run(t, reqs...)
	for i, c := range cases {
		if errno, _ := parseResp(t, resp[i]); errno != einval {
			t.Errorf("%s with a malformed body: errno = %d, want -EINVAL", c.op, errno)
		}
	}
}

func TestSession_WithLoggerAndServeError(t *testing.T) {
	// WithLogger is accepted and the session still serves.
	conn := &capConn{reqs: [][]byte{req(fuse.OpGetattr, fuse.RootNodeID, 1, nil)}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := fuse.NewSession(conn, fuse.NewMemFS(), fuse.WithLogger(logger)).Serve(); err != nil {
		t.Fatalf("Serve with logger: %v", err)
	}

	// A non-EOF read error propagates out of Serve.
	boom := &errConn{err: errors.New("device gone")}
	if err := fuse.NewSession(boom, fuse.NewMemFS()).Serve(); err == nil {
		t.Error("Serve should return a non-EOF read error")
	}
}

type errConn struct{ err error }

func (c *errConn) ReadRequest() ([]byte, error) { return nil, c.err }
func (c *errConn) WriteResponse([]byte) error   { return nil }
func (c *errConn) Close() error                 { return nil }

// writeErrConn yields one request then fails the reply write.
type writeErrConn struct{ done bool }

func (c *writeErrConn) ReadRequest() ([]byte, error) {
	if c.done {
		return nil, io.EOF
	}
	c.done = true
	return req(fuse.OpGetattr, fuse.RootNodeID, 1, nil), nil
}
func (c *writeErrConn) WriteResponse([]byte) error { return errors.New("reply write failed") }
func (c *writeErrConn) Close() error               { return nil }

func TestSession_WriteErrorPropagates(t *testing.T) {
	if err := fuse.NewSession(&writeErrConn{}, fuse.NewMemFS()).Serve(); err == nil {
		t.Error("a reply-write failure should propagate out of Serve")
	}
}

func TestSession_InitMinorDowngradeAndReaddirError(t *testing.T) {
	root := fuse.RootNodeID
	lowInit := make([]byte, 16)
	binary.LittleEndian.PutUint32(lowInit[0:4], 7)
	binary.LittleEndian.PutUint32(lowInit[4:8], 10)
	resp := run(t,
		req(fuse.OpInit, 0, 1, lowInit),                     // 0: negotiates to minor 10
		req(fuse.OpCreate, root, 2, createBody(0o644, "f")), // 1: id 2
		req(fuse.OpReaddir, 2, 3, readBody(0, 0, 4096)),     // 2: ENOTDIR on a file
	)
	if _, body := parseResp(t, resp[0]); binary.LittleEndian.Uint32(body[4:8]) != 10 {
		t.Errorf("negotiated minor = %d, want 10", binary.LittleEndian.Uint32(body[4:8]))
	}
	if errno, _ := parseResp(t, resp[2]); errno != -int32(fuse.ENOTDIR) {
		t.Errorf("readdir on a file errno = %d, want -ENOTDIR", errno)
	}
}

// parseDirents extracts the names from a packed fuse_dirent stream.
func parseDirents(b []byte) map[string]bool {
	names := map[string]bool{}
	for len(b) >= 24 {
		namelen := int(binary.LittleEndian.Uint32(b[16:20]))
		rec := 24 + namelen
		padded := (rec + 7) &^ 7
		if padded > len(b) {
			break
		}
		names[string(b[24:24+namelen])] = true
		b = b[padded:]
	}
	return names
}
