package fuse

import (
	"errors"
	"io"
	"log/slog"
)

// Conn is the channel a Session reads requests from and writes replies to. The
// production implementation wraps /dev/fuse (see conn_linux.go); tests supply an
// in-memory pipe, so the entire protocol is exercised without a kernel. Each
// ReadRequest returns exactly one complete FUSE message (its first four bytes
// are the length); each reply is one WriteResponse.
type Conn interface {
	ReadRequest() ([]byte, error)
	WriteResponse([]byte) error
	Close() error
}

// Session decodes FUSE requests off a Conn, dispatches them to a Filesystem,
// and writes the replies. It negotiates the protocol version on INIT and serves
// requests sequentially until the Conn reports EOF (the mount went away) or a
// read error.
type Session struct {
	conn     Conn
	fs       Filesystem
	logger   *slog.Logger
	maxWrite uint32
}

// SessionOption configures a Session.
type SessionOption func(*Session)

// WithLogger attaches a logger; without one the session is silent.
func WithLogger(l *slog.Logger) SessionOption {
	return func(s *Session) { s.logger = l }
}

// DefaultMaxWrite is the largest write payload the session advertises to the
// kernel (128 KiB, the conventional FUSE default).
const DefaultMaxWrite = 128 * 1024

// NewSession binds a Filesystem to a Conn.
func NewSession(conn Conn, fs Filesystem, opts ...SessionOption) *Session {
	s := &Session{conn: conn, fs: fs, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), maxWrite: DefaultMaxWrite}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Serve reads and dispatches requests until the connection closes. It returns
// nil on a clean EOF (unmount) and the read error otherwise.
func (s *Session) Serve() error {
	for {
		req, err := s.conn.ReadRequest()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := s.dispatch(req); err != nil {
			return err
		}
	}
}

// dispatch decodes one request and routes it to the right handler. A handler
// error is a write failure on the Conn (fatal); a Filesystem error becomes an
// errno reply, not a dispatch error.
func (s *Session) dispatch(req []byte) error {
	h, err := DecodeInHeader(req)
	if err != nil {
		s.logger.Warn("fuse: dropping malformed request", "error", err)
		return nil
	}
	body := req[InHeaderSize:]

	switch h.Opcode {
	case OpInit:
		return s.handleInit(h, body)
	case OpDestroy:
		return s.reply(h.Unique, OK, nil)
	case OpForget:
		if in, err := decodeForgetIn(body); err == nil {
			s.fs.Forget(h.Nodeid, in.Nlookup)
		}
		return nil // FORGET has no reply
	case OpGetattr:
		attr, errno := s.fs.Getattr(h.Nodeid)
		return s.replyOrErrno(h.Unique, errno, AttrOut(attr))
	case OpSetattr:
		return s.handleSetattr(h, body)
	case OpLookup:
		return s.handleLookup(h, body)
	case OpMkdir:
		return s.handleMkdir(h, body)
	case OpRmdir:
		return s.handleNamed(h, body, s.fs.Rmdir)
	case OpUnlink:
		return s.handleNamed(h, body, s.fs.Unlink)
	case OpCreate:
		return s.handleCreate(h, body)
	case OpOpen:
		return s.handleOpen(h, body, s.fs.Open)
	case OpOpendir:
		return s.handleOpen(h, body, s.fs.Opendir)
	case OpRead:
		return s.handleRead(h, body)
	case OpReaddir:
		return s.handleReaddir(h, body)
	case OpWrite:
		return s.handleWrite(h, body)
	case OpFlush:
		return s.handleFh(h, body, s.fs.Flush)
	case OpRelease:
		return s.handleFh(h, body, s.fs.Release)
	case OpReleasedir:
		return s.handleFh(h, body, func(uint64, uint64) Errno { return OK })
	default:
		return s.reply(h.Unique, ENOSYS, nil)
	}
}

func (s *Session) handleInit(h InHeader, body []byte) error {
	in, err := decodeInitIn(body)
	if err != nil {
		return s.reply(h.Unique, EINVAL, nil)
	}
	minor := uint32(KernelMinorVersion)
	if in.Minor < minor {
		minor = in.Minor
	}
	out := newLE(64).
		u32(KernelVersion).u32(minor).
		u32(in.MaxReadahead).u32(0). // max_readahead, flags
		u32(0).                      // max_background(16) + congestion_threshold(16)
		u32(s.maxWrite).             // max_write
		u32(1).                      // time_gran
		u32(0).                      // max_pages(16) + map_alignment(16)
		u32(0).                      // flags2
		zero(28).                    // unused[7]
		bytes()
	return s.reply(h.Unique, OK, out)
}

func (s *Session) handleSetattr(h InHeader, body []byte) error {
	in, err := decodeSetattrIn(body)
	if err != nil {
		return s.reply(h.Unique, EINVAL, nil)
	}
	attr, errno := s.fs.Setattr(h.Nodeid, in)
	return s.replyOrErrno(h.Unique, errno, AttrOut(attr))
}

func (s *Session) handleLookup(h InHeader, body []byte) error {
	name, err := cstr(body)
	if err != nil {
		return s.reply(h.Unique, EINVAL, nil)
	}
	nodeID, attr, errno := s.fs.Lookup(h.Nodeid, name)
	return s.replyOrErrno(h.Unique, errno, EntryOut(nodeID, attr))
}

func (s *Session) handleMkdir(h InHeader, body []byte) error {
	in, err := decodeMkdirIn(body)
	if err != nil {
		return s.reply(h.Unique, EINVAL, nil)
	}
	nodeID, attr, errno := s.fs.Mkdir(h.Nodeid, in.Name, in.Mode)
	return s.replyOrErrno(h.Unique, errno, EntryOut(nodeID, attr))
}

func (s *Session) handleNamed(h InHeader, body []byte, fn func(parent uint64, name string) Errno) error {
	name, err := cstr(body)
	if err != nil {
		return s.reply(h.Unique, EINVAL, nil)
	}
	return s.reply(h.Unique, fn(h.Nodeid, name), nil)
}

func (s *Session) handleCreate(h InHeader, body []byte) error {
	in, err := decodeCreateIn(body)
	if err != nil {
		return s.reply(h.Unique, EINVAL, nil)
	}
	nodeID, attr, fh, errno := s.fs.Create(h.Nodeid, in.Name, in.Flags, in.Mode)
	return s.replyOrErrno(h.Unique, errno, CreateOut(nodeID, attr, fh, 0))
}

func (s *Session) handleOpen(h InHeader, body []byte, fn func(nodeID uint64, flags uint32) (uint64, Errno)) error {
	in, err := decodeOpenIn(body)
	if err != nil {
		return s.reply(h.Unique, EINVAL, nil)
	}
	fh, errno := fn(h.Nodeid, in.Flags)
	return s.replyOrErrno(h.Unique, errno, OpenOut(fh, 0))
}

func (s *Session) handleRead(h InHeader, body []byte) error {
	in, err := decodeReadIn(body)
	if err != nil {
		return s.reply(h.Unique, EINVAL, nil)
	}
	data, errno := s.fs.Read(h.Nodeid, in.Fh, in.Offset, in.Size)
	return s.replyOrErrno(h.Unique, errno, data)
}

func (s *Session) handleReaddir(h InHeader, body []byte) error {
	in, err := decodeReadIn(body)
	if err != nil {
		return s.reply(h.Unique, EINVAL, nil)
	}
	entries, errno := s.fs.ReadDir(h.Nodeid)
	if errno != OK {
		return s.reply(h.Unique, errno, nil)
	}
	return s.reply(h.Unique, OK, packDirents(entries, in.Offset, int(in.Size)))
}

func (s *Session) handleWrite(h InHeader, body []byte) error {
	in, err := decodeWriteIn(body)
	if err != nil {
		return s.reply(h.Unique, EINVAL, nil)
	}
	n, errno := s.fs.Write(h.Nodeid, in.Fh, in.Offset, in.Data)
	return s.replyOrErrno(h.Unique, errno, WriteOut(n))
}

func (s *Session) handleFh(h InHeader, body []byte, fn func(nodeID, fh uint64) Errno) error {
	in, err := decodeReleaseIn(body) // release_in and flush_in both carry fh first
	if err != nil {
		return s.reply(h.Unique, EINVAL, nil)
	}
	return s.reply(h.Unique, fn(h.Nodeid, in.Fh), nil)
}

// replyOrErrno writes body on success, or an empty errno reply on failure.
func (s *Session) replyOrErrno(unique uint64, errno Errno, body []byte) error {
	if errno != OK {
		return s.reply(unique, errno, nil)
	}
	return s.reply(unique, OK, body)
}

// reply writes a fuse_out_header followed by body. The kernel expects the error
// field as a negative errno.
func (s *Session) reply(unique uint64, errno Errno, body []byte) error {
	out := make([]byte, OutHeaderSize+len(body))
	encodeOutHeader(out, uint32(len(out)), -int32(errno), unique) //nolint:gosec // reply length is bounded by the buffer
	copy(out[OutHeaderSize:], body)
	return s.conn.WriteResponse(out)
}
