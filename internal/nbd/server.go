// Package nbd is a minimal Network Block Device server: it speaks the NBD
// "fixed newstyle" handshake and transmission protocol over a stream and maps
// each block request onto a Device (an io.ReaderAt/io.WriterAt with a size).
// silo serves a volume's block-I/O SDK through it, so a Linux host can
// nbd-client + mkfs + mount a silo volume. The protocol is implemented over
// the standard library only — no NBD dependency.
package nbd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
)

// Protocol magics and flags (see the NBD protocol spec).
const (
	magicNBD     uint64 = 0x4e42444d41474943 // "NBDMAGIC"
	magicIHAVE   uint64 = 0x49484156454f5054 // "IHAVEOPT"
	magicRep     uint64 = 0x0003e889045565a9 // option reply
	magicRequest uint32 = 0x25609513
	magicReply   uint32 = 0x67446698

	flagFixedNewstyle uint16 = 1 << 0
	flagNoZeroes      uint16 = 1 << 1

	cFlagNoZeroes uint32 = 1 << 1

	optExportName uint32 = 1
	optAbort      uint32 = 2
	optGo         uint32 = 7

	repAck      uint32 = 1
	repInfo     uint32 = 3
	repErrUnsup uint32 = 0x80000000 | 1

	infoExport uint16 = 0

	tFlagHasFlags  uint16 = 1 << 0
	tFlagSendFlush uint16 = 1 << 2
	tFlagSendTrim  uint16 = 1 << 5

	cmdRead  uint16 = 0
	cmdWrite uint16 = 1
	cmdDisc  uint16 = 2
	cmdFlush uint16 = 3
	cmdTrim  uint16 = 4

	errIO    uint32 = 5
	errInval uint32 = 22
)

// maxRequestBytes caps a single read/write (and option payload) so a hostile
// or buggy client cannot make the server allocate unbounded memory per
// request. A var so tests can lower it without sending huge payloads.
var maxRequestBytes uint32 = 64 << 20

// maxInflightPerConn bounds the number of NBD requests one connection can
// have outstanding inside the server. The transmission protocol allows the
// server to reply out of order (each reply carries the client's handle), so
// reads and writes overlap across the worker pool while the connection's
// single TCP reader keeps pulling new requests off the wire. A small cap
// keeps a misbehaving client from spawning unbounded per-request goroutines
// and chunkstore fan-outs from one socket.
const maxInflightPerConn = 32

// Device is the block device an export serves: random-access I/O plus a fixed
// advertised size.
type Device interface {
	io.ReaderAt
	io.WriterAt
	Size() int64
}

// Backend resolves an export name to a Device. release is called once the
// client disconnects (e.g. to drop the volume's lease); it may be nil.
type Backend interface {
	Open(ctx context.Context, export string) (dev Device, release func(), err error)
}

// Server serves NBD over accepted connections, one Device per connection.
type Server struct {
	backend Backend
	logger  *slog.Logger
}

// NewServer wires a backend onto the NBD protocol. A nil logger defaults to
// slog.Default.
func NewServer(backend Backend, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{backend: backend, logger: logger}
}

// Serve accepts connections until ctx is cancelled or the listener errors,
// handling each in its own goroutine. On cancellation it closes every
// in-flight connection so a transmit loop blocked reading the next request
// unblocks and its goroutine exits, then waits for them to drain — a graceful
// shutdown rather than leaving established sessions running until the client
// happens to disconnect.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	var (
		mu      sync.Mutex
		conns   = map[net.Conn]struct{}{}
		closing bool
		wg      sync.WaitGroup
	)
	// track registers a live connection, reporting false if shutdown has
	// already begun (so the caller closes it immediately rather than racing
	// the close-all sweep and lingering).
	track := func(c net.Conn) bool {
		mu.Lock()
		defer mu.Unlock()
		if closing {
			return false
		}
		conns[c] = struct{}{}
		return true
	}
	untrack := func(c net.Conn) {
		mu.Lock()
		delete(conns, c)
		mu.Unlock()
	}
	closeAll := func() {
		mu.Lock()
		closing = true
		live := make([]net.Conn, 0, len(conns))
		for c := range conns {
			live = append(live, c)
		}
		mu.Unlock()
		for _, c := range live {
			_ = c.Close()
		}
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			closeAll()
		case <-stop:
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				wg.Wait() // let the closed in-flight connections drain
				return nil
			}
			return fmt.Errorf("nbd: accept failed (%w)", err)
		}
		if !track(conn) {
			_ = conn.Close()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer untrack(conn)
			if err := s.ServeConn(ctx, conn); err != nil {
				s.logger.Warn("nbd connection ended", "error", err)
			}
		}()
	}
}

// ServeConn runs the handshake and transmission phases on one connection,
// closing it on return. Exported so a test can drive it over a pipe.
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	defer func() { _ = conn.Close() }()

	dev, release, export, err := s.handshake(ctx, conn)
	if err != nil {
		return err
	}
	if dev == nil { // a clean abort during option haggling
		return nil
	}
	if release != nil {
		defer release()
	}
	s.logger.Info("nbd export attached", "export", export, "size", dev.Size())
	return s.transmit(conn, dev)
}

// handshake performs the fixed-newstyle greeting and option haggling, returning
// the Device for the requested export once the client asks to go. A nil Device
// with a nil error means the client aborted cleanly.
func (s *Server) handshake(ctx context.Context, conn net.Conn) (Device, func(), string, error) {
	if err := writeAll(conn,
		be64(magicNBD), be64(magicIHAVE), be16(flagFixedNewstyle|flagNoZeroes),
	); err != nil {
		return nil, nil, "", err
	}
	var clientFlags uint32
	if err := binary.Read(conn, binary.BigEndian, &clientFlags); err != nil {
		return nil, nil, "", fmt.Errorf("nbd: reading client flags (%w)", err)
	}

	for {
		magic, opt, data, err := readOption(conn)
		if err != nil {
			return nil, nil, "", err
		}
		if magic != magicIHAVE {
			return nil, nil, "", fmt.Errorf("nbd: bad option magic %#x", magic)
		}
		switch opt {
		case optAbort:
			_ = writeOptionReply(conn, opt, repAck, nil)
			return nil, nil, "", nil
		case optExportName:
			dev, release, err := s.open(ctx, string(data))
			if err != nil {
				// EXPORT_NAME has no error reply; the only recourse is to drop
				// the connection so the client retries or fails.
				return nil, nil, "", err
			}
			if err := s.finishExportName(conn, dev, clientFlags); err != nil {
				release()
				return nil, nil, "", err
			}
			return dev, release, string(data), nil
		case optGo:
			export, err := parseGoExport(data)
			if err != nil {
				return nil, nil, "", err
			}
			dev, release, err := s.open(ctx, export)
			if err != nil {
				_ = writeOptionReply(conn, opt, repErrUnsup, nil)
				continue
			}
			if err := s.finishGo(conn, dev); err != nil {
				release()
				return nil, nil, "", err
			}
			return dev, release, export, nil
		default:
			if err := writeOptionReply(conn, opt, repErrUnsup, nil); err != nil {
				return nil, nil, "", err
			}
		}
	}
}

func (s *Server) open(ctx context.Context, export string) (Device, func(), error) {
	dev, release, err := s.backend.Open(ctx, export)
	if err != nil {
		return nil, nil, fmt.Errorf("nbd: could not open export %q (%w)", export, err)
	}
	if release == nil {
		release = func() {}
	}
	return dev, release, nil
}

// finishExportName replies to NBD_OPT_EXPORT_NAME: size, transmission flags,
// and (unless the client accepted no zeroes) 124 bytes of padding.
func (s *Server) finishExportName(conn net.Conn, dev Device, clientFlags uint32) error {
	if err := writeAll(conn, be64(uint64(dev.Size())), be16(transmissionFlags())); err != nil { // #nosec G115 -- device size is non-negative
		return err
	}
	if clientFlags&cFlagNoZeroes == 0 {
		return writeAll(conn, make([]byte, 124))
	}
	return nil
}

// finishGo replies to NBD_OPT_GO with an NBD_INFO_EXPORT info reply followed by
// an ACK, after which transmission begins.
func (s *Server) finishGo(conn net.Conn, dev Device) error {
	info := append(be16(infoExport), append(be64(uint64(dev.Size())), be16(transmissionFlags())...)...) // #nosec G115 -- device size is non-negative
	if err := writeOptionReply(conn, optGo, repInfo, info); err != nil {
		return err
	}
	return writeOptionReply(conn, optGo, repAck, nil)
}

func transmissionFlags() uint16 {
	return tFlagHasFlags | tFlagSendFlush | tFlagSendTrim
}

// transmit runs the request/reply loop until the client disconnects or the
// connection is closed (which is how a shutdown unblocks the read).
//
// Requests are dispatched to a bounded worker pool so reads and writes overlap
// across the connection — the NBD spec lets the server reply out of order, and
// the kernel's nbd client correlates replies by handle. Reply writes go through
// a single mutex so a multi-byte reply (a read's header + payload) isn't
// interleaved with another worker's reply. cmdFlush drains the inflight set
// before replying so its ack means "every prior write is durable," and cmdDisc
// drains so we don't return while workers are mid-write.
func (s *Server) transmit(conn net.Conn, dev Device) error {
	cs := &connState{conn: conn, dev: dev, logger: s.logger, sem: make(chan struct{}, maxInflightPerConn)}
	readerErr := cs.readLoop()
	cs.inflight.Wait()
	if e := cs.firstWriteErr.Load(); e != nil {
		return *e
	}
	return readerErr
}

// connState carries the per-connection plumbing the reader loop and the worker
// goroutines share: the conn itself, a write mutex that serialises one full
// reply at a time, the inflight WaitGroup that drains on flush/disc/shutdown,
// and an atomic that records the first reply-write error so the reader can
// surface it on return (a worker's failed write means the client is gone, but
// that's still a connection-level error the caller wants to log).
type connState struct {
	conn          net.Conn
	dev           Device
	logger        *slog.Logger
	writeMu       sync.Mutex
	inflight      sync.WaitGroup
	sem           chan struct{}
	firstWriteErr atomic.Pointer[error]
}

func (cs *connState) readLoop() error {
	header := make([]byte, 28)
	for {
		if _, err := io.ReadFull(cs.conn, header); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("nbd: reading request (%w)", err)
		}
		if magic := binary.BigEndian.Uint32(header[0:4]); magic != magicRequest {
			return fmt.Errorf("nbd: bad request magic %#x", magic)
		}
		cmd := binary.BigEndian.Uint16(header[6:8])
		handle := binary.BigEndian.Uint64(header[8:16])
		offset := binary.BigEndian.Uint64(header[16:24])
		length := binary.BigEndian.Uint32(header[24:28])

		// Read the write payload synchronously on the reader loop — only this
		// goroutine ever reads from conn, so workers never race for input.
		var writePayload []byte
		if cmd == cmdWrite {
			if length > maxRequestBytes {
				// Drain the oversized payload so the wire stays framed, then
				// queue an errInval reply against the same handle.
				if _, err := io.CopyN(io.Discard, cs.conn, int64(length)); err != nil {
					return err
				}
				cs.schedule(func() { cs.reply(errInval, handle, nil) })
				continue
			}
			writePayload = make([]byte, length)
			if _, err := io.ReadFull(cs.conn, writePayload); err != nil {
				return fmt.Errorf("nbd: reading write payload (%w)", err)
			}
		}

		switch cmd {
		case cmdDisc:
			return nil
		case cmdFlush:
			// Drain in-flight before acking the flush so the client's
			// "everything prior is durable" contract holds even though earlier
			// writes ran on the worker pool.
			cs.inflight.Wait()
			cs.reply(0, handle, nil)
		case cmdTrim:
			// silo volumes are sparse already; treat discard as a successful no-op.
			cs.schedule(func() { cs.reply(0, handle, nil) })
		case cmdRead:
			cs.schedule(func() { cs.serveRead(handle, offset, length) })
		case cmdWrite:
			cs.schedule(func() { cs.serveWrite(handle, offset, length, writePayload) })
		default:
			cs.schedule(func() { cs.reply(errInval, handle, nil) })
		}
	}
}

// schedule fires fn on the worker pool. The semaphore acquire happens on the
// reader's goroutine so the loop blocks when the pool is full — that is the
// per-connection backpressure that keeps a hot client from outrunning the
// chunk store.
func (cs *connState) schedule(fn func()) {
	cs.sem <- struct{}{}
	cs.inflight.Add(1)
	go func() {
		defer cs.inflight.Done()
		defer func() { <-cs.sem }()
		fn()
	}()
}

// reply serializes one full NBD reply (header + optional data) under writeMu
// so a read's header + payload isn't interleaved with another worker's reply,
// and records the first write error for the reader loop to surface. A failed
// reply means the conn is broken; the reader's next ReadFull will hit the
// same condition and unwind.
func (cs *connState) reply(errCode uint32, handle uint64, data []byte) {
	cs.writeMu.Lock()
	err := writeReply(cs.conn, errCode, handle, data)
	cs.writeMu.Unlock()
	if err != nil {
		cs.firstWriteErr.CompareAndSwap(nil, &err)
	}
}

func (cs *connState) serveRead(handle, offset uint64, length uint32) {
	if length > maxRequestBytes || outOfBounds(cs.dev, offset, length) {
		cs.reply(errInval, handle, nil)
		return
	}
	buf := make([]byte, length)
	// A short read (n < length) with a nil error would otherwise ship the
	// untouched tail of buf as a successful reply. Treat it as an I/O error
	// so the client never sees fabricated zeros where bytes were expected.
	if n, err := cs.dev.ReadAt(buf, int64(offset)); err != nil || uint32(n) != length { // #nosec G115 -- offset bounds-checked; n is in [0,length]
		cs.logger.Warn("nbd read failed", "offset", offset, "length", length, "read", n, "error", err)
		cs.reply(errIO, handle, nil)
		return
	}
	cs.reply(0, handle, buf)
}

func (cs *connState) serveWrite(handle, offset uint64, length uint32, payload []byte) {
	if outOfBounds(cs.dev, offset, length) {
		cs.reply(errInval, handle, nil)
		return
	}
	if _, err := cs.dev.WriteAt(payload, int64(offset)); err != nil { // #nosec G115 -- offset bounds-checked
		cs.logger.Warn("nbd write failed", "offset", offset, "length", length, "error", err)
		cs.reply(errIO, handle, nil)
		return
	}
	cs.reply(0, handle, nil)
}

// outOfBounds reports whether a [offset, offset+length) request from the client
// falls outside the device — rejecting untrusted offsets that would overflow
// int64 or read/write past the advertised size.
func outOfBounds(dev Device, offset uint64, length uint32) bool {
	size := dev.Size()
	if size < 0 {
		return true
	}
	end := offset + uint64(length)
	return end < offset || end > uint64(size) // #nosec G115 -- size is non-negative
}

// --- wire helpers ------------------------------------------------------------

func readOption(conn net.Conn) (magic uint64, opt uint32, data []byte, err error) {
	head := make([]byte, 16)
	if _, err = io.ReadFull(conn, head); err != nil {
		return 0, 0, nil, fmt.Errorf("nbd: reading option header (%w)", err)
	}
	magic = binary.BigEndian.Uint64(head[0:8])
	opt = binary.BigEndian.Uint32(head[8:12])
	length := binary.BigEndian.Uint32(head[12:16])
	if length > maxRequestBytes {
		return 0, 0, nil, fmt.Errorf("nbd: option data length %d exceeds the cap", length)
	}
	data = make([]byte, length)
	if _, err = io.ReadFull(conn, data); err != nil {
		return 0, 0, nil, fmt.Errorf("nbd: reading option data (%w)", err)
	}
	return magic, opt, data, nil
}

func writeOptionReply(conn net.Conn, opt, rep uint32, payload []byte) error {
	return writeAll(conn,
		be64(magicRep), be32(opt), be32(rep), be32(uint32(len(payload))), payload, // #nosec G115 -- reply payloads are small and bounded
	)
}

func writeReply(conn net.Conn, errCode uint32, handle uint64, data []byte) error {
	return writeAll(conn, be32(magicReply), be32(errCode), be64(handle), data)
}

// parseGoExport reads the export name from an NBD_OPT_GO/INFO payload:
// a 32-bit name length, the name, then info requests we ignore.
func parseGoExport(data []byte) (string, error) {
	if len(data) < 4 {
		return "", errors.New("nbd: GO option too short")
	}
	n := binary.BigEndian.Uint32(data[0:4])
	if int(n) > len(data)-4 {
		return "", errors.New("nbd: GO export name length out of range")
	}
	return string(data[4 : 4+n]), nil
}

func writeAll(conn net.Conn, parts ...[]byte) error {
	for _, p := range parts {
		if len(p) == 0 {
			continue
		}
		if _, err := conn.Write(p); err != nil {
			return fmt.Errorf("nbd: write failed (%w)", err)
		}
	}
	return nil
}

func be16(v uint16) []byte { return binary.BigEndian.AppendUint16(nil, v) }
func be32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
func be64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
