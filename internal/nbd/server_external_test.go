package nbd_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/nbd"
)

// --- a fake device + backend -------------------------------------------------

type memDevice struct {
	mu       sync.Mutex
	data     []byte
	readErr  error
	writeErr error
}

func (d *memDevice) Size() int64 { return int64(len(d.data)) }

func (d *memDevice) ReadAt(p []byte, off int64) (int, error) {
	if d.readErr != nil {
		return 0, d.readErr
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return copy(p, d.data[off:]), nil
}

func (d *memDevice) WriteAt(p []byte, off int64) (int, error) {
	if d.writeErr != nil {
		return 0, d.writeErr
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return copy(d.data[off:], p), nil
}

type backend struct {
	dev      *memDevice
	openErr  error
	released atomic.Bool
}

func (b *backend) Open(_ context.Context, _ string) (nbd.Device, func(), error) {
	if b.openErr != nil {
		return nil, nil, b.openErr
	}
	return b.dev, func() { b.released.Store(true) }, nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// serveConn runs a server over one end of a pipe and returns the client end and
// a channel carrying ServeConn's result.
func serveConn(t *testing.T, b nbd.Backend) (net.Conn, <-chan error) {
	t.Helper()
	srv := nbd.NewServer(b, discardLogger())
	cli, ser := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- srv.ServeConn(context.Background(), ser) }()
	t.Cleanup(func() { _ = cli.Close() })
	return cli, done
}

// --- a minimal NBD client over the pipe --------------------------------------

const (
	magicIHAVE    = 0x49484156454f5054
	magicRep      = 0x0003e889045565a9
	magicRequest  = 0x25609513
	magicReply    = 0x67446698
	cFlagNoZeroes = 1 << 1
)

type testClient struct {
	t    *testing.T
	conn net.Conn
}

func (c *testClient) greet(clientFlags uint32) {
	c.t.Helper()
	head := make([]byte, 18)
	c.must(io.ReadFull(c.conn, head))
	if binary.BigEndian.Uint64(head[8:16]) != magicIHAVE {
		c.t.Fatalf("bad IHAVEOPT magic in greeting")
	}
	c.write(be32(clientFlags))
}

func (c *testClient) sendOption(opt uint32, data []byte) {
	c.t.Helper()
	c.write(be64(magicIHAVE))
	c.write(be32(opt))
	c.write(be32(uint32(len(data))))
	if len(data) > 0 {
		c.write(data)
	}
}

// readOptionReply returns (rep code, payload).
func (c *testClient) readOptionReply() (uint32, []byte) {
	c.t.Helper()
	head := make([]byte, 20)
	c.must(io.ReadFull(c.conn, head))
	if binary.BigEndian.Uint64(head[0:8]) != magicRep {
		c.t.Fatalf("bad option reply magic")
	}
	rep := binary.BigEndian.Uint32(head[12:16])
	n := binary.BigEndian.Uint32(head[16:20])
	payload := make([]byte, n)
	c.must(io.ReadFull(c.conn, payload))
	return rep, payload
}

// exportName negotiates NBD_OPT_EXPORT_NAME and returns the advertised size.
func (c *testClient) exportName(name string, noZeroes bool) uint64 {
	c.t.Helper()
	c.sendOption(1, []byte(name))
	head := make([]byte, 10)
	c.must(io.ReadFull(c.conn, head))
	size := binary.BigEndian.Uint64(head[0:8])
	if !noZeroes {
		c.must(io.ReadFull(c.conn, make([]byte, 124)))
	}
	return size
}

// goExport negotiates NBD_OPT_GO and returns the advertised size.
func (c *testClient) goExport(name string) uint64 {
	c.t.Helper()
	payload := append(be32(uint32(len(name))), []byte(name)...)
	payload = append(payload, be16(0)...) // zero info requests
	c.sendOption(7, payload)
	var size uint64
	for {
		rep, info := c.readOptionReply()
		if rep == 3 { // NBD_REP_INFO
			size = binary.BigEndian.Uint64(info[2:10])
			continue
		}
		if rep == 1 { // NBD_REP_ACK
			return size
		}
		c.t.Fatalf("unexpected GO reply %#x", rep)
	}
}

// request issues a transmission command and returns (errCode, readData).
func (c *testClient) request(cmd uint16, offset uint64, length uint32, payload []byte) (uint32, []byte) {
	c.t.Helper()
	h := be32(magicRequest)
	h = append(h, be16(0)...)      // command flags
	h = append(h, be16(cmd)...)    // type
	h = append(h, be64(1)...)      // handle
	h = append(h, be64(offset)...) // offset
	h = append(h, be32(length)...) // length
	c.write(h)
	if len(payload) > 0 {
		c.write(payload)
	}
	rh := make([]byte, 16)
	c.must(io.ReadFull(c.conn, rh))
	if binary.BigEndian.Uint32(rh[0:4]) != magicReply {
		c.t.Fatalf("bad reply magic")
	}
	errCode := binary.BigEndian.Uint32(rh[4:8])
	var data []byte
	if cmd == 0 && errCode == 0 { // READ
		data = make([]byte, length)
		c.must(io.ReadFull(c.conn, data))
	}
	return errCode, data
}

// disconnect sends NBD_CMD_DISC, which the server answers by closing the
// connection rather than replying — so, unlike request, it reads nothing back.
func (c *testClient) disconnect() {
	c.t.Helper()
	h := be32(magicRequest)
	h = append(h, be16(0)...)
	h = append(h, be16(2)...) // NBD_CMD_DISC
	h = append(h, be64(1)...)
	h = append(h, be64(0)...)
	h = append(h, be32(0)...)
	c.write(h)
}

func (c *testClient) write(b []byte) {
	c.t.Helper()
	if _, err := c.conn.Write(b); err != nil {
		c.t.Fatalf("client write: %v", err)
	}
}

func (c *testClient) must(_ int, err error) {
	c.t.Helper()
	if err != nil {
		c.t.Fatalf("client read: %v", err)
	}
}

func be16(v uint16) []byte { return binary.BigEndian.AppendUint16(nil, v) }
func be32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
func be64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

// --- tests -------------------------------------------------------------------

func TestServer_ExportNameReadWriteFlow(t *testing.T) {
	b := &backend{dev: &memDevice{data: make([]byte, 4096)}}
	conn, done := serveConn(t, b)
	c := &testClient{t: t, conn: conn}

	c.greet(cFlagNoZeroes)
	if size := c.exportName("vol", true); size != 4096 {
		t.Fatalf("export size = %d, want 4096", size)
	}

	if errc, _ := c.request(1, 0, 5, []byte("hello")); errc != 0 { // WRITE
		t.Fatalf("write err = %d", errc)
	}
	if errc, data := c.request(0, 0, 5, nil); errc != 0 || string(data) != "hello" { // READ
		t.Fatalf("read = (%d,%q), want (0,hello)", errc, data)
	}
	if errc, _ := c.request(3, 0, 0, nil); errc != 0 { // FLUSH
		t.Errorf("flush err = %d", errc)
	}
	if errc, _ := c.request(4, 0, 5, nil); errc != 0 { // TRIM
		t.Errorf("trim err = %d", errc)
	}
	c.disconnect()

	if err := <-done; err != nil {
		t.Errorf("ServeConn: %v", err)
	}
	if !b.released.Load() {
		t.Error("release was not called on disconnect")
	}
}

func TestServer_GoNegotiation(t *testing.T) {
	b := &backend{dev: &memDevice{data: make([]byte, 8192)}}
	conn, done := serveConn(t, b)
	c := &testClient{t: t, conn: conn}

	c.greet(cFlagNoZeroes)
	if size := c.goExport("vol"); size != 8192 {
		t.Fatalf("GO size = %d, want 8192", size)
	}
	if errc, _ := c.request(1, 16, 3, []byte("abc")); errc != 0 {
		t.Fatalf("write err = %d", errc)
	}
	if _, data := c.request(0, 16, 3, nil); string(data) != "abc" {
		t.Errorf("read = %q, want abc", data)
	}
	c.disconnect()
	if err := <-done; err != nil {
		t.Errorf("ServeConn: %v", err)
	}
}

func TestServer_ExportNameWithZeroPadding(t *testing.T) {
	b := &backend{dev: &memDevice{data: make([]byte, 512)}}
	conn, done := serveConn(t, b)
	c := &testClient{t: t, conn: conn}

	c.greet(0) // no NO_ZEROES -> server sends 124 pad bytes
	if size := c.exportName("vol", false); size != 512 {
		t.Fatalf("size = %d, want 512", size)
	}
	c.disconnect()
	<-done
}

func TestServer_AbortOption(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.sendOption(2, nil) // NBD_OPT_ABORT
	if rep, _ := c.readOptionReply(); rep != 1 {
		t.Errorf("abort reply = %#x, want ACK", rep)
	}
	if err := <-done; err != nil {
		t.Errorf("ServeConn after abort: %v", err)
	}
}

func TestServer_UnsupportedOption(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.sendOption(3, nil) // NBD_OPT_LIST -> unsupported
	if rep, _ := c.readOptionReply(); rep != (0x80000000 | 1) {
		t.Errorf("unsupported reply = %#x, want ERR_UNSUP", rep)
	}
	c.sendOption(2, nil)
	c.readOptionReply()
	<-done
}

func TestServer_ReadWriteErrors(t *testing.T) {
	dev := &memDevice{data: make([]byte, 4096)}
	dev.readErr = errors.New("read boom")
	dev.writeErr = errors.New("write boom")
	conn, done := serveConn(t, &backend{dev: dev})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)

	if errc, _ := c.request(0, 0, 4, nil); errc != 5 { // READ -> EIO
		t.Errorf("read err = %d, want 5 (EIO)", errc)
	}
	if errc, _ := c.request(1, 0, 4, []byte("data")); errc != 5 { // WRITE -> EIO
		t.Errorf("write err = %d, want 5 (EIO)", errc)
	}
	c.disconnect()
	<-done
}

func TestServer_OversizeAndUnknownCommands(t *testing.T) {
	t.Cleanup(nbd.SetMaxRequestBytesForTest(8))
	conn, done := serveConn(t, &backend{dev: &memDevice{data: make([]byte, 4096)}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)

	if errc, _ := c.request(0, 0, 9, nil); errc != 22 { // READ oversize -> EINVAL
		t.Errorf("oversize read err = %d, want 22", errc)
	}
	if errc, _ := c.request(1, 0, 9, make([]byte, 9)); errc != 22 { // WRITE oversize -> EINVAL (drained)
		t.Errorf("oversize write err = %d, want 22", errc)
	}
	if errc, _ := c.request(99, 0, 0, nil); errc != 22 { // unknown command -> EINVAL
		t.Errorf("unknown cmd err = %d, want 22", errc)
	}
	c.disconnect()
	<-done
}

func TestServer_ExportNameOpenErrorDropsConn(t *testing.T) {
	conn, done := serveConn(t, &backend{openErr: errors.New("no such volume")})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.sendOption(1, []byte("ghost")) // EXPORT_NAME with a failing backend
	if err := <-done; err == nil {
		t.Error("ServeConn should error when EXPORT_NAME open fails")
	}
}

func TestServer_GoOpenErrorReplies(t *testing.T) {
	conn, done := serveConn(t, &backend{openErr: errors.New("no such volume")})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.goExportExpectingError(t, "ghost")
	c.sendOption(2, nil) // abort
	c.readOptionReply()
	<-done
}

func (c *testClient) goExportExpectingError(t *testing.T, name string) {
	t.Helper()
	payload := append(be32(uint32(len(name))), []byte(name)...)
	payload = append(payload, be16(0)...)
	c.sendOption(7, payload)
	if rep, _ := c.readOptionReply(); rep != (0x80000000 | 1) {
		t.Errorf("GO open-error reply = %#x, want ERR_UNSUP", rep)
	}
}

func TestServer_BadRequestMagic(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{data: make([]byte, 512)}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)
	c.write(be32(0xdeadbeef)) // wrong request magic
	c.write(make([]byte, 24))
	if err := <-done; err == nil {
		t.Error("ServeConn should error on a bad request magic")
	}
}

func TestServer_BadOptionMagic(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.write(be64(0xbad)) // wrong option magic
	c.write(be32(1))
	c.write(be32(0))
	if err := <-done; err == nil {
		t.Error("ServeConn should error on a bad option magic")
	}
}

func TestServer_GoMalformed(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.sendOption(7, []byte{0, 0}) // GO payload shorter than the 4-byte name length
	if err := <-done; err == nil {
		t.Error("ServeConn should error on a malformed GO option")
	}
}

func TestServer_GoNameLengthOutOfRange(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.sendOption(7, append(be32(99), []byte("short")...)) // namelen 99 > data
	if err := <-done; err == nil {
		t.Error("ServeConn should error on an out-of-range GO name length")
	}
}

type negSizeDevice struct{}

func (negSizeDevice) Size() int64                        { return -1 }
func (negSizeDevice) ReadAt([]byte, int64) (int, error)  { return 0, nil }
func (negSizeDevice) WriteAt([]byte, int64) (int, error) { return 0, nil }

type negSizeBackend struct{}

func (negSizeBackend) Open(context.Context, string) (nbd.Device, func(), error) {
	return negSizeDevice{}, nil, nil
}

// shortReadDevice returns fewer bytes than requested with a nil error, the
// case that would otherwise let the server ship fabricated zeros as success.
type shortReadDevice struct{ size int64 }

func (d shortReadDevice) Size() int64 { return d.size }
func (shortReadDevice) ReadAt(p []byte, _ int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil // short read, no error
}
func (shortReadDevice) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }

type shortReadBackend struct{}

func (shortReadBackend) Open(context.Context, string) (nbd.Device, func(), error) {
	return shortReadDevice{size: 4096}, nil, nil
}

func TestServer_ShortReadIsIOError(t *testing.T) {
	conn, done := serveConn(t, shortReadBackend{})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)
	if errc, _ := c.request(0, 0, 8, nil); errc != 5 { // short read -> EIO, not zeros
		t.Errorf("short read err = %d, want 5 (EIO)", errc)
	}
	c.disconnect()
	<-done
}

func TestServer_ServeShutdownClosesInflightAndRejectsNew(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := nbd.NewServer(&backend{dev: &memDevice{data: make([]byte, 512)}}, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, ln) }()

	// conn1: complete the handshake so it is accepted, tracked, and parked in
	// the transmit loop reading the next request.
	conn1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial conn1: %v", err)
	}
	c1 := &testClient{t: t, conn: conn1}
	c1.greet(cFlagNoZeroes)
	c1.exportName("vol", true)

	// Cancelling must force the in-flight connection closed, not leave it
	// dangling until the client happens to disconnect.
	cancel()
	_ = conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn1.Read(make([]byte, 1)); err == nil {
		t.Error("expected the server to close the in-flight connection on shutdown")
	}
	_ = conn1.Close()

	// conn1's read returning establishes the close-all sweep has run (closing
	// is set), so a connection accepted now is rejected immediately.
	if conn2, err := net.Dial("tcp", ln.Addr().String()); err == nil {
		_ = conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn2.Read(make([]byte, 1)); err == nil {
			t.Error("expected a connection accepted during shutdown to be closed")
		}
		_ = conn2.Close()
	}

	_ = ln.Close()
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("Serve returned %v, want nil after shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after shutdown drain")
	}
}

func TestServer_OutOfBoundsRejected(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{data: make([]byte, 512)}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)

	if errc, _ := c.request(0, 512, 4, nil); errc != 22 { // READ past the end
		t.Errorf("read past end err = %d, want 22 (EINVAL)", errc)
	}
	if errc, _ := c.request(1, 510, 8, make([]byte, 8)); errc != 22 { // WRITE past the end
		t.Errorf("write past end err = %d, want 22 (EINVAL)", errc)
	}
	if errc, _ := c.request(0, 0xFFFFFFFFFFFFFFFF, 4, nil); errc != 22 { // offset+len overflow
		t.Errorf("overflow read err = %d, want 22 (EINVAL)", errc)
	}
	c.disconnect()
	<-done
}

func TestServer_NegativeDeviceSizeRejectsIO(t *testing.T) {
	conn, done := serveConn(t, negSizeBackend{})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)
	if errc, _ := c.request(0, 0, 4, nil); errc != 22 { // a negative-size device rejects all I/O
		t.Errorf("read on negative-size device err = %d, want 22 (EINVAL)", errc)
	}
	c.disconnect()
	<-done
}

func TestServer_NilLoggerAndNilRelease(t *testing.T) {
	// nil logger -> slog.Default; nil release from the backend -> a no-op.
	cli, ser := net.Pipe()
	srv := nbd.NewServer(&nilReleaseBackend{dev: &memDevice{data: make([]byte, 512)}}, nil)
	done := make(chan error, 1)
	go func() { done <- srv.ServeConn(context.Background(), ser) }()
	c := &testClient{t: t, conn: cli}
	t.Cleanup(func() { _ = cli.Close() })
	c.greet(cFlagNoZeroes)
	if size := c.exportName("vol", true); size != 512 {
		t.Fatalf("size = %d", size)
	}
	c.disconnect()
	if err := <-done; err != nil {
		t.Errorf("ServeConn: %v", err)
	}
}

type nilReleaseBackend struct{ dev *memDevice }

func (b *nilReleaseBackend) Open(context.Context, string) (nbd.Device, func(), error) {
	return b.dev, nil, nil // nil release must be tolerated
}

func TestServer_CleanCloseAfterHandshake(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{data: make([]byte, 512)}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)
	_ = conn.Close() // close without DISC -> server read sees a clean EOF
	if err := <-done; err != nil {
		t.Errorf("ServeConn on clean close = %v, want nil", err)
	}
}

func TestServer_PartialRequestHeaderErrors(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{data: make([]byte, 512)}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)
	c.write(make([]byte, 10)) // a partial 28-byte request header
	_ = conn.Close()
	if err := <-done; err == nil {
		t.Error("ServeConn should error on a truncated request header")
	}
}

func TestServer_GreetingWriteError(t *testing.T) {
	cli, ser := net.Pipe()
	srv := nbd.NewServer(&backend{dev: &memDevice{}}, discardLogger())
	done := make(chan error, 1)
	go func() { done <- srv.ServeConn(context.Background(), ser) }()
	_ = cli.Close() // never read the greeting -> the server's first write fails
	if err := <-done; err == nil {
		t.Error("ServeConn should error when the greeting cannot be written")
	}
}

func TestServer_ClientFlagsReadError(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{}})
	c := &testClient{t: t, conn: conn}
	c.must(io.ReadFull(conn, make([]byte, 18))) // read greeting, then close before flags
	_ = conn.Close()
	if err := <-done; err == nil {
		t.Error("ServeConn should error when client flags cannot be read")
	}
}

func TestServer_OptionReadError(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes) // greet, then close before sending any option
	_ = conn.Close()
	if err := <-done; err == nil {
		t.Error("ServeConn should error when an option cannot be read")
	}
}

func TestServer_OversizeOption(t *testing.T) {
	t.Cleanup(nbd.SetMaxRequestBytesForTest(8))
	conn, done := serveConn(t, &backend{dev: &memDevice{}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	// Option header claiming 9 data bytes (over the cap); send none, since the
	// server rejects on the length before reading the data.
	c.write(be64(magicIHAVE))
	c.write(be32(1))
	c.write(be32(9))
	if err := <-done; err == nil {
		t.Error("ServeConn should error on an over-cap option length")
	}
}

func TestServer_OptionDataReadError(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.write(be64(magicIHAVE))
	c.write(be32(1))
	c.write(be32(5))      // header claims 5 data bytes...
	c.write([]byte("ab")) // ...but only 2 arrive before close
	_ = conn.Close()
	if err := <-done; err == nil {
		t.Error("ServeConn should error when option data is truncated")
	}
}

func TestServer_WritePayloadReadError(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{data: make([]byte, 512)}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)
	h := be32(magicRequest)
	h = append(h, be16(0)...)
	h = append(h, be16(1)...) // WRITE
	h = append(h, be64(1)...)
	h = append(h, be64(0)...)
	h = append(h, be32(5)...) // claims 5 payload bytes
	c.write(h)
	c.write([]byte("ab")) // only 2 before close
	_ = conn.Close()
	if err := <-done; err == nil {
		t.Error("ServeConn should error on a truncated write payload")
	}
}

func TestServer_ReplyWriteErrorOnRead(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{data: make([]byte, 512)}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)
	// Send a READ but close before reading the reply -> the server's reply
	// write fails.
	h := be32(magicRequest)
	h = append(h, be16(0)...)
	h = append(h, be16(0)...) // READ
	h = append(h, be64(1)...)
	h = append(h, be64(0)...)
	h = append(h, be32(4)...)
	c.write(h)
	_ = conn.Close()
	if err := <-done; err == nil {
		t.Error("ServeConn should error when a read reply cannot be written")
	}
}

func TestServer_ReplyWriteErrors(t *testing.T) {
	// Closing right after sending an option, before reading the reply, makes
	// the server's reply write fail (net.Pipe blocks the write until a read).
	t.Run("export name", func(t *testing.T) {
		conn, done := serveConn(t, &backend{dev: &memDevice{data: make([]byte, 512)}})
		c := &testClient{t: t, conn: conn}
		c.greet(cFlagNoZeroes)
		c.sendOption(1, []byte("vol"))
		_ = conn.Close()
		if err := <-done; err == nil {
			t.Error("ServeConn should error when the EXPORT_NAME reply cannot be written")
		}
	})
	t.Run("go", func(t *testing.T) {
		conn, done := serveConn(t, &backend{dev: &memDevice{data: make([]byte, 512)}})
		c := &testClient{t: t, conn: conn}
		c.greet(cFlagNoZeroes)
		payload := append(be32(3), []byte("vol")...)
		payload = append(payload, be16(0)...)
		c.sendOption(7, payload)
		_ = conn.Close()
		if err := <-done; err == nil {
			t.Error("ServeConn should error when the GO reply cannot be written")
		}
	})
}

func TestServer_UnsupportedReplyWriteError(t *testing.T) {
	conn, done := serveConn(t, &backend{dev: &memDevice{}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.sendOption(3, nil) // NBD_OPT_LIST -> unsupported reply
	_ = conn.Close()     // close before reading it, so the reply write fails
	if err := <-done; err == nil {
		t.Error("ServeConn should error when an unsupported reply cannot be written")
	}
}

func TestServer_OversizeWriteDrainError(t *testing.T) {
	t.Cleanup(nbd.SetMaxRequestBytesForTest(8))
	conn, done := serveConn(t, &backend{dev: &memDevice{data: make([]byte, 512)}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)
	h := be32(magicRequest)
	h = append(h, be16(0)...)
	h = append(h, be16(1)...) // WRITE
	h = append(h, be64(1)...)
	h = append(h, be64(0)...)
	h = append(h, be32(9)...) // over the cap of 8 -> server drains, but...
	c.write(h)
	c.write([]byte("abc")) // ...only 3 of 9 bytes arrive before close
	_ = conn.Close()
	if err := <-done; err == nil {
		t.Error("ServeConn should error when draining an oversize write fails")
	}
}

// signalHandler signals on the first log record, letting a test wait for the
// Serve goroutine's error log deterministically.
type signalHandler struct{ ch chan struct{} }

func (signalHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h signalHandler) Handle(context.Context, slog.Record) error {
	select {
	case h.ch <- struct{}{}:
	default:
	}
	return nil
}
func (h signalHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h signalHandler) WithGroup(string) slog.Handler      { return h }

func TestServer_ServeLogsConnectionError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	logged := make(chan struct{}, 1)
	srv := nbd.NewServer(&backend{dev: &memDevice{}}, slog.New(signalHandler{ch: logged}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.Serve(ctx, ln) }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.Close() // close before the handshake completes -> ServeConn errors -> logged

	select {
	case <-logged:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not log the connection error")
	}
}

func TestServer_ServeAcceptsAndStops(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := nbd.NewServer(&backend{dev: &memDevice{data: make([]byte, 512)}}, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, ln) }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)
	c.disconnect()
	_ = conn.Close()

	cancel()
	_ = ln.Close()
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("Serve returned %v, want nil after ctx cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after ctx cancel + listener close")
	}
}

// TestServer_ConcurrentRequestsAreCorrelatedByHandle proves the parallel
// transmit loop. A reader and a writer fire many in-flight requests with
// distinct handles, never waiting between them, then read all the replies in
// whatever order the server happens to emit them. The test passes if every
// reply matches the request that carried the same handle — that is, the
// server can serve requests out of order and still attribute each reply
// correctly.
//
// We make the device slow so the worker pool actually overlaps the requests:
// without the artificial latency the server would finish each request before
// the next header arrives over net.Pipe, and the test would silently pass on
// a sequential implementation too.
func TestServer_ConcurrentRequestsAreCorrelatedByHandle(t *testing.T) {
	dev := &memDevice{data: make([]byte, 4096)}
	conn, done := serveConn(t, &slowBackend{dev: &slowDevice{Device: dev, perOp: 20 * time.Millisecond}})
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.exportName("vol", true)

	// Seed every distinct extent we'll read so the data is stable.
	for i := 0; i < 8; i++ {
		payload := []byte{byte('A' + i)}
		c.write(buildRequest(1, uint64(i)*8, 1, uint64(100+i)))
		c.write(payload)
	}
	// Drain the 8 write replies (order does not matter; writes carry no data).
	for i := 0; i < 8; i++ {
		c.readOneReply(0)
	}

	// Fire 16 reads back-to-back without reading any reply in between, so the
	// server has them all in flight at once.
	const reads = 16
	for i := 0; i < reads; i++ {
		c.write(buildRequest(0, uint64(i%8)*8, 1, uint64(1000+i)))
	}
	// Match each reply to its request by handle; require every handle to come
	// back exactly once.
	pending := map[uint64]byte{}
	for i := 0; i < reads; i++ {
		pending[uint64(1000+i)] = byte('A' + (i % 8))
	}
	for i := 0; i < reads; i++ {
		rep := c.readOneReply(1)
		want, ok := pending[rep.handle]
		if !ok {
			t.Fatalf("reply for unknown handle %d", rep.handle)
		}
		delete(pending, rep.handle)
		if rep.errCode != 0 {
			t.Fatalf("read handle %d errored: %d", rep.handle, rep.errCode)
		}
		if len(rep.data) != 1 || rep.data[0] != want {
			t.Fatalf("read handle %d data = %v (%c), want %c", rep.handle, rep.data, rep.data[0], want)
		}
	}
	if len(pending) > 0 {
		t.Fatalf("missing replies for handles %v", pending)
	}
	c.disconnect()
	if err := <-done; err != nil {
		t.Errorf("ServeConn: %v", err)
	}
}

// buildRequest assembles one NBD transmission request header for the given
// command/offset/length/handle. The test uses this directly so it can stage
// many requests on the wire without reading replies between them.
func buildRequest(cmd uint16, offset uint64, length uint32, handle uint64) []byte {
	h := be32(magicRequest)
	h = append(h, be16(0)...)
	h = append(h, be16(cmd)...)
	h = append(h, be64(handle)...)
	h = append(h, be64(offset)...)
	h = append(h, be32(length)...)
	return h
}

// rep is a parsed NBD reply: error code, the handle the server echoed back,
// and the payload (empty for everything except a successful read).
type rep struct {
	errCode uint32
	handle  uint64
	data    []byte
}

// readOneReply parses a transmission-phase reply: 16-byte header (magic,
// errCode, handle) followed by dataLen bytes of payload when the reply is
// for a successful read. The caller passes dataLen because the reply
// header does not carry it — that's the request's `length` field.
func (c *testClient) readOneReply(dataLen int) rep {
	c.t.Helper()
	head := make([]byte, 16)
	c.must(io.ReadFull(c.conn, head))
	if binary.BigEndian.Uint32(head[0:4]) != magicReply {
		c.t.Fatalf("bad reply magic")
	}
	r := rep{
		errCode: binary.BigEndian.Uint32(head[4:8]),
		handle:  binary.BigEndian.Uint64(head[8:16]),
	}
	if r.errCode == 0 && dataLen > 0 {
		r.data = make([]byte, dataLen)
		c.must(io.ReadFull(c.conn, r.data))
	}
	return r
}

// slowDevice wraps a Device and sleeps perOp before each ReadAt / WriteAt so
// concurrent requests overlap inside the server's worker pool. Without this
// the net.Pipe + memDevice path completes each request before the next
// header arrives, and the test can't distinguish parallel from sequential.
type slowDevice struct {
	nbd.Device
	perOp time.Duration
}

func (d *slowDevice) ReadAt(p []byte, off int64) (int, error) {
	time.Sleep(d.perOp)
	return d.Device.ReadAt(p, off)
}

func (d *slowDevice) WriteAt(p []byte, off int64) (int, error) {
	time.Sleep(d.perOp)
	return d.Device.WriteAt(p, off)
}

// slowBackend is the equivalent of backend but keyed on the nbd.Device
// interface so the concurrent-handles test can hand it a slowDevice wrapper.
// Kept narrow on purpose — the existing tests stay on the concrete backend
// type so a refactor of memDevice can't cascade unexpectedly into them.
type slowBackend struct {
	dev      nbd.Device
	released atomic.Bool
}

func (b *slowBackend) Open(_ context.Context, _ string) (nbd.Device, func(), error) {
	return b.dev, func() { b.released.Store(true) }, nil
}

func TestServer_ServeReturnsErrorOnListenerClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := nbd.NewServer(&backend{dev: &memDevice{}}, discardLogger())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(context.Background(), ln) }() // ctx never cancelled
	_ = ln.Close()                                                // accept now fails
	select {
	case err := <-served:
		if err == nil {
			t.Error("Serve should return an error when the listener closes without ctx cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after listener close")
	}
}

// blockingDevice parks ReadAt until released, so a test can hold a request
// in flight across a shutdown and prove what happens to its reply.
type blockingDevice struct {
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
}

func (d *blockingDevice) Size() int64 { return 4096 }

func (d *blockingDevice) ReadAt(p []byte, _ int64) (int, error) {
	d.enterOnce.Do(func() { close(d.entered) })
	<-d.release
	for i := range p {
		p[i] = 0xab
	}
	return len(p), nil
}

func (d *blockingDevice) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }

// drainClient attaches over a real listener and leaves one read in flight on
// the blocked device, returning the raw conn for reply inspection.
func drainClient(t *testing.T, addr string, dev *blockingDevice) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &testClient{t: t, conn: conn}
	c.greet(cFlagNoZeroes)
	c.goExport("vol")

	// One read request, sent raw so the reply can be read after the drain.
	h := be32(magicRequest)
	h = append(h, be16(0)...) // command flags
	h = append(h, be16(0)...) // NBD_CMD_READ
	h = append(h, be64(7)...) // handle
	h = append(h, be64(0)...) // offset
	h = append(h, be32(8)...) // length
	c.write(h)
	<-dev.entered
	return conn
}

// TestServer_ShutdownDrainsInFlightReplies is the rollout contract: a request
// that silod already accepted when the shutdown starts still gets its reply
// before the connection closes, so the client never has to guess whether an
// acknowledged write happened.
func TestServer_ShutdownDrainsInFlightReplies(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dev := &blockingDevice{entered: make(chan struct{}), release: make(chan struct{})}
	srv := nbd.NewServer(&blockingBackend{dev: dev}, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, ln) }()

	conn := drainClient(t, ln.Addr().String(), dev)

	cancel()
	_ = ln.Close()
	// Give the drain a moment to interrupt the reader, then let the device
	// finish — the reply must still arrive on the half-drained connection.
	time.Sleep(50 * time.Millisecond)
	close(dev.release)

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	rh := make([]byte, 16)
	if _, err := io.ReadFull(conn, rh); err != nil {
		t.Fatalf("the drain dropped the in-flight reply: %v", err)
	}
	if binary.BigEndian.Uint32(rh[0:4]) != magicReply || binary.BigEndian.Uint32(rh[4:8]) != 0 {
		t.Fatalf("bad drained reply header: % x", rh)
	}
	if binary.BigEndian.Uint64(rh[8:16]) != 7 {
		t.Fatalf("drained reply handle = %d, want 7", binary.BigEndian.Uint64(rh[8:16]))
	}
	data := make([]byte, 8)
	if _, err := io.ReadFull(conn, data); err != nil {
		t.Fatalf("drained reply payload: %v", err)
	}
	// After the drain the server closes the connection.
	if _, err := io.ReadFull(conn, rh[:1]); err == nil {
		t.Fatal("the connection should close once the drain completes")
	}

	select {
	case err := <-served:
		if err != nil {
			t.Errorf("Serve returned %v, want nil after a drained shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after the drain")
	}
}

// TestServer_ShutdownGraceCutsStuckConnections bounds the drain: a device
// operation that never finishes must not hold the shutdown hostage — the
// connection is cut once the grace expires.
func TestServer_ShutdownGraceCutsStuckConnections(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dev := &blockingDevice{entered: make(chan struct{}), release: make(chan struct{})}
	srv := nbd.NewServer(&blockingBackend{dev: dev}, discardLogger(), nbd.WithDrainGrace(100*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, ln) }()

	conn := drainClient(t, ln.Addr().String(), dev)

	cancel()
	_ = ln.Close()

	// The device stays stuck past the grace, so the client's connection must
	// be cut rather than kept open indefinitely.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err == nil {
		t.Fatal("the grace period should cut a stuck connection")
	} else if errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("unexpected read result: %v", err)
	}

	// Only after the device finally releases can Serve finish its wait.
	close(dev.release)
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("Serve returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after the stuck device released")
	}
}

// blockingBackend serves the blockingDevice.
type blockingBackend struct{ dev *blockingDevice }

func (b *blockingBackend) Open(context.Context, string) (nbd.Device, func(), error) {
	return b.dev, func() {}, nil
}
