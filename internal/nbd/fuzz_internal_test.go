package nbd

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// fuzzConn adapts a byte slice to the read side of a net.Conn so the wire
// decoders can be driven from fuzzed input. Writes are discarded.
type fuzzConn struct{ r *bytes.Reader }

func (c fuzzConn) Read(p []byte) (int, error)     { return c.r.Read(p) }
func (fuzzConn) Write(p []byte) (int, error)      { return len(p), nil }
func (fuzzConn) Close() error                     { return nil }
func (fuzzConn) LocalAddr() net.Addr              { return nil }
func (fuzzConn) RemoteAddr() net.Addr             { return nil }
func (fuzzConn) SetDeadline(time.Time) error      { return nil }
func (fuzzConn) SetReadDeadline(time.Time) error  { return nil }
func (fuzzConn) SetWriteDeadline(time.Time) error { return nil }

// fuzzDevice is a fixed-size device for exercising outOfBounds.
type fuzzDevice int64

func (d fuzzDevice) Size() int64                      { return int64(d) }
func (fuzzDevice) ReadAt([]byte, int64) (int, error)  { return 0, nil }
func (fuzzDevice) WriteAt([]byte, int64) (int, error) { return 0, nil }

// FuzzReadOption hardens the NBD option-haggling decoder, which reads a 16-byte
// header and a length-prefixed body straight off an untrusted client socket.
// Truncated and oversized inputs must error, never panic or over-allocate. The
// allocation cap is lowered so the fuzzer can't make us reserve large buffers.
func FuzzReadOption(f *testing.F) {
	old := maxRequestBytes
	maxRequestBytes = 4096
	f.Cleanup(func() { maxRequestBytes = old })

	f.Add([]byte{})
	f.Add(make([]byte, 16))
	f.Add([]byte{0x49, 0x48, 0x41, 0x56, 0x45, 0x4f, 0x50, 0x54, 0, 0, 0, 7, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _, _, _ = readOption(fuzzConn{r: bytes.NewReader(data)})
	})
}

// FuzzParseGoExport hardens the NBD_OPT_GO export-name decoder, which slices a
// length-prefixed name out of an untrusted option payload.
func FuzzParseGoExport(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 3, 'v', 'o', 'l'})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0, 0, 0, 10, 'v', 'o', 'l'}) // length past the buffer

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = parseGoExport(data)
	})
}

// FuzzOutOfBounds checks the [offset, offset+length) bounds math that gates
// every read and write: whenever it reports a request in-bounds, the range must
// actually fit within a non-negative device size without unsigned wraparound.
func FuzzOutOfBounds(f *testing.F) {
	f.Add(uint64(0), uint32(0), int64(512))
	f.Add(^uint64(0), uint32(1), int64(512))
	f.Add(uint64(510), uint32(8), int64(512))

	f.Fuzz(func(t *testing.T, offset uint64, length uint32, size int64) {
		if outOfBounds(fuzzDevice(size), offset, length) {
			return
		}
		end := offset + uint64(length)
		if size < 0 || end < offset || end > uint64(size) {
			t.Fatalf("outOfBounds reported in-bounds for offset=%d length=%d size=%d (overflow or past end)", offset, length, size)
		}
	})
}
