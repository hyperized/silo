package fuse

import "testing"

// FuzzDecodeRequest hardens every FUSE request decoder. The bytes come straight
// off the /dev/fuse kernel channel, and each decoder slices fixed offsets after
// a length guard — a wrong guard is a slice out-of-bounds panic that would take
// down the FUSE server. Feeding arbitrary bytes to the header decoder and every
// body decoder must always error-or-return, never panic.
func FuzzDecodeRequest(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 40))
	f.Add(make([]byte, 56))
	f.Add([]byte{'n', 'a', 'm', 'e', 0})

	f.Fuzz(func(_ *testing.T, b []byte) {
		_, _ = DecodeInHeader(b)
		_, _ = decodeInitIn(b)
		_, _ = decodeReadIn(b)
		_, _ = decodeWriteIn(b)
		_, _ = decodeOpenIn(b)
		_, _ = decodeReleaseIn(b)
		_, _ = decodeMkdirIn(b)
		_, _ = decodeCreateIn(b)
		_, _ = decodeForgetIn(b)
		_, _ = decodeSetattrIn(b)
		_, _ = cstr(b)
	})
}
