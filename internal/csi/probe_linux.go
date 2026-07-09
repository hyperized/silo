//go:build linux

package csi

import (
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// probeDeviceLiveness reports whether an NBD device currently answers reads.
// It exists for adopted attachments: their connection may have died while no
// supervisor was running, and the kernel's status flag only reports
// configuration, not link health. A direct read forces a real round-trip to
// silod (the page cache would answer for a dead link); on a dead link the
// read queues inside the kernel, so the deadline is the verdict.
//
// The goroutine behind a timed-out probe stays in the read until the link is
// repaired or the kernel fails the queued I/O — bounded by the reconnect
// window, and the read completes harmlessly then.
var probeDeviceLiveness = func(device string, timeout time.Duration) bool {
	done := make(chan bool, 1)
	go func() {
		fd, err := unix.Open(device, unix.O_RDONLY|unix.O_DIRECT|unix.O_CLOEXEC, 0)
		if err != nil {
			done <- false
			return
		}
		defer func() { _ = unix.Close(fd) }()
		buf := alignedBlock(4096)
		_, err = unix.Pread(fd, buf, 0)
		done <- err == nil
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(timeout):
		return false
	}
}

// alignedBlock returns a size-byte buffer aligned to 512 bytes, the alignment
// O_DIRECT requires. The unsafe.Pointer is only read for its address so the
// slice can be offset to an aligned start; nothing is dereferenced.
func alignedBlock(size int) []byte {
	raw := make([]byte, size+512)
	off := int(uintptr(unsafe.Pointer(&raw[0])) & 511) // #nosec G103 -- address arithmetic for O_DIRECT alignment only
	if off != 0 {
		off = 512 - off
	}
	return raw[off : off+size]
}
