//go:build linux

package csi

import (
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// probeDeviceLiveness reads from an NBD device to learn the state of its
// link. It exists for devices without a socket watch: their connection may
// have died unnoticed, and the kernel's status flag only reports
// configuration, not link health. A direct read forces a real round-trip to
// silod (the page cache would answer for a dead link). On a dead link the
// read queues inside the kernel, so hitting the deadline means probeStalled;
// once the kernel's reconnect window has expired it fails reads at once,
// which is probeFailed.
//
// The goroutine behind a timed-out probe stays in the read until the link is
// repaired or the kernel fails the queued I/O, which the reconnect window
// bounds, and the read completes harmlessly then.
var probeDeviceLiveness = func(device string, timeout time.Duration) probeVerdict {
	done := make(chan probeVerdict, 1)
	go func() {
		fd, err := unix.Open(device, unix.O_RDONLY|unix.O_DIRECT|unix.O_CLOEXEC, 0)
		if err != nil {
			done <- probeFailed
			return
		}
		defer func() { _ = unix.Close(fd) }()
		buf := alignedBlock(4096)
		_, err = unix.Pread(fd, buf, 0)
		done <- readVerdict(err)
	}()
	select {
	case v := <-done:
		return v
	case <-time.After(timeout):
		return probeStalled
	}
}

func readVerdict(err error) probeVerdict {
	if err != nil {
		return probeFailed
	}
	return probeAlive
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
