//go:build linux

package fuse

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// This file is the OS boundary: opening /dev/fuse and the mount(2) syscall that
// hands the kernel one end of the connection. It is the one part of the library
// that needs a real kernel and mount privileges, so it cannot be exercised in a
// unit test (there is no /dev/fuse in CI) — the protocol above is tested over an
// in-memory Conn instead. Keep this layer thin so the untested surface stays
// small.

// devBufSize is the read buffer for one request: the negotiated max write plus
// headroom for the request headers. The kernel returns one whole message per
// read, so the buffer must be at least this large.
const devBufSize = DefaultMaxWrite + 4096

// DevConn is a Conn backed by /dev/fuse for a live mount.
type DevConn struct {
	f          *os.File
	mountpoint string
}

// Mount opens /dev/fuse and mounts a FUSE filesystem at mountpoint, returning a
// Conn a Session serves. The caller unmounts by closing the returned DevConn.
// mountpoint must be an existing directory.
func Mount(mountpoint string) (*DevConn, error) {
	dev, err := os.OpenFile("/dev/fuse", os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("fuse: could not open /dev/fuse (%w); load the fuse module (modprobe fuse) and ensure the process may use it", err)
	}
	opts := fmt.Sprintf("fd=%d,rootmode=%o,user_id=%d,group_id=%d",
		dev.Fd(), syscall.S_IFDIR, os.Getuid(), os.Getgid())
	flags := uintptr(syscall.MS_NOSUID | syscall.MS_NODEV)
	if err := syscall.Mount("silo", mountpoint, "fuse", flags, opts); err != nil {
		_ = dev.Close()
		return nil, fmt.Errorf("fuse: could not mount at %s (%w); check the path exists and the process has CAP_SYS_ADMIN", mountpoint, err)
	}
	return &DevConn{f: dev, mountpoint: mountpoint}, nil
}

// ReadRequest reads one complete FUSE request. The kernel delivers exactly one
// message per read, so a single Read suffices. After unmount the device reports
// ENODEV, which is surfaced as io.EOF so Serve returns cleanly.
func (c *DevConn) ReadRequest() ([]byte, error) {
	buf := make([]byte, devBufSize)
	n, err := c.f.Read(buf)
	if err != nil {
		if errors.Is(err, syscall.ENODEV) {
			return nil, io.EOF // the mount went away
		}
		return nil, err
	}
	return buf[:n], nil
}

// WriteResponse writes one reply message in a single write, as the kernel
// requires.
func (c *DevConn) WriteResponse(b []byte) error {
	_, err := c.f.Write(b)
	return err
}

// Close unmounts the filesystem and closes /dev/fuse.
func (c *DevConn) Close() error {
	uerr := syscall.Unmount(c.mountpoint, 0)
	cerr := c.f.Close()
	if uerr != nil {
		return fmt.Errorf("fuse: could not unmount %s (%w); a process may still have the mount busy", c.mountpoint, uerr)
	}
	return cerr
}
