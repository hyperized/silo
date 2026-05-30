//go:build !linux

package fuse

import "fmt"

// Mount is unavailable off Linux: FUSE is a Linux kernel interface. The protocol
// library still builds and tests everywhere; only the live mount is Linux-only.
func Mount(mountpoint string) (*DevConn, error) {
	return nil, fmt.Errorf("fuse: mounting is only supported on Linux (mountpoint %s)", mountpoint)
}

// DevConn is declared off-Linux only so signatures referencing it compile; its
// methods are never reached because Mount always errors.
type DevConn struct{}

func (*DevConn) ReadRequest() ([]byte, error) { return nil, fmt.Errorf("fuse: unsupported platform") }
func (*DevConn) WriteResponse([]byte) error   { return fmt.Errorf("fuse: unsupported platform") }
func (*DevConn) Close() error                 { return nil }
