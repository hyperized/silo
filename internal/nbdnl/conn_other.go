//go:build !linux

package nbdnl

import (
	"errors"
	"time"
)

// ErrUnsupportedOS explains why NBD devices cannot be attached here: the NBD
// driver is Linux-only, so on other platforms the API compiles but every
// operation fails with this error.
var ErrUnsupportedOS = errors.New("nbdnl: NBD block devices require Linux; this build supports the API but cannot attach devices on this OS")

// Conn is the non-Linux placeholder for the kernel NBD netlink session.
type Conn struct{}

// Dial always fails: there is no NBD driver to talk to on this OS.
func Dial() (*Conn, error) { return nil, ErrUnsupportedOS }

func (c *Conn) Close() error { return ErrUnsupportedOS }

func (c *Conn) Connect(ConnectConfig) (uint32, error) { return 0, ErrUnsupportedOS }

func (c *Conn) Reconfigure(uint32, int, time.Duration, time.Duration) error {
	return ErrUnsupportedOS
}

func (c *Conn) Disconnect(uint32) error { return ErrUnsupportedOS }

func (c *Conn) Connected(uint32) (bool, error) { return false, ErrUnsupportedOS }

func (c *Conn) Watch() (*Watcher, error) { return nil, ErrUnsupportedOS }

// Watcher is the non-Linux placeholder for the dead-link watch.
type Watcher struct{}

func (w *Watcher) Next() (uint32, error) { return 0, ErrUnsupportedOS }

func (w *Watcher) Close() error { return ErrUnsupportedOS }
