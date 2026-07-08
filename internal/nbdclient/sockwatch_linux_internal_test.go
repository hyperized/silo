//go:build linux

package nbdclient

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestWatchSocketFiresOnPeerClose(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	defer func() { _ = unix.Close(fds[0]) }()

	kicked := make(chan struct{}, 1)
	stop, err := watchSocket(fds[0], func() { kicked <- struct{}{} })
	if err != nil {
		t.Fatalf("watchSocket: %v", err)
	}
	defer stop()

	// Closing the peer end makes the watched fd's poll report a hangup, which is
	// exactly the dead-link signal the supervisor reconnects on.
	_ = unix.Close(fds[1])
	select {
	case <-kicked:
	case <-time.After(2 * time.Second):
		t.Fatal("watchSocket did not fire after the peer closed")
	}
}

func TestWatchSocketStopsWithoutFiring(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	defer func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) }()

	kicked := make(chan struct{}, 1)
	stop, err := watchSocket(fds[0], func() { kicked <- struct{}{} })
	if err != nil {
		t.Fatalf("watchSocket: %v", err)
	}
	stop() // unblock the poll via the self-pipe while the peer is still alive

	// With the peer still open, the only kick that could arrive is a spurious
	// wakeup; give the goroutine time to exit and assert it stayed quiet.
	select {
	case <-kicked:
		t.Fatal("watchSocket fired after stop with a live peer")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWatchSocketRejectsBadFD(t *testing.T) {
	if _, err := watchSocket(-1, func() {}); err == nil {
		t.Fatal("watchSocket(-1) should fail to duplicate the fd")
	}
}
