//go:build linux

package nbdclient

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// watchSocket duplicates a connection's fd and watches the shared TCP state
// for the peer closing or erroring, invoking kick once. It never reads — the
// kernel's NBD machinery owns the data — and a duplicated fd observes hangups
// even after the original is closed. This is the primary dead-link detector:
// unlike the netlink multicast it cannot be dropped, and unlike the kernel's
// status flag it reports the link, not just the configuration.
//
// One parked OS thread per attachment while polling; nodes serve at most a
// few dozen volumes, so that beats a shared poller's complexity.
var watchSocket = func(fd int, kick func()) (stop func(), err error) {
	dup, err := unix.Dup(fd)
	if err != nil {
		return nil, fmt.Errorf("nbdclient: could not duplicate the socket to watch it (%w)", err)
	}
	// The self-pipe unblocks the poll when the session stops watching.
	var pipe [2]int
	if err := unix.Pipe2(pipe[:], unix.O_CLOEXEC); err != nil {
		_ = unix.Close(dup)
		return nil, fmt.Errorf("nbdclient: could not create the watch stop pipe (%w)", err)
	}
	go func() {
		defer func() {
			_ = unix.Close(dup)
			_ = unix.Close(pipe[0])
		}()
		fds := []unix.PollFd{
			{Fd: int32(dup), Events: unix.POLLRDHUP | unix.POLLERR | unix.POLLHUP}, // #nosec G115 -- fds fit in int32
			{Fd: int32(pipe[0]), Events: unix.POLLIN},                              // #nosec G115 -- fds fit in int32
		}
		for {
			for i := range fds {
				fds[i].Revents = 0
			}
			n, err := unix.Poll(fds, -1)
			if err == unix.EINTR {
				continue
			}
			if err != nil || n == 0 {
				return
			}
			if fds[1].Revents != 0 {
				return // stopped
			}
			if fds[0].Revents&(unix.POLLRDHUP|unix.POLLERR|unix.POLLHUP) != 0 {
				kick()
				return
			}
		}
	}()
	return func() { _ = unix.Close(pipe[1]) }, nil
}
