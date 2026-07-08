//go:build linux

package nbdnl

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

func init() {
	// Surface kernel error codes as errno values so callers can test with
	// errors.Is(err, unix.ENOENT) and friends.
	errnoToError = func(code int32) error { return unix.Errno(-code) }
}

// receiveBufSize fits any reply the NBD family produces, including a status
// dump listing every device; a datagram larger than the read buffer would be
// silently truncated, so err on the large side.
const receiveBufSize = 64 << 10

// netlinkSocket opens a generic-netlink socket bound to this process and
// connected to the kernel, wrapped in an *os.File so reads go through the
// runtime poller (Close unblocks a pending Read — a raw blocking fd would
// stay stuck in recvfrom forever).
func netlinkSocket() (*os.File, uint32, error) {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.NETLINK_GENERIC)
	if err != nil {
		return nil, 0, fmt.Errorf("nbdnl: could not open a netlink socket (%w)", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		_ = unix.Close(fd)
		return nil, 0, fmt.Errorf("nbdnl: could not bind the netlink socket (%w)", err)
	}
	sa, err := unix.Getsockname(fd)
	if err != nil {
		_ = unix.Close(fd)
		return nil, 0, fmt.Errorf("nbdnl: could not read the netlink socket's address (%w)", err)
	}
	nlsa, ok := sa.(*unix.SockaddrNetlink)
	if !ok {
		_ = unix.Close(fd)
		return nil, 0, errors.New("nbdnl: netlink socket has a non-netlink address")
	}
	if err := unix.Connect(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		_ = unix.Close(fd)
		return nil, 0, fmt.Errorf("nbdnl: could not connect the netlink socket to the kernel (%w)", err)
	}
	return os.NewFile(uintptr(fd), "netlink:nbd"), nlsa.Pid, nil
}

// Conn is a session with the kernel's NBD generic-netlink family. Methods are
// safe for concurrent use; each command is one request/ack round-trip.
type Conn struct {
	mu     sync.Mutex
	f      *os.File
	pid    uint32
	seq    uint32
	family familyInfo
}

// Dial opens the netlink session and resolves the NBD family, which fails
// with an instructive error when the nbd module is not loaded.
func Dial() (*Conn, error) {
	f, pid, err := netlinkSocket()
	if err != nil {
		return nil, err
	}
	c := &Conn{f: f, pid: pid}
	reply, err := c.roundTrip(func(seq uint32) []byte {
		return getFamilyRequest(seq, pid, nbdFamilyName)
	}, true)
	if err != nil {
		_ = f.Close()
		if errors.Is(err, unix.ENOENT) {
			return nil, errors.New("nbdnl: the kernel's NBD driver is not loaded; load the nbd module first (modprobe nbd — on Talos, add nbd to machine.kernel.modules)")
		}
		return nil, fmt.Errorf("nbdnl: could not resolve the NBD netlink family (%w)", err)
	}
	info, err := parseFamilyReply(reply)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	c.family = info
	return c, nil
}

// Close releases the netlink socket.
func (c *Conn) Close() error { return c.f.Close() }

// roundTrip sends one request and reads messages until its ack, returning the
// command's payload reply if one arrived. build receives the sequence number
// so requests are constructed under the same lock that serialises the wire.
func (c *Conn) roundTrip(build func(seq uint32) []byte, wantReply bool) (message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	seq := c.seq
	if _, err := c.f.Write(build(seq)); err != nil {
		return message{}, fmt.Errorf("nbdnl: netlink send failed (%w)", err)
	}
	var reply message
	var haveReply bool
	buf := make([]byte, receiveBufSize)
	for {
		n, err := c.f.Read(buf)
		if err != nil {
			return message{}, fmt.Errorf("nbdnl: netlink receive failed (%w)", err)
		}
		msgs, err := parseMessages(buf[:n])
		if err != nil {
			return message{}, err
		}
		for _, m := range msgs {
			if m.Seq != seq {
				continue // a stray notification or an earlier command's residue
			}
			switch {
			case m.Err != nil:
				return message{}, m.Err
			case m.IsAck:
				if wantReply && !haveReply {
					return message{}, errors.New("nbdnl: the kernel acknowledged the request without the expected reply")
				}
				return reply, nil
			case m.Type == nlmsgDone:
				// Terminates a multipart reply; the ack still follows.
			default:
				reply = m
				haveReply = true
			}
		}
	}
}

// Connect attaches a handshaken socket to a kernel-chosen free NBD device and
// returns its index. The kernel keeps its own reference to the socket.
func (c *Conn) Connect(cfg ConnectConfig) (uint32, error) {
	reply, err := c.roundTrip(func(seq uint32) []byte {
		return connectRequest(c.family.id, seq, c.pid, cfg)
	}, true)
	if err != nil {
		if errors.Is(err, unix.EPERM) {
			return 0, errors.New("nbdnl: not permitted to configure NBD devices; the node plugin must run privileged (CAP_NET_ADMIN and CAP_SYS_ADMIN)")
		}
		return 0, fmt.Errorf("nbdnl: the kernel refused to connect the NBD device (%w)", err)
	}
	return parseConnectReply(reply)
}

// Reconfigure splices a fresh handshaken socket into a live device — the
// reconnect path after silod restarts. The kernel requeues I/O that was
// pending while the connection was down onto the new socket.
func (c *Conn) Reconfigure(index uint32, socketFD int, ioTimeout, deadConnTimeout time.Duration) error {
	_, err := c.roundTrip(func(seq uint32) []byte {
		return reconfigureRequest(c.family.id, seq, c.pid, index, socketFD, ioTimeout, deadConnTimeout)
	}, false)
	if err != nil {
		return fmt.Errorf("nbdnl: could not hand device %d its new connection (%w)", index, err)
	}
	return nil
}

// Disconnect tears down the device's connection and configuration.
func (c *Conn) Disconnect(index uint32) error {
	_, err := c.roundTrip(func(seq uint32) []byte {
		return disconnectRequest(c.family.id, seq, c.pid, index)
	}, false)
	if err != nil {
		return fmt.Errorf("nbdnl: could not disconnect device %d (%w)", index, err)
	}
	return nil
}

// Connected reports whether the device currently has a live connection.
func (c *Conn) Connected(index uint32) (bool, error) {
	reply, err := c.roundTrip(func(seq uint32) []byte {
		return statusRequest(c.family.id, seq, c.pid, index)
	}, true)
	if err != nil {
		return false, fmt.Errorf("nbdnl: could not read device %d's status (%w)", index, err)
	}
	return parseStatusReply(reply, index)
}

// Watcher delivers the kernel's dead-link notifications: the index of each
// NBD device whose connection just died. One watcher serves a whole process.
type Watcher struct {
	f      *os.File
	family familyInfo
}

// Watch subscribes to the NBD status multicast group.
func (c *Conn) Watch() (*Watcher, error) {
	group, ok := c.family.groups[nbdMcastGroup]
	if !ok {
		return nil, fmt.Errorf("nbdnl: the kernel's NBD family exposes no %q multicast group; the running kernel is too old to notify about dead links", nbdMcastGroup)
	}
	f, _, err := netlinkSocket()
	if err != nil {
		return nil, err
	}
	sc, err := f.SyscallConn()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("nbdnl: could not reach the watch socket's fd (%w)", err)
	}
	var soErr error
	if err := sc.Control(func(fd uintptr) {
		soErr = unix.SetsockoptInt(int(fd), unix.SOL_NETLINK, unix.NETLINK_ADD_MEMBERSHIP, int(group))
	}); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("nbdnl: could not configure the watch socket (%w)", err)
	}
	if soErr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("nbdnl: could not join the NBD status multicast group (%w)", soErr)
	}
	return &Watcher{f: f, family: c.family}, nil
}

// Next blocks until a device's connection dies and returns its index. It
// returns an error once the watcher is closed.
func (w *Watcher) Next() (uint32, error) {
	buf := make([]byte, receiveBufSize)
	for {
		n, err := w.f.Read(buf)
		if err != nil {
			return 0, fmt.Errorf("nbdnl: the dead-link watch ended (%w)", err)
		}
		msgs, err := parseMessages(buf[:n])
		if err != nil {
			// A malformed unrelated notification should not kill the watch.
			continue
		}
		for _, m := range msgs {
			if m.Type != w.family.id {
				continue
			}
			if idx, ok := parseLinkDead(m); ok {
				return idx, nil
			}
		}
	}
}

// Close ends the watch, unblocking a pending Next.
func (w *Watcher) Close() error { return w.f.Close() }
