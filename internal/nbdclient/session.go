package nbdclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hyperized/silo/internal/nbdnl"
)

// KernelNBD is the seam to the kernel's NBD netlink interface;
// *nbdnl.Conn satisfies it, tests substitute a fake kernel.
type KernelNBD interface {
	Connect(cfg nbdnl.ConnectConfig) (uint32, error)
	Reconfigure(index uint32, socketFD int, ioTimeout, deadConnTimeout time.Duration) error
	Disconnect(index uint32) error
}

// State is a session's health as the volume-condition surface reports it.
type State string

const (
	// StateHealthy means the device has a live connection to silod.
	StateHealthy State = "healthy"
	// StateReconnecting means the connection died and the supervisor is
	// re-establishing it while the kernel queues the volume's I/O.
	StateReconnecting State = "reconnecting"
	// StateDetached means the device was deliberately disconnected.
	StateDetached State = "detached"
)

// Defaults for Config fields left zero.
const (
	// DefaultReconnectWindow is how long the kernel holds a volume's I/O
	// while silod is away before failing it — long enough to cover a rolling
	// restart including an image pull.
	DefaultReconnectWindow = 5 * time.Minute
	// defaultDialTimeout bounds one dial+handshake attempt.
	defaultDialTimeout = 15 * time.Second
)

// Reconnect backoff bounds; vars so tests can tighten them.
var (
	backoffFloor = 250 * time.Millisecond
	backoffCap   = 10 * time.Second
)

// Config describes one supervised attachment.
type Config struct {
	// Addr is silod's NBD listener, e.g. 127.0.0.1:10809.
	Addr string
	// Export is the volume's namespace path, which is its NBD export name.
	Export string
	// ReconnectWindow is how long the kernel queues I/O while disconnected,
	// waiting for the supervisor to reconnect (0 = DefaultReconnectWindow).
	ReconnectWindow time.Duration
	// IOTimeout bounds a single I/O request in the kernel (0 = kernel default).
	IOTimeout time.Duration
	// BlockSize is the device's logical block size (0 = kernel default).
	BlockSize uint64
	// Kernel talks to the NBD driver; required.
	Kernel KernelNBD
	// Dial opens the TCP connection to Addr (nil = a plain TCP dialer).
	Dial func(ctx context.Context, addr string) (net.Conn, error)
	// WatchSocket watches a connection's fd for the peer dying and calls the
	// kick (nil = the platform default; tests inject a fake since their fake
	// conns have no real fd).
	WatchSocket func(fd int, kick func()) (stop func(), err error)
	// Logger records reconnect activity (nil = slog.Default).
	Logger *slog.Logger
}

func (cfg *Config) applyDefaults() error {
	if cfg.Kernel == nil {
		return errors.New("nbdclient: a kernel NBD interface is required")
	}
	if cfg.Addr == "" || cfg.Export == "" {
		return errors.New("nbdclient: both the NBD address and the export name are required")
	}
	if cfg.ReconnectWindow <= 0 {
		cfg.ReconnectWindow = DefaultReconnectWindow
	}
	if cfg.WatchSocket == nil {
		cfg.WatchSocket = watchSocket
	}
	if cfg.Dial == nil {
		cfg.Dial = func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return nil
}

// socketFD extracts a connection's file descriptor so it can be handed to the
// kernel; swappable so tests can pair fake conns with the fake kernel.
var socketFD = func(conn net.Conn) (int, error) {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return -1, fmt.Errorf("nbdclient: %T exposes no file descriptor to hand to the kernel", conn)
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return -1, fmt.Errorf("nbdclient: could not reach the connection's file descriptor (%w)", err)
	}
	fd := -1
	if err := raw.Control(func(u uintptr) { fd = int(u) }); err != nil {
		return -1, fmt.Errorf("nbdclient: could not read the connection's file descriptor (%w)", err)
	}
	return fd, nil
}

// Session is one supervised NBD attachment: a /dev/nbdX whose connection to
// silod is watched and re-established for as long as the session lives.
type Session struct {
	cfg    Config
	index  uint32
	device string

	ctx    context.Context
	cancel context.CancelFunc
	kicks  chan struct{}
	wg     sync.WaitGroup

	mu         sync.Mutex
	state      State
	size       uint64
	stopWatch  func()
	reconnects atomic.Uint64
}

// Attach negotiates the export, connects it to a kernel-chosen NBD device and
// starts supervising it. ctx bounds only the initial attach.
func Attach(ctx context.Context, cfg Config) (*Session, error) {
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	export, fd, closeConn, err := dialAndNegotiate(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer closeConn()
	index, err := cfg.Kernel.Connect(nbdnl.ConnectConfig{
		SocketFD:        fd,
		SizeBytes:       export.Size,
		BlockSizeBytes:  cfg.BlockSize,
		ServerFlags:     export.TransmissionFlags,
		IOTimeout:       cfg.IOTimeout,
		DeadConnTimeout: cfg.ReconnectWindow,
	})
	if err != nil {
		return nil, err
	}
	s := newSession(cfg, index, export.Size, StateHealthy)
	s.watch(fd)
	s.cfg.Logger.Info("nbd volume attached", "export", cfg.Export, "device", s.device, "size", export.Size)
	return s, nil
}

// Adopt resumes supervising a device this process (or a predecessor) attached
// earlier — the node plugin restarting must not orphan live attachments. size
// is the expected export size from the attachment record (0 skips the check).
// The old connection's fd is gone with the old process, so death detection
// relies on the dead-link watcher and the caller probing the device once.
func Adopt(_ context.Context, cfg Config, index uint32, size uint64) (*Session, error) {
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	s := newSession(cfg, index, size, StateHealthy)
	s.cfg.Logger.Info("nbd volume adopted", "export", cfg.Export, "device", s.device)
	return s, nil
}

func newSession(cfg Config, index uint32, size uint64, state State) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		cfg:    cfg,
		index:  index,
		device: nbdnl.DevicePath(index),
		ctx:    ctx,
		cancel: cancel,
		kicks:  make(chan struct{}, 1),
		state:  state,
		size:   size,
	}
	s.wg.Add(1)
	go s.supervise()
	return s
}

// dialAndNegotiate opens and handshakes one connection, returning the export,
// the socket fd, and a closer the caller runs once the kernel holds its own
// reference to the socket.
func dialAndNegotiate(ctx context.Context, cfg Config) (Export, int, func(), error) {
	dctx, cancel := context.WithTimeout(ctx, defaultDialTimeout)
	defer cancel()
	conn, err := cfg.Dial(dctx, cfg.Addr)
	if err != nil {
		return Export{}, -1, nil, fmt.Errorf("nbdclient: could not reach silod's NBD listener at %s (%w); check that silod is running with SILO_NBD_ADDR set", cfg.Addr, err)
	}
	export, err := Negotiate(dctx, conn, cfg.Export)
	if err != nil {
		_ = conn.Close()
		return Export{}, -1, nil, err
	}
	fd, err := socketFD(conn)
	if err != nil {
		_ = conn.Close()
		return Export{}, -1, nil, err
	}
	return export, fd, func() { _ = conn.Close() }, nil
}

// Kick asks the supervisor to check the connection now — the dead-link
// watcher calls this the moment the kernel reports the link down.
func (s *Session) Kick() {
	select {
	case s.kicks <- struct{}{}:
	default: // a check is already pending
	}
}

// supervise waits for kicks — from the socket watch, the dead-link watcher,
// or a device probe — reconnecting until the session is detached.
func (s *Session) supervise() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.kicks:
			s.reconnect()
		}
	}
}

// watch points the socket watcher at the connection's fd; failure only costs
// the fastest detection path, so it is logged, not fatal.
func (s *Session) watch(fd int) {
	stop, err := s.cfg.WatchSocket(fd, s.Kick)
	if err != nil {
		s.cfg.Logger.Warn("cannot watch the nbd socket; relying on the kernel's dead-link notifications", "export", s.cfg.Export, "error", err)
		return
	}
	s.mu.Lock()
	if s.stopWatch != nil {
		s.stopWatch()
	}
	s.stopWatch = stop
	s.mu.Unlock()
}

func (s *Session) unwatch() {
	s.mu.Lock()
	stop := s.stopWatch
	s.stopWatch = nil
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// reconnect re-establishes the device's connection, retrying with backoff
// until it succeeds or the session is detached. The kernel keeps queueing the
// volume's I/O for ReconnectWindow, so a success within the window means the
// workload only observed a pause.
func (s *Session) reconnect() {
	s.setState(StateReconnecting)
	s.cfg.Logger.Warn("nbd connection lost; reconnecting", "export", s.cfg.Export, "device", s.device)
	delay := backoffFloor
	for attempt := 1; ; attempt++ {
		if s.ctx.Err() != nil {
			return
		}
		err := s.reconnectOnce()
		if err == nil {
			// Kicks that queued while we were reconnecting describe the death
			// we just repaired; acting on them would tear down the connection
			// we just handed the kernel.
			select {
			case <-s.kicks:
			default:
			}
			s.setState(StateHealthy)
			s.reconnects.Add(1)
			s.cfg.Logger.Info("nbd connection re-established", "export", s.cfg.Export, "device", s.device, "attempts", attempt)
			return
		}
		s.cfg.Logger.Warn("nbd reconnect attempt failed", "export", s.cfg.Export, "device", s.device, "attempt", attempt, "error", err)
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay *= 2; delay > backoffCap {
			delay = backoffCap
		}
	}
}

func (s *Session) reconnectOnce() error {
	export, fd, closeConn, err := dialAndNegotiate(s.ctx, s.cfg)
	if err != nil {
		return err
	}
	defer closeConn()
	s.mu.Lock()
	expected := s.size
	s.mu.Unlock()
	if expected != 0 && export.Size != expected {
		// A different size means this export is no longer the volume this
		// device was serving; splicing it in would corrupt the filesystem.
		return fmt.Errorf("export %q is now %d bytes where the attached device is %d; refusing to reconnect a changed volume", s.cfg.Export, export.Size, expected)
	}
	if err := s.cfg.Kernel.Reconfigure(s.index, fd, s.cfg.IOTimeout, s.cfg.ReconnectWindow); err != nil {
		return err
	}
	s.watch(fd)
	if expected == 0 {
		s.mu.Lock()
		s.size = export.Size
		s.mu.Unlock()
	}
	return nil
}

// Detach stops supervising and disconnects the device. It is idempotent.
func (s *Session) Detach(context.Context) error {
	s.cancel()
	s.wg.Wait()
	s.unwatch()
	s.mu.Lock()
	alreadyDetached := s.state == StateDetached
	s.state = StateDetached
	s.mu.Unlock()
	if alreadyDetached {
		return nil
	}
	if err := s.cfg.Kernel.Disconnect(s.index); err != nil {
		return fmt.Errorf("nbdclient: could not detach %s (%w)", s.device, err)
	}
	s.cfg.Logger.Info("nbd volume detached", "export", s.cfg.Export, "device", s.device)
	return nil
}

// Stop ends supervision without touching the device — shutdown of the node
// plugin process must leave live attachments serving I/O.
func (s *Session) Stop() {
	s.cancel()
	s.wg.Wait()
	s.unwatch()
}

// Device returns the attached device node, e.g. /dev/nbd3.
func (s *Session) Device() string { return s.device }

// Index returns the kernel device index.
func (s *Session) Index() uint32 { return s.index }

// Size returns the export size the session is serving (0 if not yet known).
func (s *Session) Size() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// State reports the session's current health.
func (s *Session) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Reconnects counts completed reconnections — observability for how often
// silod restarts interrupted this attachment.
func (s *Session) Reconnects() uint64 { return s.reconnects.Load() }

func (s *Session) setState(state State) {
	s.mu.Lock()
	s.state = state
	s.mu.Unlock()
}
