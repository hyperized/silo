package csi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hyperized/silo/internal/nbdclient"
	"github.com/hyperized/silo/internal/nbdnl"
)

// nbdSession is the supervised attachment surface the attacher drives;
// *nbdclient.Session satisfies it, tests substitute fakes.
type nbdSession interface {
	Device() string
	Export() string
	Index() uint32
	Size() uint64
	State() nbdclient.State
	Watched() bool
	Reconnects() uint64
	Kick()
	Detach(ctx context.Context) error
	Stop()
}

// linkWatcher delivers kernel dead-link notifications; *nbdnl.Watcher
// satisfies it.
type linkWatcher interface {
	Next() (uint32, error)
	Close() error
}

// NBDAttacher attaches silo volumes as NBD block devices served by the node's
// own silod, and keeps them attached: every attachment is supervised, so when
// silod restarts (a rollout, an upgrade, a crash) the attacher reconnects the
// device and the kernel resumes the queued I/O — pods only observe a pause.
//
// Attachments are recorded on disk so a restarted node plugin resumes
// supervising the devices it attached earlier, and Detach stays correct.
type NBDAttacher struct {
	addr      string
	window    time.Duration
	ioTimeout time.Duration
	logger    *slog.Logger
	stateDir  string

	kernel      nbdclient.KernelNBD
	closeKernel func() error
	newWatcher  func() (linkWatcher, error)
	attach      func(ctx context.Context, cfg nbdclient.Config) (nbdSession, error)
	adopt       func(ctx context.Context, cfg nbdclient.Config, index uint32, size uint64) (nbdSession, error)
	// configured reports whether an NBD device currently holds a kernel
	// configuration — the test that separates "connection died, reconnectable"
	// from "gone entirely" (e.g. after a node reboot), where a stale record
	// must be dropped rather than adopted.
	configured func(index uint32) bool
	// probe reads from a device to learn whether its link is up. Sessions
	// without a socket watch need it: their link may have died while no
	// supervisor was running, or without a notification arriving, and only
	// an actual read can tell.
	probe func(device string, timeout time.Duration) probeVerdict

	// stop is closed by Close to end the background goroutines.
	stop      chan struct{}
	closeOnce sync.Once

	mu       sync.Mutex
	sessions map[string]nbdSession
	records  map[string]attachmentRecord
	store    *attachmentStore
	watcher  linkWatcher
	wg       sync.WaitGroup
	// reconnectsBase preserves the reconnect counts of sessions that have
	// been detached, so the exported total never goes backwards.
	reconnectsBase uint64
}

// NBDAttacherOption configures an NBDAttacher.
type NBDAttacherOption func(*NBDAttacher)

// WithReconnectWindow sets how long a volume's I/O waits for silod to come
// back before erroring (the kernel's dead-connection timeout).
func WithReconnectWindow(d time.Duration) NBDAttacherOption {
	return func(a *NBDAttacher) {
		if d > 0 {
			a.window = d
		}
	}
}

// WithRequestTimeout bounds a single block request in the kernel; a request
// that exceeds it has its connection retired and is retried on the next one.
// Zero disables the bound.
func WithRequestTimeout(d time.Duration) NBDAttacherOption {
	return func(a *NBDAttacher) {
		if d >= 0 {
			a.ioTimeout = d
		}
	}
}

// WithStateDir sets where attachment state is persisted; it should be the
// plugin directory, which is host-backed and survives pod restarts.
func WithStateDir(dir string) NBDAttacherOption {
	return func(a *NBDAttacher) {
		if dir != "" {
			a.stateDir = dir
		}
	}
}

// WithAttacherLogger sets the logger for attach/reconnect activity.
func WithAttacherLogger(logger *slog.Logger) NBDAttacherOption {
	return func(a *NBDAttacher) {
		if logger != nil {
			a.logger = logger
		}
	}
}

// withNBDBackend injects the kernel interface and session constructors —
// the test seam that keeps unit tests off the real netlink socket.
func withNBDBackend(
	kernel nbdclient.KernelNBD,
	newWatcher func() (linkWatcher, error),
	attach func(context.Context, nbdclient.Config) (nbdSession, error),
	adopt func(context.Context, nbdclient.Config, uint32, uint64) (nbdSession, error),
	configured func(uint32) bool,
	probe func(string, time.Duration) probeVerdict,
) NBDAttacherOption {
	return func(a *NBDAttacher) {
		a.kernel = kernel
		a.newWatcher = newWatcher
		a.attach = attach
		a.adopt = adopt
		a.configured = configured
		a.probe = probe
	}
}

// NewNBDAttacher builds the attacher against the local silod NBD listener at
// nbdAddr (host:port), resumes supervision of any attachments recorded by a
// previous run, and starts watching for dead links: through the kernel's
// notifications and by periodically probing every device that has no socket
// watch.
func NewNBDAttacher(nbdAddr string, opts ...NBDAttacherOption) (*NBDAttacher, error) {
	if _, _, err := splitHostPort(nbdAddr); err != nil {
		return nil, err
	}
	a := &NBDAttacher{
		addr:     nbdAddr,
		window:   nbdclient.DefaultReconnectWindow,
		logger:   slog.Default(),
		stateDir: "/csi",
		sessions: map[string]nbdSession{},
		records:  map[string]attachmentRecord{},
		stop:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(a)
	}
	if a.kernel == nil {
		conn, err := nbdnl.Dial()
		if err != nil {
			return nil, fmt.Errorf("csi: the node cannot drive NBD devices (%w)", err)
		}
		a.kernel = conn
		a.closeKernel = conn.Close
		a.newWatcher = func() (linkWatcher, error) { return conn.Watch() }
		a.attach = func(ctx context.Context, cfg nbdclient.Config) (nbdSession, error) {
			return nbdclient.Attach(ctx, cfg)
		}
		a.adopt = func(ctx context.Context, cfg nbdclient.Config, index uint32, size uint64) (nbdSession, error) {
			return nbdclient.Adopt(ctx, cfg, index, size)
		}
		a.configured = deviceConfigured
		a.probe = probeDeviceLiveness
	}
	a.store = newAttachmentStore(a.stateDir)
	a.resume()
	a.startWatcher()
	a.startLinkCheck(linkCheckInterval)
	return a, nil
}

// deviceConfigured probes sysfs: the kernel exposes /sys/block/nbdX/pid only
// while the device holds a configuration, dead link or not.
func deviceConfigured(index uint32) bool {
	_, err := os.Stat(fmt.Sprintf("/sys/block/nbd%d/pid", index))
	return err == nil
}

// resume reloads the recorded attachments and re-supervises each device that
// still exists; records for devices that vanished (a node reboot) are dropped.
func (a *NBDAttacher) resume() {
	records, err := a.store.load()
	if err != nil {
		a.logger.Error("could not reload attachment state; previously attached volumes will be supervised again once they are re-published", "error", err)
		return
	}
	kept := records[:0]
	for _, rec := range records {
		if !a.configured(rec.Index) {
			a.logger.Info("dropping a stale attachment record; its device no longer exists", "volume", rec.Volume, "device", nbdnl.DevicePath(rec.Index))
			continue
		}
		s, err := a.adopt(context.Background(), a.sessionConfig(rec.Volume), rec.Index, rec.Size)
		if err != nil {
			a.logger.Warn("could not resume supervising an attached volume; it stays attached and will be adopted when next published", "volume", rec.Volume, "device", nbdnl.DevicePath(rec.Index), "error", err)
			kept = append(kept, rec)
			a.records[rec.Volume] = rec
			continue
		}
		a.repairIfDead(s)
		a.sessions[rec.Volume] = s
		a.records[rec.Volume] = rec
		kept = append(kept, rec)
	}
	if len(kept) != len(records) {
		if err := a.persistLocked(); err != nil {
			a.logger.Warn("could not persist the pruned attachment state", "error", err)
		}
	}
}

// probeVerdict is what a read from a device says about its link.
type probeVerdict int

const (
	// probeAlive means the read returned data: the link is up.
	probeAlive probeVerdict = iota
	// probeStalled means the read did not finish in time: the link is dead
	// and the kernel is queueing I/O, waiting for a reconnect within its
	// reconnect window.
	probeStalled
	// probeFailed means the read returned an error: the kernel has already
	// given up on the device and is failing its I/O.
	probeFailed
)

// probeTimeout bounds the device read probe: long enough for a slow but
// healthy silod, far shorter than the reconnect window a dead link queues
// I/O for.
const probeTimeout = 3 * time.Second

// linkCheckInterval is how often devices without a socket watch are probed;
// a var so tests can tighten it. It has to stay well inside the reconnect
// window, since a dead link found after the window has already cost the
// volume its I/O.
var linkCheckInterval = 15 * time.Second

// Bounds for re-opening the dead-link watch after it fails; vars so tests can
// tighten them.
var (
	watcherRetryFloor = time.Second
	watcherRetryCap   = 30 * time.Second
)

func (a *NBDAttacher) startWatcher() {
	if a.newWatcher == nil {
		return
	}
	w, err := a.newWatcher()
	if err != nil {
		a.logger.Warn("the kernel's dead-link notifications are unavailable; volumes this plugin cannot watch directly are checked with a test read at every interval instead", "interval", linkCheckInterval, "error", err)
		return
	}
	a.mu.Lock()
	a.watcher = w
	a.mu.Unlock()
	a.wg.Add(1)
	go a.consumeDeadLinks(w)
}

// consumeDeadLinks kicks the session behind every dead-link notification. The
// watch must outlive any single socket error, because for an adopted device
// it is the fastest way a dead link gets noticed.
func (a *NBDAttacher) consumeDeadLinks(w linkWatcher) {
	defer a.wg.Done()
	for {
		index, err := w.Next()
		switch {
		case err == nil:
			a.kickIndex(index)
		case errors.Is(err, nbdnl.ErrNotificationsDropped):
			a.logger.Warn("the kernel dropped some dead-link notifications; checking every volume this plugin cannot watch directly now")
			a.checkUnwatched()
		default:
			if w = a.reopenWatcher(w, err); w == nil {
				return
			}
		}
	}
}

func (a *NBDAttacher) kickIndex(index uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, s := range a.sessions {
		if s.Index() == index {
			s.Kick()
		}
	}
}

// reopenWatcher replaces a failed watch, retrying with backoff until it
// succeeds or the attacher closes (nil). Notifications sent while no watch
// was open are lost, so a successful re-open is followed by a link check.
func (a *NBDAttacher) reopenWatcher(failed linkWatcher, cause error) linkWatcher {
	if a.closing() {
		return nil
	}
	_ = failed.Close()
	a.logger.Warn("the dead-link watch failed; re-opening it", "error", cause)
	delay := watcherRetryFloor
	for {
		select {
		case <-a.stop:
			return nil
		case <-time.After(delay):
		}
		w, err := a.newWatcher()
		if err != nil {
			a.logger.Warn("could not re-open the dead-link watch; retrying", "retry_in", delay, "error", err)
			if delay *= 2; delay > watcherRetryCap {
				delay = watcherRetryCap
			}
			continue
		}
		if !a.installWatcher(w) {
			_ = w.Close()
			return nil
		}
		a.logger.Info("the dead-link watch is back")
		a.checkUnwatched()
		return w
	}
}

// installWatcher publishes w for Close to find, unless Close has begun: Close
// reads a.watcher only after closing a.stop, so checking stop under the lock
// guarantees every watch gets closed exactly once.
func (a *NBDAttacher) installWatcher(w linkWatcher) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closing() {
		return false
	}
	a.watcher = w
	return true
}

func (a *NBDAttacher) closing() bool {
	select {
	case <-a.stop:
		return true
	default:
		return false
	}
}

// startLinkCheck probes every device without a socket watch at each interval.
// An adopted device has no fd to watch, and the kernel's dead-link
// notifications are best-effort, so without this a dead link can go
// unnoticed until the kernel's reconnect window expires and fails the
// volume's I/O.
func (a *NBDAttacher) startLinkCheck(interval time.Duration) {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-a.stop:
				return
			case <-ticker.C:
				a.checkUnwatched()
			}
		}
	}()
}

// checkUnwatched probes, in parallel, every healthy session that has no
// socket watch, and repairs the dead ones. Sessions already reconnecting are
// skipped: their supervisor is on it. The lock is not held while probing,
// since a dead link holds a probe for its full timeout.
func (a *NBDAttacher) checkUnwatched() {
	a.mu.Lock()
	candidates := make([]nbdSession, 0, len(a.sessions))
	for _, s := range a.sessions {
		if !s.Watched() && s.State() == nbdclient.StateHealthy {
			candidates = append(candidates, s)
		}
	}
	a.mu.Unlock()
	var wg sync.WaitGroup
	for _, s := range candidates {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.repairIfDead(s)
		}()
	}
	wg.Wait()
}

// repairIfDead probes a device the supervisor cannot watch and kicks its
// session when the link is down.
func (a *NBDAttacher) repairIfDead(s nbdSession) {
	verdict := a.probe(s.Device(), probeTimeout)
	if verdict == probeAlive || s.State() == nbdclient.StateDetached {
		// A session detached while the probe ran fails its read for that
		// reason alone; there is nothing to repair or report.
		return
	}
	if verdict == probeFailed {
		a.logger.Error("the kernel already failed I/O on this volume because its connection stayed down longer than the reconnect window; a filesystem on it may now be read-only, so restart the pod using it once the connection is re-established",
			"device", s.Device(), "volume", s.Export(), "reconnect_window", a.window)
	} else {
		a.logger.Warn("a volume's connection is dead; reconnecting before the kernel's reconnect window runs out", "device", s.Device(), "volume", s.Export())
	}
	s.Kick()
}

func (a *NBDAttacher) sessionConfig(volumePath string) nbdclient.Config {
	return nbdclient.Config{
		Addr:            a.addr,
		Export:          volumePath,
		ReconnectWindow: a.window,
		IOTimeout:       a.ioTimeout,
		Kernel:          a.kernel,
		Logger:          a.logger,
	}
}

// Attach exposes the volume as a local NBD device and returns the device
// path. An already-attached volume returns its existing device; a recorded
// but unsupervised volume (the plugin restarted) is adopted rather than
// attached twice.
func (a *NBDAttacher) Attach(ctx context.Context, volumePath string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if s, ok := a.sessions[volumePath]; ok {
		return s.Device(), nil
	}
	if rec, ok := a.records[volumePath]; ok {
		if a.configured(rec.Index) {
			s, err := a.adopt(ctx, a.sessionConfig(volumePath), rec.Index, rec.Size)
			if err != nil {
				return "", fmt.Errorf("volume %q is already attached as %s but cannot be supervised (%w); restart silod on this node if this persists", volumePath, nbdnl.DevicePath(rec.Index), err)
			}
			a.repairIfDead(s)
			a.sessions[volumePath] = s
			return s.Device(), nil
		}
		// The recorded device is gone (node reboot); attach afresh below.
		delete(a.records, volumePath)
	}
	s, err := a.attach(ctx, a.sessionConfig(volumePath))
	if err != nil {
		return "", err
	}
	a.sessions[volumePath] = s
	a.records[volumePath] = attachmentRecord{Volume: volumePath, Index: s.Index(), Size: s.Size()}
	if err := a.persistLocked(); err != nil {
		// An unrecorded attachment would be orphaned by the next plugin
		// restart — undo and fail the publish so the kubelet retries.
		_ = s.Detach(ctx)
		delete(a.sessions, volumePath)
		delete(a.records, volumePath)
		return "", err
	}
	return s.Device(), nil
}

// Detach disconnects the volume's device. A volume that is not attached here
// is not an error.
func (a *NBDAttacher) Detach(ctx context.Context, volumePath string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if s, ok := a.sessions[volumePath]; ok {
		if err := s.Detach(ctx); err != nil {
			return err
		}
		a.reconnectsBase += s.Reconnects()
		delete(a.sessions, volumePath)
	} else if rec, ok := a.records[volumePath]; ok && a.configured(rec.Index) {
		// Recorded but never adopted (a failed resume): disconnect directly.
		if err := a.kernel.Disconnect(rec.Index); err != nil {
			return fmt.Errorf("could not detach %s backing %q (%w)", nbdnl.DevicePath(rec.Index), volumePath, err)
		}
	}
	if _, ok := a.records[volumePath]; !ok {
		return nil
	}
	delete(a.records, volumePath)
	return a.persistLocked()
}

// AttachmentHealth is one attachment's condition for the volume-stats surface.
type AttachmentHealth struct {
	Device     string
	State      nbdclient.State
	Reconnects uint64
}

// Health reports the attachment's condition; ok is false when the volume is
// not attached on this node.
func (a *NBDAttacher) Health(volumePath string) (AttachmentHealth, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if s, ok := a.sessions[volumePath]; ok {
		return AttachmentHealth{Device: s.Device(), State: s.State(), Reconnects: s.Reconnects()}, true
	}
	if rec, ok := a.records[volumePath]; ok {
		// Recorded but unsupervised: attached, health unknown until adopted.
		return AttachmentHealth{Device: nbdnl.DevicePath(rec.Index), State: nbdclient.StateReconnecting}, true
	}
	return AttachmentHealth{}, false
}

// Close stops supervising (devices stay attached and serving) and releases
// the netlink resources. Meant for process shutdown.
func (a *NBDAttacher) Close() error {
	a.closeOnce.Do(func() { close(a.stop) })
	a.mu.Lock()
	watcher := a.watcher
	a.watcher = nil
	sessions := make([]nbdSession, 0, len(a.sessions))
	for _, s := range a.sessions {
		sessions = append(sessions, s)
	}
	a.mu.Unlock()
	if watcher != nil {
		_ = watcher.Close()
	}
	for _, s := range sessions {
		s.Stop()
	}
	a.wg.Wait()
	if a.closeKernel != nil {
		return a.closeKernel()
	}
	return nil
}

// persistLocked saves the records; callers hold a.mu.
func (a *NBDAttacher) persistLocked() error {
	records := make([]attachmentRecord, 0, len(a.records))
	for _, rec := range a.records {
		records = append(records, rec)
	}
	return a.store.save(records)
}

// splitHostPort splits a host:port advertise address, with an actionable error.
func splitHostPort(addr string) (host, port string, err error) {
	i := strings.LastIndex(addr, ":")
	if i <= 0 || i == len(addr)-1 {
		return "", "", fmt.Errorf("%q is not a host:port NBD address; set the node plugin's silod NBD address, e.g. 127.0.0.1:10809", addr)
	}
	return addr[:i], addr[i+1:], nil
}
