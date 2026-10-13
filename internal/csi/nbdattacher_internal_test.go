package csi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/metrics"
	"github.com/hyperized/silo/internal/nbdclient"
	"github.com/hyperized/silo/internal/nbdnl"
)

// fakeSession is a supervised attachment the attacher tests drive directly.
type fakeSession struct {
	mu         sync.Mutex
	index      uint32
	export     string
	watched    bool
	size       uint64
	state      nbdclient.State
	kicks      int
	detaches   int
	stops      int
	detachErr  error
	reconnects uint64
}

func (s *fakeSession) Device() string { return nbdnl.DevicePath(s.index) }
func (s *fakeSession) Export() string { return s.export }
func (s *fakeSession) Index() uint32  { return s.index }

func (s *fakeSession) Watched() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watched
}

func (s *fakeSession) Size() uint64 { return s.size }

func (s *fakeSession) State() nbdclient.State {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == "" {
		return nbdclient.StateHealthy
	}
	return s.state
}

func (s *fakeSession) Reconnects() uint64 { return s.reconnects }

func (s *fakeSession) Kick() {
	s.mu.Lock()
	s.kicks++
	s.mu.Unlock()
}

func (s *fakeSession) kicked() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kicks
}

func (s *fakeSession) Detach(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.detachErr != nil {
		return s.detachErr
	}
	s.detaches++
	return nil
}

func (s *fakeSession) Stop() {
	s.mu.Lock()
	s.stops++
	s.mu.Unlock()
}

// fakeBackend wires an attacher entirely onto fakes: sessions come from here,
// no netlink, no sockets.
type fakeBackend struct {
	mu            sync.Mutex
	nextIndex     uint32
	attaches      []string
	adopts        []uint32
	sessions      map[string]*fakeSession
	attachErr     error
	adoptErr      error
	configured    map[uint32]bool
	verdicts      map[string]probeVerdict // probe results by device; unlisted devices are alive
	deadLinks     chan uint32
	watchErrs     chan error // errors the fake watcher's Next returns
	watcherErrs   []error    // results of the next newWatcher calls; nil entries succeed
	watchers      int
	disconnects   []uint32
	disconnectErr error
	specs         map[string]sessionSpec // how attach shapes a volume's session
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		sessions:   map[string]*fakeSession{},
		configured: map[uint32]bool{},
		verdicts:   map[string]probeVerdict{},
		deadLinks:  make(chan uint32),
		watchErrs:  make(chan error),
	}
}

func (b *fakeBackend) option() NBDAttacherOption {
	attach := func(_ context.Context, cfg nbdclient.Config) (nbdSession, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.attachErr != nil {
			return nil, b.attachErr
		}
		b.attaches = append(b.attaches, cfg.Export)
		spec := b.specs[cfg.Export]
		s := &fakeSession{index: b.nextIndex, export: cfg.Export, size: 1 << 20, watched: spec.watched, state: spec.state}
		b.nextIndex++
		b.sessions[cfg.Export] = s
		b.configured[s.index] = true
		return s, nil
	}
	adopt := func(_ context.Context, cfg nbdclient.Config, index uint32, size uint64) (nbdSession, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.adoptErr != nil {
			return nil, b.adoptErr
		}
		b.adopts = append(b.adopts, index)
		s := &fakeSession{index: index, export: cfg.Export, size: size}
		b.sessions[cfg.Export] = s
		return s, nil
	}
	watcher := func() (linkWatcher, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.watchers++
		if len(b.watcherErrs) > 0 {
			err := b.watcherErrs[0]
			b.watcherErrs = b.watcherErrs[1:]
			if err != nil {
				return nil, err
			}
		}
		return &fakeWatcher{ch: b.deadLinks, errs: b.watchErrs, done: make(chan struct{})}, nil
	}
	configured := func(index uint32) bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.configured[index]
	}
	probe := func(device string, _ time.Duration) probeVerdict {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.verdicts[device]
	}
	return withNBDBackend(&fakeKernelNBD{b: b}, watcher, attach, adopt, configured, probe)
}

func (b *fakeBackend) session(volume string) *fakeSession {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sessions[volume]
}

func (b *fakeBackend) setDisconnectErr(err error) {
	b.mu.Lock()
	b.disconnectErr = err
	b.mu.Unlock()
}

func (b *fakeBackend) disconnectsOf() []uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]uint32(nil), b.disconnects...)
}

type fakeKernelNBD struct{ b *fakeBackend }

func (k *fakeKernelNBD) Connect(nbdnl.ConnectConfig) (uint32, error) { return 0, nil }
func (k *fakeKernelNBD) Reconfigure(uint32, int, time.Duration, time.Duration) error {
	return nil
}

func (k *fakeKernelNBD) SetTimeouts(uint32, time.Duration, time.Duration) error { return nil }

func (k *fakeKernelNBD) Disconnect(index uint32) error {
	k.b.mu.Lock()
	defer k.b.mu.Unlock()
	if k.b.disconnectErr != nil {
		return k.b.disconnectErr
	}
	k.b.disconnects = append(k.b.disconnects, index)
	return nil
}
func (k *fakeKernelNBD) Connected(uint32) (bool, error) { return true, nil }

type fakeWatcher struct {
	ch     chan uint32
	errs   chan error
	closed sync.Once
	done   chan struct{}
}

func (w *fakeWatcher) Next() (uint32, error) {
	select {
	case idx := <-w.ch:
		return idx, nil
	case err := <-w.errs:
		return 0, err
	case <-w.done:
		return 0, errors.New("closed")
	}
}

func (w *fakeWatcher) Close() error {
	w.closed.Do(func() { close(w.done) })
	return nil
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestAttacher(t *testing.T, backend *fakeBackend, dir string) *NBDAttacher {
	t.Helper()
	a, err := NewNBDAttacher(
		"127.0.0.1:10809",
		backend.option(),
		WithStateDir(dir),
		WithAttacherLogger(quietLogger()),
		WithReconnectWindow(time.Minute),
	)
	if err != nil {
		t.Fatalf("NewNBDAttacher: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func TestNBDAttacherAttachDetachPersists(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	a := newTestAttacher(t, backend, dir)
	ctx := context.Background()

	dev, err := a.Attach(ctx, "/csi/volumes/pvc-1")
	if err != nil || dev != "/dev/nbd0" {
		t.Fatalf("Attach = (%q, %v), want /dev/nbd0", dev, err)
	}
	// Idempotent: the same volume returns the same device with no new attach.
	dev2, err := a.Attach(ctx, "/csi/volumes/pvc-1")
	if err != nil || dev2 != dev || len(backend.attaches) != 1 {
		t.Fatalf("repeat Attach = (%q, %v), attaches=%d", dev2, err, len(backend.attaches))
	}

	// The attachment is recorded on disk for the next plugin process.
	records, err := newAttachmentStore(dir).load()
	if err != nil || len(records) != 1 || records[0].Volume != "/csi/volumes/pvc-1" {
		t.Fatalf("state = (%+v, %v), want the attachment recorded", records, err)
	}

	if err := a.Detach(ctx, "/csi/volumes/pvc-1"); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	if backend.session("/csi/volumes/pvc-1").detaches != 1 {
		t.Fatal("Detach should tear down the session")
	}
	records, _ = newAttachmentStore(dir).load()
	if len(records) != 0 {
		t.Fatalf("state after detach = %+v, want empty", records)
	}
	// Detaching an unknown volume is a no-op.
	if err := a.Detach(ctx, "/unknown"); err != nil {
		t.Fatalf("Detach unknown: %v", err)
	}
}

func TestNBDAttacherResumesRecordedAttachments(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	backend.configured[3] = true
	store := newAttachmentStore(dir)
	if err := store.save([]attachmentRecord{
		{Volume: "/vol/live", Index: 3, Size: 1 << 20},
		{Volume: "/vol/gone", Index: 9, Size: 1 << 20}, // device no longer exists
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	a := newTestAttacher(t, backend, dir)

	// The live device is adopted, the stale record dropped.
	if len(backend.adopts) != 1 || backend.adopts[0] != 3 {
		t.Fatalf("adopts = %v, want [3]", backend.adopts)
	}
	// A healthy adopted device must not be kicked into reconnecting.
	if got := backend.session("/vol/live").kicked(); got != 0 {
		t.Fatalf("healthy adopted session kicks = %d, want 0", got)
	}
	if h, ok := a.Health("/vol/live"); !ok || h.Device != "/dev/nbd3" {
		t.Fatalf("Health(/vol/live) = (%+v, %v)", h, ok)
	}
	if _, ok := a.Health("/vol/gone"); ok {
		t.Fatal("the stale record should be gone")
	}
	records, _ := store.load()
	if len(records) != 1 || records[0].Volume != "/vol/live" {
		t.Fatalf("persisted state = %+v, want only /vol/live", records)
	}

	// Attaching the adopted volume returns its device — never a second one.
	dev, err := a.Attach(context.Background(), "/vol/live")
	if err != nil || dev != "/dev/nbd3" {
		t.Fatalf("Attach adopted = (%q, %v), want /dev/nbd3", dev, err)
	}
	if len(backend.attaches) != 0 {
		t.Fatalf("attaches = %v, want none", backend.attaches)
	}
}

func TestNBDAttacherResumeRepairsDeadAdoptedLink(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	backend.configured[4] = true
	backend.verdicts["/dev/nbd4"] = probeStalled // the link died while no plugin was running
	if err := newAttachmentStore(dir).save([]attachmentRecord{{Volume: "/vol/db", Index: 4, Size: 1 << 20}}); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	a := newTestAttacher(t, backend, dir)
	_ = a

	if got := backend.session("/vol/db").kicked(); got != 1 {
		t.Fatalf("dead adopted session kicks = %d, want 1 (the probe must trigger the repair)", got)
	}
}

func TestNBDAttacherAttachAfterRebootReattaches(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	// Recorded attachment whose device vanished (node reboot): adoption must
	// not be attempted; a fresh attach happens on demand.
	if err := newAttachmentStore(dir).save([]attachmentRecord{{Volume: "/vol/db", Index: 5, Size: 1 << 20}}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	a := newTestAttacher(t, backend, dir)

	dev, err := a.Attach(context.Background(), "/vol/db")
	if err != nil || dev != "/dev/nbd0" {
		t.Fatalf("Attach = (%q, %v), want a fresh /dev/nbd0", dev, err)
	}
	if len(backend.adopts) != 0 || len(backend.attaches) != 1 {
		t.Fatalf("adopts=%v attaches=%v, want a fresh attach only", backend.adopts, backend.attaches)
	}
}

func TestNBDAttacherDeadLinkKicksTheRightSession(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	a := newTestAttacher(t, backend, dir)
	ctx := context.Background()

	if _, err := a.Attach(ctx, "/vol/a"); err != nil {
		t.Fatalf("Attach a: %v", err)
	}
	if _, err := a.Attach(ctx, "/vol/b"); err != nil {
		t.Fatalf("Attach b: %v", err)
	}

	backend.deadLinks <- backend.session("/vol/b").index

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if backend.session("/vol/b").kicked() == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := backend.session("/vol/b").kicked(); got != 1 {
		t.Fatalf("session b kicks = %d, want 1", got)
	}
	if got := backend.session("/vol/a").kicked(); got != 0 {
		t.Fatalf("session a kicks = %d, want 0", got)
	}
}

func TestNBDAttacherHealth(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	a := newTestAttacher(t, backend, dir)

	if _, err := a.Attach(context.Background(), "/vol/a"); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	h, ok := a.Health("/vol/a")
	if !ok || h.State != nbdclient.StateHealthy || h.Device != "/dev/nbd0" {
		t.Fatalf("Health = (%+v, %v)", h, ok)
	}
	if _, ok := a.Health("/vol/none"); ok {
		t.Fatal("Health of an unattached volume should report not found")
	}
}

func TestNBDAttacherCloseStopsWithoutDetaching(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	a := newTestAttacher(t, backend, dir)

	if _, err := a.Attach(context.Background(), "/vol/a"); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s := backend.session("/vol/a")
	if s.stops != 1 || s.detaches != 0 {
		t.Fatalf("Close: stops=%d detaches=%d, want supervision stopped and the device left attached", s.stops, s.detaches)
	}
}

func TestNBDAttacherPersistFailureFailsAttach(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	a := newTestAttacher(t, backend, dir)

	// Make the state file unwritable by replacing the directory with a file.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, []byte("not a dir"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := a.Attach(context.Background(), "/vol/a")
	if err == nil {
		t.Fatal("Attach must fail when the attachment cannot be recorded")
	}
	// The failed attach must not leave a half-registered session behind.
	if s := backend.session("/vol/a"); s == nil || s.detaches != 1 {
		t.Fatalf("session = %+v, want it detached after the persist failure", backend.session("/vol/a"))
	}
	if _, ok := a.Health("/vol/a"); ok {
		t.Fatal("a failed attach should leave no health entry")
	}
}

func TestNBDAttacherRejectsBadAddress(t *testing.T) {
	for _, bad := range []string{"", "noport", ":10809", "host:"} {
		if _, err := NewNBDAttacher(bad); err == nil {
			t.Errorf("NewNBDAttacher(%q) should error", bad)
		}
	}
}

func TestAttachmentStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store := newAttachmentStore(dir)

	// Empty state: no file yet.
	records, err := store.load()
	if err != nil || records != nil {
		t.Fatalf("empty load = (%+v, %v)", records, err)
	}

	in := []attachmentRecord{
		{Volume: "/b", Index: 2, Size: 20},
		{Volume: "/a", Index: 1, Size: 10},
	}
	if err := store.save(in); err != nil {
		t.Fatalf("save: %v", err)
	}
	records, err = store.load()
	if err != nil || len(records) != 2 {
		t.Fatalf("load = (%+v, %v)", records, err)
	}
	// Sorted by volume for a stable, diffable file.
	if records[0].Volume != "/a" || records[1].Volume != "/b" {
		t.Fatalf("order = %+v, want sorted by volume", records)
	}

	// Corrupt state surfaces an instructive error.
	if err := os.WriteFile(filepath.Join(dir, "attachments.json"), []byte("{nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.load(); err == nil {
		t.Fatal("corrupt state should error")
	}
}

func TestNBDAttacherResumeSurvivesCorruptState(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "attachments.json"), []byte("{nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend()
	// Construction must succeed: a corrupt state file degrades to "no records",
	// it must not brick the node plugin.
	a := newTestAttacher(t, backend, dir)
	if _, err := a.Attach(context.Background(), "/vol/a"); err != nil {
		t.Fatalf("Attach after corrupt state: %v", err)
	}
}

func TestDeviceConfiguredProbesSysfs(t *testing.T) {
	// The production probe reads /sys/block; on any OS it must simply report
	// false for a device that does not exist.
	if deviceConfigured(4294967294) {
		t.Fatal("a nonexistent device cannot be configured")
	}
}

func TestNewNBDAttacherWithoutBackendNeedsNetlink(t *testing.T) {
	// With no backend option the constructor dials the real NBD netlink family.
	// On the darwin dev box (and CI without the nbd module loaded) that dial
	// fails, which is the branch under test. A host that happens to have a live
	// nbd family is tolerated: close and skip rather than leave it dangling.
	a, err := NewNBDAttacher("127.0.0.1:10809", WithStateDir(t.TempDir()), WithAttacherLogger(quietLogger()))
	if err != nil {
		return
	}
	_ = a.Close()
	t.Skip("host has a live nbd netlink family; the no-backend failure path cannot be exercised here")
}

func TestNBDAttacherResumeKeepsRecordWhenAdoptFails(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	backend.configured[3] = true
	backend.adoptErr = errors.New("device busy")
	if err := newAttachmentStore(dir).save([]attachmentRecord{{Volume: "/vol/db", Index: 3, Size: 1 << 20}}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	a := newTestAttacher(t, backend, dir)

	// A failed adoption keeps the record — in memory and on disk — but registers
	// no session, so the device stays attached and a later publish can retry.
	if backend.session("/vol/db") != nil {
		t.Fatal("a failed adoption must not register a session")
	}
	h, ok := a.Health("/vol/db")
	if !ok || h.State != nbdclient.StateReconnecting || h.Device != "/dev/nbd3" {
		t.Fatalf("Health = (%+v, %v), want the record-only reconnecting branch", h, ok)
	}
	records, _ := newAttachmentStore(dir).load()
	if len(records) != 1 || records[0].Volume != "/vol/db" {
		t.Fatalf("persisted = %+v, want the record kept", records)
	}

	// Publishing it while adoption still fails must surface the actionable error.
	if _, err := a.Attach(context.Background(), "/vol/db"); err == nil || !strings.Contains(err.Error(), "cannot be supervised") {
		t.Fatalf("Attach = %v, want a 'cannot be supervised' error", err)
	}
}

func TestNBDAttacherAttachPropagatesBackendError(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	backend.attachErr = errors.New("silod unreachable")
	a := newTestAttacher(t, backend, dir)

	if _, err := a.Attach(context.Background(), "/vol/db"); err == nil {
		t.Fatal("Attach should propagate the backend attach error")
	}
	if _, ok := a.Health("/vol/db"); ok {
		t.Fatal("a failed attach must record nothing")
	}
	records, _ := newAttachmentStore(dir).load()
	if len(records) != 0 {
		t.Fatalf("records = %+v, want none after a failed attach", records)
	}
}

func TestNBDAttacherDetachDisconnectsUnsupervisedRecord(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	backend.configured[3] = true
	backend.adoptErr = errors.New("cannot adopt yet") // resume keeps a record, no session
	if err := newAttachmentStore(dir).save([]attachmentRecord{{Volume: "/vol/db", Index: 3, Size: 1 << 20}}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	a := newTestAttacher(t, backend, dir)
	ctx := context.Background()

	// First make the kernel Disconnect fail: Detach must surface it and keep the
	// record, so a still-configured device is never silently forgotten.
	backend.setDisconnectErr(errors.New("kernel busy"))
	if err := a.Detach(ctx, "/vol/db"); err == nil {
		t.Fatal("Detach should surface the kernel Disconnect failure")
	}
	if _, ok := a.Health("/vol/db"); !ok {
		t.Fatal("a failed Detach must keep the record")
	}

	// Now let it succeed: an unsupervised but configured record is disconnected
	// by index and removed.
	backend.setDisconnectErr(nil)
	if err := a.Detach(ctx, "/vol/db"); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	if got := backend.disconnectsOf(); len(got) == 0 || got[len(got)-1] != 3 {
		t.Fatalf("kernel disconnects = %v, want device 3 disconnected", got)
	}
	if _, ok := a.Health("/vol/db"); ok {
		t.Fatal("the record must be gone after a successful Detach")
	}
}

func TestNBDAttacherStartWatcherFailureIsNonFatal(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	failWatcher := func(a *NBDAttacher) {
		a.newWatcher = func() (linkWatcher, error) { return nil, errors.New("mcast group unavailable") }
	}
	a, err := NewNBDAttacher(
		"127.0.0.1:10809",
		backend.option(),
		failWatcher, // applied after the backend option, so it wins
		WithStateDir(dir),
		WithAttacherLogger(quietLogger()),
	)
	if err != nil {
		t.Fatalf("a watcher failure must not fail construction: %v", err)
	}
	// Close must not hang when no watcher goroutine ever started.
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAttachmentStoreSaveIntoMissingDirErrors(t *testing.T) {
	store := newAttachmentStore(filepath.Join(t.TempDir(), "does-not-exist"))
	if err := store.save([]attachmentRecord{{Volume: "/v", Index: 1, Size: 1}}); err == nil {
		t.Fatal("save into a missing directory should error")
	}
}

func TestAttachmentStoreLoadWrongShapeErrors(t *testing.T) {
	dir := t.TempDir()
	// Valid JSON, wrong shape: an object where a record array is expected.
	if err := os.WriteFile(filepath.Join(dir, "attachments.json"), []byte(`{"a":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := newAttachmentStore(dir).load()
	if err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("load of wrong-shape JSON = %v, want a corrupt-state error", err)
	}
}

// Compile-time checks: the real session and kernel types satisfy the seams.
var (
	_ nbdSession          = (*nbdclient.Session)(nil)
	_ linkWatcher         = (*nbdnl.Watcher)(nil)
	_ nbdclient.KernelNBD = (*nbdnl.Conn)(nil)
)

func TestNBDAttacherMetrics(t *testing.T) {
	dir := t.TempDir()
	backend := newFakeBackend()
	a := newTestAttacher(t, backend, dir)
	ctx := context.Background()

	if a.MetricPrefix() != "silo_csi" {
		t.Fatalf("prefix = %q", a.MetricPrefix())
	}

	if _, err := a.Attach(ctx, "/vol/a"); err != nil {
		t.Fatalf("Attach a: %v", err)
	}
	if _, err := a.Attach(ctx, "/vol/b"); err != nil {
		t.Fatalf("Attach b: %v", err)
	}
	sa, sb := backend.session("/vol/a"), backend.session("/vol/b")
	sa.mu.Lock()
	sa.state = nbdclient.StateReconnecting
	sa.mu.Unlock()
	sb.mu.Lock()
	sb.reconnects = 3
	sb.mu.Unlock()

	byName := func(ms []metrics.Metric, name string) float64 {
		t.Helper()
		for _, m := range ms {
			if m.Name == name {
				return m.Value
			}
		}
		t.Fatalf("metric %q missing", name)
		return 0
	}
	ms := a.CollectMetrics()
	if v := byName(ms, "nbd_attached_volumes"); v != 2 {
		t.Fatalf("attached = %v, want 2", v)
	}
	if v := byName(ms, "nbd_reconnecting_volumes"); v != 1 {
		t.Fatalf("reconnecting = %v, want 1", v)
	}
	if v := byName(ms, "nbd_reconnects_total"); v != 3 {
		t.Fatalf("reconnects = %v, want 3", v)
	}

	// Detaching a session must not lose its reconnect count: the exported
	// counter may never go backwards.
	if err := a.Detach(ctx, "/vol/b"); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	ms = a.CollectMetrics()
	if v := byName(ms, "nbd_reconnects_total"); v != 3 {
		t.Fatalf("reconnects after detach = %v, want 3", v)
	}
	if v := byName(ms, "nbd_attached_volumes"); v != 1 {
		t.Fatalf("attached after detach = %v, want 1", v)
	}
}

// lockedBuffer is a log sink background goroutines may write to while the
// test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// setLinkCheckTiming tightens the periodic check and the watcher retry for
// one test. It must run before the attacher is built.
func setLinkCheckTiming(t *testing.T, interval, retryFloor, retryCap time.Duration) {
	t.Helper()
	oldInterval, oldFloor, oldCap := linkCheckInterval, watcherRetryFloor, watcherRetryCap
	linkCheckInterval, watcherRetryFloor, watcherRetryCap = interval, retryFloor, retryCap
	t.Cleanup(func() {
		linkCheckInterval, watcherRetryFloor, watcherRetryCap = oldInterval, oldFloor, oldCap
	})
}

// newLoggedAttacher is newTestAttacher with the log captured.
func newLoggedAttacher(t *testing.T, backend *fakeBackend, extra ...NBDAttacherOption) (*NBDAttacher, *lockedBuffer) {
	t.Helper()
	logs := &lockedBuffer{}
	opts := append([]NBDAttacherOption{
		backend.option(),
		WithStateDir(t.TempDir()),
		WithAttacherLogger(slog.New(slog.NewTextHandler(logs, nil))),
		WithReconnectWindow(time.Minute),
	}, extra...)
	a, err := NewNBDAttacher("127.0.0.1:10809", opts...)
	if err != nil {
		t.Fatalf("NewNBDAttacher: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a, logs
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestNBDAttacherRepairIfDead(t *testing.T) {
	for _, tc := range []struct {
		name      string
		verdict   probeVerdict
		state     nbdclient.State
		wantKicks int
		wantLog   string
	}{
		{name: "alive", verdict: probeAlive, wantKicks: 0},
		{name: "stalled link reconnects", verdict: probeStalled, wantKicks: 1, wantLog: "level=WARN msg=\"a volume's connection is dead"},
		{name: "failed device tells the operator to restart the pod", verdict: probeFailed, wantKicks: 1, wantLog: "level=ERROR msg=\"the kernel already failed I/O on this volume"},
		{name: "detached while probing", verdict: probeFailed, state: nbdclient.StateDetached, wantKicks: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := newFakeBackend()
			a, logs := newLoggedAttacher(t, backend)
			s := &fakeSession{index: 6, export: "/vol/db", state: tc.state}
			backend.mu.Lock()
			backend.verdicts[s.Device()] = tc.verdict
			backend.mu.Unlock()

			a.repairIfDead(s)

			if got := s.kicked(); got != tc.wantKicks {
				t.Fatalf("kicks = %d, want %d", got, tc.wantKicks)
			}
			out := logs.String()
			if tc.wantLog == "" {
				if strings.Contains(out, "level=WARN") || strings.Contains(out, "level=ERROR") {
					t.Fatalf("nothing should be reported, got:\n%s", out)
				}
				return
			}
			if !strings.Contains(out, tc.wantLog) {
				t.Fatalf("log lacks %q:\n%s", tc.wantLog, out)
			}
			if !strings.Contains(out, "volume=/vol/db") || !strings.Contains(out, "device=/dev/nbd6") {
				t.Fatalf("log should name the volume and device:\n%s", out)
			}
			if tc.verdict == probeFailed && !strings.Contains(out, "restart the pod") {
				t.Fatalf("a failed device must tell the operator to restart the pod:\n%s", out)
			}
		})
	}
}

// sessionSpec is how the fake backend shapes one attached session.
type sessionSpec struct {
	watched bool
	state   nbdclient.State
}

// attachSessions attaches one volume per entry, each session created with its
// spec's watch and state already in place and its device's probe stalling, so
// a concurrent link check never sees a half-configured session.
func attachSessions(t *testing.T, a *NBDAttacher, backend *fakeBackend, specs map[string]sessionSpec) map[string]*fakeSession {
	t.Helper()
	backend.mu.Lock()
	backend.specs = specs
	for i := range uint32(len(specs)) {
		backend.verdicts[nbdnl.DevicePath(backend.nextIndex+i)] = probeStalled
	}
	backend.mu.Unlock()
	out := make(map[string]*fakeSession, len(specs))
	for vol := range specs {
		if _, err := a.Attach(context.Background(), vol); err != nil {
			t.Fatalf("Attach %s: %v", vol, err)
		}
		out[vol] = backend.session(vol)
	}
	return out
}

func TestNBDAttacherPeriodicLinkCheck(t *testing.T) {
	setLinkCheckTiming(t, 5*time.Millisecond, time.Millisecond, time.Millisecond)
	backend := newFakeBackend()
	a, _ := newLoggedAttacher(t, backend)
	s := attachSessions(t, a, backend, map[string]sessionSpec{
		"/vol/adopted":      {watched: false},
		"/vol/watched":      {watched: true},
		"/vol/reconnecting": {watched: false, state: nbdclient.StateReconnecting},
	})

	waitUntil(t, "the unwatched session to be kicked", func() bool { return s["/vol/adopted"].kicked() > 0 })
	if got := s["/vol/watched"].kicked(); got != 0 {
		t.Fatalf("watched session kicks = %d, want 0: its socket watch covers it", got)
	}
	if got := s["/vol/reconnecting"].kicked(); got != 0 {
		t.Fatalf("reconnecting session kicks = %d, want 0: its supervisor is already on it", got)
	}
}

func TestNBDAttacherCloseStopsPeriodicLinkCheck(t *testing.T) {
	setLinkCheckTiming(t, time.Millisecond, time.Millisecond, time.Millisecond)
	backend := newFakeBackend()
	a, _ := newLoggedAttacher(t, backend)
	s := attachSessions(t, a, backend, map[string]sessionSpec{"/vol/a": {}})
	waitUntil(t, "a periodic kick", func() bool { return s["/vol/a"].kicked() > 0 })

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	after := s["/vol/a"].kicked()
	time.Sleep(20 * time.Millisecond)
	if got := s["/vol/a"].kicked(); got != after {
		t.Fatalf("kicks went from %d to %d after Close; the periodic check is still running", after, got)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("a second Close must be harmless: %v", err)
	}
}

func TestNBDAttacherDroppedNotificationsTriggerSweep(t *testing.T) {
	// The default interval keeps the periodic check out of the way, so every
	// kick here comes from the watcher goroutine.
	backend := newFakeBackend()
	a, logs := newLoggedAttacher(t, backend)
	s := attachSessions(t, a, backend, map[string]sessionSpec{
		"/vol/adopted": {watched: false},
		"/vol/watched": {watched: true},
	})

	backend.watchErrs <- nbdnl.ErrNotificationsDropped
	waitUntil(t, "the sweep to kick the unwatched session", func() bool { return s["/vol/adopted"].kicked() == 1 })
	if got := s["/vol/watched"].kicked(); got != 0 {
		t.Fatalf("watched session kicks = %d, want 0", got)
	}
	if !strings.Contains(logs.String(), "dropped some dead-link notifications") {
		t.Fatalf("the overflow should be logged:\n%s", logs.String())
	}

	// The goroutine keeps consuming notifications after the overflow.
	backend.deadLinks <- s["/vol/watched"].index
	waitUntil(t, "the dead-link kick", func() bool { return s["/vol/watched"].kicked() == 1 })
}

func TestNBDAttacherReopensFailedWatcher(t *testing.T) {
	setLinkCheckTiming(t, time.Hour, time.Millisecond, time.Millisecond)
	backend := newFakeBackend()
	// The initial watch opens; the first re-open attempt fails, the second
	// succeeds.
	backend.watcherErrs = []error{nil, errors.New("netlink busy"), nil}
	a, logs := newLoggedAttacher(t, backend)
	s := attachSessions(t, a, backend, map[string]sessionSpec{
		"/vol/adopted": {watched: false},
		"/vol/watched": {watched: true},
	})

	backend.watchErrs <- errors.New("socket gone")
	// The re-opened watch sweeps for links that died while it was down.
	waitUntil(t, "the post-reopen sweep", func() bool { return s["/vol/adopted"].kicked() == 1 })
	backend.mu.Lock()
	watchers := backend.watchers
	backend.mu.Unlock()
	if watchers != 3 {
		t.Fatalf("newWatcher calls = %d, want 3 (initial, failed retry, success)", watchers)
	}
	for _, want := range []string{"the dead-link watch failed", "could not re-open the dead-link watch", "the dead-link watch is back"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log lacks %q:\n%s", want, logs.String())
		}
	}

	// Notifications flow through the replacement watch.
	backend.deadLinks <- s["/vol/watched"].index
	waitUntil(t, "the dead-link kick", func() bool { return s["/vol/watched"].kicked() == 1 })
}

func TestNBDAttacherCloseDuringWatcherReopen(t *testing.T) {
	for _, tc := range []struct {
		name string
		// reopen runs when the attacher retries the watch.
		reopen func(a *NBDAttacher) (linkWatcher, error)
	}{
		{
			name:   "while waiting to retry",
			reopen: func(*NBDAttacher) (linkWatcher, error) { return nil, errors.New("still failing") },
		},
		{
			name: "right as a new watch opens",
			reopen: func(a *NBDAttacher) (linkWatcher, error) {
				a.closeOnce.Do(func() { close(a.stop) })
				return &fakeWatcher{done: make(chan struct{})}, nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setLinkCheckTiming(t, time.Hour, time.Millisecond, time.Millisecond)
			backend := newFakeBackend()
			var reopened sync.WaitGroup
			reopened.Add(1)
			var once sync.Once
			var mu sync.Mutex
			first := true
			hook := func(att *NBDAttacher) {
				att.newWatcher = func() (linkWatcher, error) {
					mu.Lock()
					isFirst := first
					first = false
					mu.Unlock()
					if isFirst {
						return &fakeWatcher{ch: backend.deadLinks, errs: backend.watchErrs, done: make(chan struct{})}, nil
					}
					defer once.Do(reopened.Done)
					return tc.reopen(att)
				}
			}
			a, _ := newLoggedAttacher(t, backend, hook)
			backend.watchErrs <- errors.New("socket gone")
			reopened.Wait()

			done := make(chan struct{})
			go func() {
				_ = a.Close()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Close hung while the watch was being re-opened")
			}
		})
	}
}

func TestNBDAttacherWithoutWatcherStillChecksLinks(t *testing.T) {
	setLinkCheckTiming(t, time.Millisecond, time.Millisecond, time.Millisecond)
	backend := newFakeBackend()
	noWatcher := func(a *NBDAttacher) { a.newWatcher = nil }
	a, _ := newLoggedAttacher(t, backend, noWatcher)
	s := attachSessions(t, a, backend, map[string]sessionSpec{"/vol/a": {}})
	waitUntil(t, "a periodic kick", func() bool { return s["/vol/a"].kicked() > 0 })
}
