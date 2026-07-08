package csi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/nbdclient"
	"github.com/hyperized/silo/internal/nbdnl"
)

// fakeSession is a supervised attachment the attacher tests drive directly.
type fakeSession struct {
	mu         sync.Mutex
	index      uint32
	size       uint64
	state      nbdclient.State
	kicks      int
	detaches   int
	stops      int
	detachErr  error
	reconnects uint64
}

func (s *fakeSession) Device() string { return nbdnl.DevicePath(s.index) }
func (s *fakeSession) Index() uint32  { return s.index }
func (s *fakeSession) Size() uint64   { return s.size }

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
	mu          sync.Mutex
	nextIndex   uint32
	attaches    []string
	adopts      []uint32
	sessions    map[string]*fakeSession
	attachErr   error
	adoptErr    error
	configured  map[uint32]bool
	deadLinks   chan uint32
	disconnects []uint32
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		sessions:   map[string]*fakeSession{},
		configured: map[uint32]bool{},
		deadLinks:  make(chan uint32),
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
		s := &fakeSession{index: b.nextIndex, size: 1 << 20}
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
		s := &fakeSession{index: index, size: size}
		b.sessions[cfg.Export] = s
		return s, nil
	}
	watcher := func() (linkWatcher, error) {
		return &fakeWatcher{ch: b.deadLinks, done: make(chan struct{})}, nil
	}
	configured := func(index uint32) bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.configured[index]
	}
	return withNBDBackend(&fakeKernelNBD{b: b}, watcher, attach, adopt, configured)
}

func (b *fakeBackend) session(volume string) *fakeSession {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sessions[volume]
}

type fakeKernelNBD struct{ b *fakeBackend }

func (k *fakeKernelNBD) Connect(nbdnl.ConnectConfig) (uint32, error) { return 0, nil }
func (k *fakeKernelNBD) Reconfigure(uint32, int, time.Duration, time.Duration) error {
	return nil
}

func (k *fakeKernelNBD) Disconnect(index uint32) error {
	k.b.mu.Lock()
	defer k.b.mu.Unlock()
	k.b.disconnects = append(k.b.disconnects, index)
	return nil
}
func (k *fakeKernelNBD) Connected(uint32) (bool, error) { return true, nil }

type fakeWatcher struct {
	ch     chan uint32
	closed sync.Once
	done   chan struct{}
}

func (w *fakeWatcher) Next() (uint32, error) {
	select {
	case idx := <-w.ch:
		return idx, nil
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

// Compile-time checks: the real session and kernel types satisfy the seams.
var (
	_ nbdSession          = (*nbdclient.Session)(nil)
	_ linkWatcher         = (*nbdnl.Watcher)(nil)
	_ nbdclient.KernelNBD = (*nbdnl.Conn)(nil)
)
