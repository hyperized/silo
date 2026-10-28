package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/diskusage"
	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

func TestForEach(t *testing.T) {
	boom := errors.New("boom")
	ctx := context.Background()

	t.Run("no items", func(t *testing.T) {
		if err := forEach(ctx, 3, nil, func(context.Context, string) error { return errors.New("called") }); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("visits every item once", func(t *testing.T) {
		var mu sync.Mutex
		seen := map[string]int{}
		items := []string{"a", "b", "c", "d", "e"}
		err := forEach(ctx, 100, items, func(_ context.Context, s string) error {
			mu.Lock()
			defer mu.Unlock()
			seen[s]++
			return nil
		})
		if err != nil || len(seen) != 5 {
			t.Fatalf("seen = %v err = %v", seen, err)
		}
		for k, n := range seen {
			if n != 1 {
				t.Fatalf("%s visited %d times", k, n)
			}
		}
	})
	t.Run("never exceeds the limit", func(t *testing.T) {
		var inflight, peak atomic.Int64
		items := make([]string, 40)
		err := forEach(ctx, 3, items, func(context.Context, string) error {
			cur := inflight.Add(1)
			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}
			inflight.Add(-1)
			return nil
		})
		if err != nil || peak.Load() > 3 {
			t.Fatalf("peak = %d err = %v", peak.Load(), err)
		}
	})
	t.Run("first error cancels the rest", func(t *testing.T) {
		var calls atomic.Int64
		items := make([]string, 100)
		err := forEach(ctx, 1, items, func(_ context.Context, _ string) error {
			calls.Add(1)
			return fmt.Errorf("wrapped: %w", boom)
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if calls.Load() >= 100 {
			t.Fatal("work was not cancelled")
		}
	})
	t.Run("parent cancelled", func(t *testing.T) {
		parent, cancel := context.WithCancel(ctx)
		cancel()
		err := forEach(parent, 2, []string{"a", "b"}, func(context.Context, string) error { return nil })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestChunkIDFromKey(t *testing.T) {
	cases := []struct {
		key  string
		id   string
		want bool
	}{
		{"chunks/abc_-1", "abc_-1", true},
		{"chunks/a/b", "", false},
		{"chunks/", "", false},
		{"chunks/a.b", "", false},
		{"other/abc", "", false},
		{"chunks/../x", "", false},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			id, ok := chunkIDFromKey(c.key)
			if id != c.id || ok != c.want {
				t.Fatalf("got %q, %v", id, ok)
			}
		})
	}
}

func TestRunStampAndIsStampJSON(t *testing.T) {
	const s = "2026-10-28T12:00:00.123Z"
	stamps := []struct{ in, want string }{
		{s + ".json", s},
		{s + "/dm9s.json", s},
		{s, ""},
		{"x.json", ""},
		{"legacy/x.json", ""},
		{"", ""},
	}
	for _, c := range stamps {
		if got := runStamp(c.in); got != c.want {
			t.Errorf("runStamp(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	for in, want := range map[string]bool{s + ".json": true, s: false, "latest.json": false, ".json": false} {
		if got := isStampJSON(in); got != want {
			t.Errorf("isStampJSON(%q) = %v", in, got)
		}
	}
}

func TestParseManifestKey(t *testing.T) {
	const s = "2026-10-28T12:00:00.123Z"
	cases := []struct {
		key        string
		node, name string
		ok         bool
	}{
		{"manifests/n1/" + s + ".json", "n1", s, true},
		{"manifests/n1/latest.json", "n1", "latest", true},
		{"manifests/n1/latest", "", "", false},
		{"manifests/n1/x.json", "", "", false},
		{"manifests/n1", "", "", false},
		{"manifests/-bad/latest.json", "", "", false},
		{"other/n1/latest.json", "", "", false},
		{"manifests/n1/a/latest.json", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			node, name, ok := parseManifestKey(c.key)
			if node != c.node || name != c.name || ok != c.ok {
				t.Fatalf("got %q %q %v", node, name, ok)
			}
		})
	}
}

func TestRunError(t *testing.T) {
	got := runError(fmt.Errorf("x: %w", context.Canceled), "mem://t")
	if want := "shutting down"; !contains(got, want) {
		t.Fatalf("%q does not mention %q", got, want)
	}
	got = runError(errors.New("disk full"), "mem://t")
	for _, want := range []string{"disk full", "siloctl backup run", "mem://t"} {
		if !contains(got, want) {
			t.Fatalf("%q does not mention %q", got, want)
		}
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

type listFailTarget struct {
	blobstore.Target
	failPrefix string
}

func (l listFailTarget) Name() string { return "stub://t" }

func (l listFailTarget) List(_ context.Context, prefix string) ([]blobstore.ObjectInfo, error) {
	if prefix == l.failPrefix {
		return nil, errors.New("denied")
	}
	return []blobstore.ObjectInfo{{Name: prefix + "x"}}, nil
}

func TestListSizes(t *testing.T) {
	ctx := context.Background()
	got, err := listSizes(ctx, listFailTarget{failPrefix: "none"}, "a/", "b/")
	if _, okA := got["a/x"]; err != nil || !okA || len(got) != 2 {
		t.Fatalf("got %v err %v", got, err)
	}
	if _, err := listSizes(ctx, listFailTarget{failPrefix: "b/"}, "a/", "b/"); err == nil {
		t.Fatal("want an error")
	}
}

func TestClaim(t *testing.T) {
	s := NewSubsystem(nil, nil, 0, nil)
	if !s.claim() || s.claim() {
		t.Fatal("claim should succeed once")
	}
	if err := s.claimManual(); !errors.Is(err, ErrRunInProgress) {
		t.Fatalf("claimManual while running = %v", err)
	}
	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
	if err := s.claimManual(); err != nil {
		t.Fatalf("claimManual = %v", err)
	}
	s.runs.Done()
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	if err := s.claimManual(); !errors.Is(err, ErrStopped) {
		t.Fatalf("claimManual after stop = %v", err)
	}
}

func TestSanitizeVersion(t *testing.T) {
	cases := []struct{ in, want string }{
		{"v1.2.3", "v1.2.3"},
		{"a+b_c-d.e", "a+b_c-d.e"},
		{"has space", "has_space"},
		{"esc\x1b[0m", "esc__0m"},
		{"\u00fcber", "_ber"},
		{strings.Repeat("x", 65), strings.Repeat("x", 64)},
		{strings.Repeat("\u00fc", 70), strings.Repeat("_", 64)},
		{"", ""},
	}
	for _, c := range cases {
		if got := sanitizeVersion(c.in); got != c.want {
			t.Errorf("sanitizeVersion(%q) = %q, want %q", c.in, got, c.want)
		}
		if !versionPattern.MatchString(sanitizeVersion(c.in)) {
			t.Errorf("sanitizeVersion(%q) does not satisfy the manifest rule", c.in)
		}
	}
}

func TestOldEnough(t *testing.T) {
	cutoff := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		mod  time.Time
		want bool
	}{
		{"older", cutoff.Add(-time.Second), true},
		{"exactly at the cutoff", cutoff, false},
		{"newer", cutoff.Add(time.Second), false},
		{"unknown age", time.Time{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := oldEnough(blobstore.ObjectInfo{ModTime: c.mod}, cutoff); got != c.want {
				t.Fatalf("oldEnough = %v, want %v", got, c.want)
			}
		})
	}
}

func TestManifestExpired(t *testing.T) {
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	opts := PruneOptions{Now: now, RetainFor: 24 * time.Hour}
	if !manifestExpired("2026-10-28T00:00:00.000Z", opts) {
		t.Error("a manifest older than RetainFor should expire")
	}
	if manifestExpired("2026-10-29T12:00:00.000Z", opts) {
		t.Error("a manifest younger than RetainFor should be kept")
	}
	if manifestExpired("not-a-stamp", opts) {
		t.Error("a name without a readable time must never expire")
	}
	opts.RetainFor = 0
	if !manifestExpired("not-a-stamp", opts) {
		t.Error("with the floor disabled every candidate expires")
	}
}

func TestStampTime(t *testing.T) {
	if got, ok := stampTime("2026-10-28T12:00:00.123Z"); !ok || got.Nanosecond() != 123_000_000 {
		t.Fatalf("stampTime = %v, %v", got, ok)
	}
	// Matches the pattern but is not a calendar date.
	for _, s := range []string{"", "latest", "2026-13-45T25:61:61.000Z"} {
		if _, ok := stampTime(s); ok {
			t.Errorf("stampTime(%q) accepted", s)
		}
	}
}

func TestCheckFreeSpace(t *testing.T) {
	swapMeasureDisk(t, func(string) (diskusage.Usage, error) { return diskusage.Usage{AvailableBytes: 100}, nil })
	if err := checkFreeSpace("/d", 100, nil); err != nil {
		t.Fatalf("exactly enough space: %v", err)
	}
	err := checkFreeSpace("/d", 101, nil)
	if !errors.Is(err, ErrInsufficientSpace) || !strings.Contains(err.Error(), "free up space") {
		t.Fatalf("err = %v", err)
	}

	swapMeasureDisk(t, func(string) (diskusage.Usage, error) { return diskusage.Usage{}, errors.New("no statfs") })
	var notices []string
	if err := checkFreeSpace("/d", 5, func(s string) { notices = append(notices, s) }); err != nil {
		t.Fatalf("measure failure must not block: %v", err)
	}
	if len(notices) != 1 || !strings.Contains(notices[0], "no statfs") || !strings.Contains(notices[0], "/d") {
		t.Fatalf("notices = %v", notices)
	}
}

func swapMeasureDisk(t *testing.T, fn func(string) (diskusage.Usage, error)) {
	t.Helper()
	orig := measureDisk
	measureDisk = fn
	t.Cleanup(func() { measureDisk = orig })
}

type staticChunks map[string][]byte

func (s staticChunks) List(context.Context) ([]string, error) {
	out := make([]string, 0, len(s))
	for id := range s {
		out = append(out, id)
	}
	return out, nil
}

func (s staticChunks) RawChunk(_ context.Context, id string) ([]byte, error) { return s[id], nil }

// localBackup exports two chunks to a local target and returns the target and
// the manifest.
func localBackup(t *testing.T) (blobstore.Target, *Manifest) {
	t.Helper()
	tgt, err := blobstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ns := namespace.New(hlc.New("node-a"))
	if _, err := ns.Mkdir("/docs"); err != nil {
		t.Fatal(err)
	}
	e := NewExporter(staticChunks{"c1": []byte("one"), "c2": []byte("two")}, ns, "node-a")
	res, err := e.Export(context.Background(), tgt)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	m, err := LoadManifest(context.Background(), tgt, "node-a", res.Stamp)
	if err != nil {
		t.Fatal(err)
	}
	return tgt, m
}

func TestRestoreFreeSpace(t *testing.T) {
	ctx := context.Background()
	t.Run("not enough space writes nothing", func(t *testing.T) {
		tgt, m := localBackup(t)
		var asked string
		swapMeasureDisk(t, func(p string) (diskusage.Usage, error) {
			asked = p
			return diskusage.Usage{AvailableBytes: m.TotalBytes() + spaceMargin - 1}, nil
		})
		dir := filepath.Join(t.TempDir(), "restore")
		_, err := Restore(ctx, tgt, m, dir, RestoreOptions{})
		if !errors.Is(err, ErrInsufficientSpace) {
			t.Fatalf("err = %v, want ErrInsufficientSpace", err)
		}
		if asked != dir {
			t.Fatalf("measured %q, want %q", asked, dir)
		}
		entries, readErr := os.ReadDir(dir)
		if readErr != nil || len(entries) != 0 {
			t.Fatalf("restore directory = %v, %v; want it empty", entries, readErr)
		}
	})
	t.Run("exactly enough space", func(t *testing.T) {
		tgt, m := localBackup(t)
		swapMeasureDisk(t, func(string) (diskusage.Usage, error) {
			return diskusage.Usage{AvailableBytes: m.TotalBytes() + spaceMargin}, nil
		})
		if _, err := Restore(ctx, tgt, m, filepath.Join(t.TempDir(), "restore"), RestoreOptions{}); err != nil {
			t.Fatalf("Restore: %v", err)
		}
	})
	t.Run("measurement failure raises a notice and continues", func(t *testing.T) {
		tgt, m := localBackup(t)
		swapMeasureDisk(t, func(string) (diskusage.Usage, error) { return diskusage.Usage{}, errors.New("unsupported platform") })
		var notices []string
		dir := filepath.Join(t.TempDir(), "restore")
		res, err := Restore(ctx, tgt, m, dir, RestoreOptions{Notice: func(s string) { notices = append(notices, s) }})
		if err != nil || res.Chunks != 2 {
			t.Fatalf("res = %+v err = %v", res, err)
		}
		if len(notices) != 1 || !strings.Contains(notices[0], "unsupported platform") || !strings.Contains(notices[0], dir) {
			t.Fatalf("notices = %v", notices)
		}
	})
	t.Run("measurement failure without a notice callback", func(t *testing.T) {
		tgt, m := localBackup(t)
		swapMeasureDisk(t, func(string) (diskusage.Usage, error) { return diskusage.Usage{}, errors.New("unsupported platform") })
		if _, err := Restore(ctx, tgt, m, filepath.Join(t.TempDir(), "restore"), RestoreOptions{}); err != nil {
			t.Fatalf("Restore: %v", err)
		}
	})
}

func TestCheckMetadataSize(t *testing.T) {
	for _, tc := range []struct {
		name    string
		n       int
		wantErr bool
	}{
		{"empty", 0, false},
		{"at limit", int(MaxMetadataBytes), false},
		{"over limit", int(MaxMetadataBytes) + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkMetadataSize("the namespace snapshot", tc.n)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
