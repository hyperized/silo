package namespace_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

// nsAt builds a namespace whose clock reads *nanos, so tests can drive HLC
// ordering deterministically (and give competing nodes distinct ids).
func nsAt(node string, nanos *int64) *namespace.Namespace {
	return namespace.New(hlc.New(node, hlc.WithNow(func() time.Time { return time.Unix(0, *nanos) })))
}

func names(entries []namespace.ResolvedEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Name
	}
	return out
}

func TestNamespace_MkdirTouchList(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)

	if _, err := ns.Mkdir("/docs"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	clk++
	if _, err := ns.Touch("/docs/readme"); err != nil {
		t.Fatalf("Touch: %v", err)
	}

	root, err := ns.List("/")
	if err != nil {
		t.Fatalf("List /: %v", err)
	}
	if got := names(root); len(got) != 1 || got[0] != "docs" {
		t.Fatalf("List / = %v, want [docs]", got)
	}
	if root[0].Type != namespace.Dir {
		t.Errorf("docs type = %s, want dir", root[0].Type)
	}

	docs, err := ns.List("/docs")
	if err != nil {
		t.Fatalf("List /docs: %v", err)
	}
	if got := names(docs); len(got) != 1 || got[0] != "readme" {
		t.Fatalf("List /docs = %v, want [readme]", got)
	}
	if docs[0].Type != namespace.File {
		t.Errorf("readme type = %s, want file", docs[0].Type)
	}
}

func TestNamespace_Errors(t *testing.T) {
	var clk int64 = 1
	ns := nsAt("a", &clk)

	cases := []struct {
		name string
		do   func() error
		want string
	}{
		{"create root", func() error { _, err := ns.Mkdir("/"); return err }, "cannot create the root"},
		{"remove root", func() error { return ns.Remove("/") }, "cannot remove the root"},
		{"parent missing", func() error { _, err := ns.Mkdir("/nope/child"); return err }, "does not exist"},
		{"parent traversal", func() error { _, err := ns.Touch("/a/../b"); return err }, `".."`},
		{"list missing", func() error { _, err := ns.List("/missing"); return err }, "does not exist"},
		{"list traversal", func() error { _, err := ns.List("/a/../b"); return err }, `".."`},
		{"remove missing", func() error { return ns.Remove("/missing") }, "does not exist"},
		{"remove traversal", func() error { return ns.Remove("/a/../b") }, `".."`},
		{"remove under missing parent", func() error { return ns.Remove("/nope/child") }, "does not exist"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.do()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want error containing %q", err, tc.want)
			}
		})
	}

	// Touch then attempt to descend into the file: not a directory.
	clk++
	if _, err := ns.Touch("/file"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if _, err := ns.Mkdir("/file/child"); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Errorf("descending into a file should fail, got %v", err)
	}
	// Creating an existing name fails.
	clk++
	if _, err := ns.Touch("/file"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("re-create should fail, got %v", err)
	}
}

func TestNamespace_Remove(t *testing.T) {
	var clk int64 = 1
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.Touch("/gone"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if err := ns.Remove("/gone"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	root, _ := ns.List("/")
	if len(root) != 0 {
		t.Errorf("List after Remove = %v, want empty", names(root))
	}
	if err := ns.Remove("/gone"); err == nil {
		t.Error("removing an absent entry should error")
	}
}

func TestNamespace_ConcurrentCreateSurfacesConflict(t *testing.T) {
	// Node a claims /report at a higher HLC than node b. After merging,
	// a's claim keeps the bare name and b's is surfaced as a conflict.
	var clkA int64 = 200
	var clkB int64 = 100
	a := nsAt("a", &clkA)
	b := nsAt("b", &clkB)

	if _, err := a.Touch("/report"); err != nil {
		t.Fatalf("a.Touch: %v", err)
	}
	if _, err := b.Touch("/report"); err != nil {
		t.Fatalf("b.Touch: %v", err)
	}

	a.Merge(b)
	listing, err := a.List("/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listing) != 2 {
		t.Fatalf("expected 2 entries (primary + conflict), got %v", names(listing))
	}

	var primary, conflict *namespace.ResolvedEntry
	for i := range listing {
		if listing[i].Conflict {
			conflict = &listing[i]
		} else {
			primary = &listing[i]
		}
	}
	if primary == nil || primary.Name != "report" {
		t.Fatalf("primary entry = %+v, want bare name 'report'", primary)
	}
	if conflict == nil || !strings.HasPrefix(conflict.Name, "report.conflict-") {
		t.Fatalf("conflict entry = %+v, want a 'report.conflict-' name", conflict)
	}
	if primary.Inode == conflict.Inode {
		t.Error("primary and conflict must be distinct inodes")
	}
}

func TestNamespace_MergeConvergesBothDirections(t *testing.T) {
	var clkA int64 = 10
	var clkB int64 = 20
	a := nsAt("a", &clkA)
	b := nsAt("b", &clkB)

	clkA++
	a.Mkdir("/shared")
	clkA++
	a.Touch("/onlyA")
	clkB++
	b.Touch("/onlyB")
	clkB++
	b.Touch("/shared") // collides with a's dir -> conflict after merge

	ab := nsAt("x", new(int64))
	ab.Merge(a)
	ab.Merge(b)

	ba := nsAt("y", new(int64))
	ba.Merge(b)
	ba.Merge(a)

	if got1, got2 := names(mustList(t, ab, "/")), names(mustList(t, ba, "/")); strings.Join(got1, ",") != strings.Join(got2, ",") {
		t.Fatalf("replicas diverged: %v vs %v", got1, got2)
	}
}

func TestNamespace_MergeIsIdempotent(t *testing.T) {
	var clkA int64 = 5
	a := nsAt("a", &clkA)
	clkA++
	a.Mkdir("/d")

	b := nsAt("b", new(int64))
	b.Merge(a)
	before := names(mustList(t, b, "/"))
	b.Merge(a) // applying the same state again must not change anything
	if after := names(mustList(t, b, "/")); strings.Join(before, ",") != strings.Join(after, ",") {
		t.Errorf("merge not idempotent: %v then %v", before, after)
	}
}

func TestNamespace_SnapshotMergeBytesConverges(t *testing.T) {
	var clkA int64 = 100
	a := nsAt("a", &clkA)
	clkA++
	a.Mkdir("/dir")
	clkA++
	a.Touch("/dir/file")
	clkA++
	a.Touch("/top")

	state, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// A fresh replica that only ever sees the serialized bytes converges.
	b := nsAt("b", new(int64))
	if err := b.MergeBytes(state); err != nil {
		t.Fatalf("MergeBytes: %v", err)
	}

	ra := names(mustList(t, a, "/"))
	rb := names(mustList(t, b, "/"))
	if strings.Join(ra, ",") != strings.Join(rb, ",") {
		t.Fatalf("root diverged after byte merge: %v vs %v", ra, rb)
	}
	if got := names(mustList(t, b, "/dir")); len(got) != 1 || got[0] != "file" {
		t.Errorf("nested dir not reconstructed from bytes: %v", got)
	}
}

func TestNamespace_MergeBytesRejectsGarbage(t *testing.T) {
	b := nsAt("b", new(int64))
	if err := b.MergeBytes([]byte("not json")); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("got %v, want a decode error", err)
	}
}

func TestNamespace_GC(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.Touch("/tmp"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	clk += 10
	if err := ns.Remove("/tmp"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// A long retention keeps the tombstone (it may not have propagated).
	clk += 5
	if got := ns.GC(time.Hour); got != 0 {
		t.Errorf("GC with a long retention reclaimed %d, want 0", got)
	}

	// Advancing the clock far past the removal and using a tiny retention
	// reclaims it.
	clk += 1_000_000
	if got := ns.GC(time.Nanosecond); got == 0 {
		t.Error("GC should reclaim the /tmp tombstone once it is old enough")
	}
	// The entry stays removed regardless.
	if root := names(mustList(t, ns, "/")); len(root) != 0 {
		t.Errorf("root not empty after GC: %v", root)
	}
}

func TestNamespace_RunGC(t *testing.T) {
	var clk int64 = 1
	ns := nsAt("a", &clk)
	clk++
	ns.Touch("/x")
	clk += 10
	ns.Remove("/x")
	clk += 1_000_000 // now far past the removal so a sweep reclaims it
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// A non-positive interval falls back to the default; an already-cancelled
	// context returns immediately.
	cancelled, cancel0 := context.WithCancel(context.Background())
	cancel0()
	ns.RunGC(cancelled, time.Hour, 0, logger)

	// A live loop ticks and sweeps until cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ns.RunGC(ctx, time.Nanosecond, time.Millisecond, logger); close(done) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunGC did not stop after ctx cancel")
	}
}

func TestInodeType_String(t *testing.T) {
	if namespace.Dir.String() != "dir" || namespace.File.String() != "file" || namespace.Volume.String() != "volume" {
		t.Errorf("type strings: dir=%q file=%q volume=%q",
			namespace.Dir.String(), namespace.File.String(), namespace.Volume.String())
	}
}

func mustList(t *testing.T, ns *namespace.Namespace, path string) []namespace.ResolvedEntry {
	t.Helper()
	entries, err := ns.List(path)
	if err != nil {
		t.Fatalf("List %s: %v", path, err)
	}
	return entries
}
