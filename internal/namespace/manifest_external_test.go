package namespace_test

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

func TestNamespace_ManifestAppendAndRead(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.Touch("/f"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	for _, c := range []string{"c0", "c1", "c2"} {
		clk++ // advance the HLC so append order is well-defined
		if err := ns.AppendChunk("/f", c); err != nil {
			t.Fatalf("AppendChunk %s: %v", c, err)
		}
	}
	got, err := ns.Manifest("/f")
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"c0", "c1", "c2"}) {
		t.Errorf("manifest = %v, want [c0 c1 c2]", got)
	}
}

func TestNamespace_AppendChunkErrors(t *testing.T) {
	var clk int64 = 1
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.Mkdir("/dir"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	cases := []struct {
		name string
		do   func() error
		want string
	}{
		{"missing file", func() error { return ns.AppendChunk("/nope", "c") }, "does not exist"},
		{"on a directory", func() error { return ns.AppendChunk("/dir", "c") }, "not a file"},
		{"root", func() error { return ns.AppendChunk("/", "c") }, "not a file"},
		{"traversal", func() error { return ns.AppendChunk("/a/../b", "c") }, `".."`},
		{"missing parent", func() error { return ns.AppendChunk("/x/y", "c") }, "does not exist"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.do(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
	// Manifest shares the resolver; spot-check that its errors surface too.
	if _, err := ns.Manifest("/dir"); err == nil || !strings.Contains(err.Error(), "not a file") {
		t.Errorf("Manifest on a directory: got %v", err)
	}
}

func TestNamespace_ManifestConvergesAcrossReplicas(t *testing.T) {
	var clkA, clkB int64 = 10, 20
	a := nsAt("a", &clkA)
	clkA++
	if _, err := a.Touch("/log"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	clkA++
	if err := a.AppendChunk("/log", "a0"); err != nil {
		t.Fatalf("a append: %v", err)
	}

	// b learns the file from a, then appends its own chunk to the same inode.
	state, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	b := nsAt("b", &clkB)
	if err := b.MergeBytes(state); err != nil {
		t.Fatalf("b merge: %v", err)
	}
	clkB++
	if err := b.AppendChunk("/log", "b0"); err != nil {
		t.Fatalf("b append: %v", err)
	}

	// a merges b's state back; the manifest holds both, in HLC order.
	bState, err := b.Snapshot()
	if err != nil {
		t.Fatalf("b snapshot: %v", err)
	}
	if err := a.MergeBytes(bState); err != nil {
		t.Fatalf("a merge: %v", err)
	}
	got, err := a.Manifest("/log")
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a0", "b0"}) {
		t.Errorf("converged manifest = %v, want [a0 b0]", got)
	}
}

func TestNamespace_ManifestPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ns.json")
	first, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := first.Touch("/f"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	for _, c := range []string{"c0", "c1"} {
		if err := first.AppendChunk("/f", c); err != nil {
			t.Fatalf("AppendChunk %s: %v", c, err)
		}
	}

	second, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := second.Manifest("/f")
	if err != nil {
		t.Fatalf("Manifest after reopen: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"c0", "c1"}) {
		t.Errorf("persisted manifest = %v, want [c0 c1]", got)
	}
}
