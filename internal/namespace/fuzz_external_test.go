package namespace_test

import (
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

// FuzzNamespaceMergeBytes hardens the decoder for peer state: MergeBytes
// runs on bytes a node receives over the gossip anti-entropy exchange —
// untrusted input — so arbitrary or corrupt bytes must error, never panic,
// and must leave the namespace usable.
func FuzzNamespaceMergeBytes(f *testing.F) {
	seed := namespace.New(hlc.New("seed"))
	_, _ = seed.Mkdir("/d")
	_, _ = seed.Touch("/d/f")
	if b, err := seed.Snapshot(); err == nil {
		f.Add(b)
	}
	f.Add([]byte("{}"))
	f.Add([]byte(""))
	f.Add([]byte(`{"inodes":[{"id":"x","type":99}]}`))
	// Regression: a directory entry that references an inode the payload
	// never includes must not panic List (a dangling reference from corrupt
	// or hostile peer state).
	f.Add([]byte(`{"inodes":[{"id":"root","type":0,"adds":[{"elem":{"Name":"ghost","Inode":"missing"},"tags":[{"wall":1,"node":"a"}]}]}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		ns := namespace.New(hlc.New("self"))
		_ = ns.MergeBytes(data)
		if _, err := ns.List("/"); err != nil {
			t.Fatalf("namespace unusable after MergeBytes: %v", err)
		}
	})
}

// FuzzNamespacePaths throws arbitrary path strings — the form siloctl and
// the gRPC layer hand in — at every path-accepting operation. None may
// panic, and the namespace must stay consistent (root still lists) no
// matter what was thrown at it.
func FuzzNamespacePaths(f *testing.F) {
	for _, p := range []string{"/a/b", "../etc/passwd", "", "/", "a//b/./c", "/a\x00b", "/\u202e"} {
		f.Add(p)
	}
	f.Fuzz(func(t *testing.T, path string) {
		ns := namespace.New(hlc.New("self"))
		_, _ = ns.Mkdir(path)
		_, _ = ns.Touch(path)
		_, _ = ns.List(path)
		_ = ns.Remove(path)
		if _, err := ns.List("/"); err != nil {
			t.Fatalf("root listing broke after path ops on %q: %v", path, err)
		}
	})
}

// FuzzNamespaceConverges drives two replicas through arbitrary create and
// remove sequences with independently advancing clocks, then merges them in
// both directions into fresh replicas. The conflict-resolved listings must
// be identical — that convergence, conflicts and all, is what lets two
// nodes heal a partition without a coordinator.
func FuzzNamespaceConverges(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 1, 1, 0, 2, 2})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		var clkA, clkB int64 = 1, 1
		a := nsAt("a", &clkA)
		b := nsAt("b", &clkB)
		paths := []string{"/x", "/y", "/z"}

		for i := 0; i+2 < len(data); i += 3 {
			ns, clk := a, &clkA
			if data[i]%2 == 1 {
				ns, clk = b, &clkB
			}
			*clk += int64(data[i+1]%5) + 1
			path := paths[int(data[i+2])%len(paths)]
			switch data[i+1] % 3 {
			case 0:
				_, _ = ns.Mkdir(path)
			case 1:
				_, _ = ns.Touch(path)
			default:
				_ = ns.Remove(path)
			}
		}

		ab := nsAt("m", new(int64))
		ab.Merge(a)
		ab.Merge(b)
		ba := nsAt("n", new(int64))
		ba.Merge(b)
		ba.Merge(a)

		listAB, _ := ab.List("/")
		listBA, _ := ba.List("/")
		if x, y := strings.Join(names(listAB), ","), strings.Join(names(listBA), ","); x != y {
			t.Fatalf("replicas diverged after merge: %q vs %q", x, y)
		}
	})
}
