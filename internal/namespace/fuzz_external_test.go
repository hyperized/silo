package namespace_test

import (
	"strings"
	"testing"
)

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
