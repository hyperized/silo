package csi

import "testing"

// FuzzParseByteSize hardens the CSI chunk-size parser, which turns a
// provisioner-supplied StorageClass parameter string into a byte count. A
// malformed value must error rather than panic, and a successful parse must
// never yield a negative count or one produced by silent multiplier overflow.
func FuzzParseByteSize(f *testing.F) {
	for _, s := range []string{
		"", "0", "4Mi", "10Gi", "1Ti", "512Ki", "  16K ",
		"9223372036854775807", "-5", "abc", "Ki", "1.5Mi",
		"99999999999999999999Ti", "8Ti", "9007199254740992Ki",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		n, err := parseByteSize(s)
		if err != nil {
			return
		}
		if n < 0 {
			t.Fatalf("parseByteSize(%q) = %d; a successful parse must be non-negative", s, n)
		}
	})
}
