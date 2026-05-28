package chunkstore_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/chunkstore"
)

// FuzzValidateID is the security-critical target: chunk ids arrive from
// clients and become filesystem paths via filepath.Join(root, id+ext).
// The invariant is concrete — every id ValidateID accepts must resolve to
// a file sitting directly inside the data dir, with no separator, no
// traversal, and no absolute path. A violation is a path-traversal hole.
func FuzzValidateID(f *testing.F) {
	for _, seed := range []string{
		"chunk-1", "a_b-C9", "", "../etc/passwd", "a/b", "/abs",
		strings.Repeat("x", 200), ".", "..", "a\x00b", "café",
	} {
		f.Add(seed)
	}

	const root = "/var/lib/silo"
	f.Fuzz(func(t *testing.T, id string) {
		if chunkstore.ValidateID(id) != nil {
			return // rejected ids carry no guarantee, and that is fine
		}
		if strings.ContainsAny(id, `/\`) {
			t.Fatalf("ValidateID accepted an id with a path separator: %q", id)
		}
		p := filepath.Join(root, id+".chunk")
		if filepath.Dir(p) != filepath.Clean(root) {
			t.Fatalf("accepted id %q escapes the data dir: resolved to %s", id, p)
		}
	})
}
