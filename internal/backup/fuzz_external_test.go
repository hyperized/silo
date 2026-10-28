package backup_test

import (
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/blobstore"
)

var (
	fuzzSum     = strings.Repeat("0123456789abcdef", 4)
	fuzzSumForm = regexp.MustCompile(`^[0-9a-f]{64}$`)
	fuzzVersion = regexp.MustCompile(`^[A-Za-z0-9.+_-]{0,64}$`)
)

// A manifest that validates must only name objects a target accepts and chunk
// files that stay inside the restore directory, carry well-formed checksums
// and sizes inside the read limits, list no chunk twice, and have a printable
// version, whatever the bytes in it.
func FuzzManifestValidate(f *testing.F) {
	f.Add(testNode, "namespace/node-a/"+stamp0+".json", "extents/node-a/"+stamp0+"/dm9s.json", "c1", "c2", "v1.2.3", fuzzSum, int64(10))
	f.Add("../x", "namespace/../x/"+stamp0+".json", "extents/../x/"+stamp0+"/a.json", "../../etc/passwd", "c", "v", fuzzSum, int64(1))
	f.Add("a", "namespace/a/"+stamp0+".json\n", "extents/a/"+stamp0+"//a.json", "id\n", "id\n", "v\x1b[31m", fuzzSum+"\n", int64(-1))
	f.Add("a.b", "namespace/a.b/"+stamp0+".json", "extents/a.b/"+stamp0+"/..json", "a/b", "a/b", "", "ABCDEF", int64(0))
	f.Add(testNode, "namespace/node-a/"+stamp0+".json", "extents/node-a/"+stamp0+"/dm9s.json", "same", "same", "dev", fuzzSum, backup.MaxChunkBytes+1)
	f.Add("", "", "", "", "", "", "", int64(0))
	f.Fuzz(func(t *testing.T, node, ns, ext, chunk, chunk2, version, sum string, size int64) {
		m := backup.Manifest{
			Format:      1,
			SiloVersion: version,
			NodeID:      node,
			Namespace:   backup.ObjectRef{Key: ns, Size: size, SHA256: sum},
			Extents:     []backup.ObjectRef{{Key: ext, Size: size, SHA256: sum}},
			Chunks:      []backup.ChunkRef{{ID: chunk, Size: size, SHA256: sum}, {ID: chunk2, Size: size, SHA256: sum}},
		}
		if m.Validate() != nil {
			return
		}
		for _, name := range []string{m.Namespace.Key, m.Extents[0].Key, "chunks/" + chunk, "chunks/" + chunk2} {
			if err := blobstore.ValidateName(name); err != nil {
				t.Fatalf("validated manifest references invalid object name %q: %v", name, err)
			}
		}
		if chunk == chunk2 {
			t.Fatalf("validated manifest lists chunk %q twice", chunk)
		}
		for _, s := range []string{m.Namespace.SHA256, m.Extents[0].SHA256, m.Chunks[0].SHA256, m.Chunks[1].SHA256} {
			if !fuzzSumForm.MatchString(s) {
				t.Fatalf("validated manifest carries checksum %q", s)
			}
		}
		if size < 1 || size > backup.MaxChunkBytes {
			t.Fatalf("validated manifest carries chunk size %d", size)
		}
		if !fuzzVersion.MatchString(version) {
			t.Fatalf("validated manifest carries version %q", version)
		}
		root := filepath.Join(t.TempDir(), "data")
		for _, id := range []string{chunk, chunk2} {
			rel, err := filepath.Rel(root, filepath.Join(root, id+".chunk"))
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				t.Fatalf("chunk id %q escapes the restore directory (rel %q, err %v)", id, rel, err)
			}
		}
	})
}

func FuzzParseManifestName(f *testing.F) {
	for _, s := range []string{"latest", "latest.json", stamp0, stamp0 + ".json", "", "../x", "latest/../x", "a/b", ".."} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		got, err := backup.ParseManifestName(name)
		if err != nil {
			return
		}
		if strings.Contains(got, "/") || strings.Contains(got, "..") {
			t.Fatalf("accepted %q as %q", name, got)
		}
	})
}
