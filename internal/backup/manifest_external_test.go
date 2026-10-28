package backup_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/backup"
)

func TestValidateNodeID(t *testing.T) {
	cases := []struct {
		id string
		ok bool
	}{
		{"node-a", true},
		{"n", true},
		{"0.node_1-x", true},
		{strings.Repeat("a", 128), true},
		{strings.Repeat("a", 129), false},
		{"", false},
		{"-lead", false},
		{".hidden", false},
		{"a/b", false},
		{"../x", false},
		{"a b", false},
		{"a\nb", false},
		{"nöde", false},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			err := backup.ValidateNodeID(c.id)
			if (err == nil) != c.ok {
				t.Fatalf("ValidateNodeID(%q) = %v, want ok=%v", c.id, err, c.ok)
			}
			if err != nil && !errors.Is(err, backup.ErrInvalidNodeID) {
				t.Fatalf("error does not wrap ErrInvalidNodeID: %v", err)
			}
		})
	}
}

func TestParseManifestName(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"latest", "latest", true},
		{"latest.json", "latest", true},
		{stamp0, stamp0, true},
		{stamp0 + ".json", stamp0, true},
		{"", "", false},
		{"../x", "", false},
		{"2026-10-28T12:00:00Z", "", false},
		{"latest/../x", "", false},
		{"latest.json.json", "", false},
		{stamp0 + "x", "", false},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := backup.ParseManifestName(c.in)
			if (err == nil) != c.ok || got != c.want {
				t.Fatalf("ParseManifestName(%q) = %q, %v; want %q ok=%v", c.in, got, err, c.want, c.ok)
			}
			if err != nil && !errors.Is(err, backup.ErrInvalidManifestName) {
				t.Fatalf("error does not wrap ErrInvalidManifestName: %v", err)
			}
		})
	}
}

func TestManifestKey(t *testing.T) {
	if got := backup.ManifestKey("n1", "latest"); got != "manifests/n1/latest.json" {
		t.Fatalf("ManifestKey = %q", got)
	}
}

var validSum = strings.Repeat("ab", 32)

func validManifest() backup.Manifest {
	return backup.Manifest{
		Format:      1,
		SiloVersion: "v1.2.3+build_4-rc",
		NodeID:      testNode,
		StartedAt:   t0,
		Namespace:   backup.ObjectRef{Key: "namespace/node-a/" + stamp0 + ".json", Size: 10, SHA256: validSum},
		Extents:     []backup.ObjectRef{{Key: "extents/node-a/" + stamp0 + "/dm9sLTE.json", Size: 5, SHA256: validSum}},
		Chunks:      []backup.ChunkRef{{ID: "c1", Size: 3, SHA256: validSum}, {ID: "chunk_2-x", Size: 4, SHA256: validSum}},
	}
}

func TestManifestValidate(t *testing.T) {
	setSum := func(sum string) func(*backup.Manifest) {
		return func(m *backup.Manifest) {
			m.Namespace.SHA256 = sum
			m.Extents[0].SHA256 = sum
			m.Chunks[0].SHA256 = sum
		}
	}
	cases := []struct {
		name   string
		mutate func(*backup.Manifest)
		ok     bool
	}{
		{"valid", func(*backup.Manifest) {}, true},
		{"valid without extents or chunks", func(m *backup.Manifest) { m.Extents, m.Chunks = nil, nil }, true},
		{"format 0", func(m *backup.Manifest) { m.Format = 0 }, false},
		{"format 2", func(m *backup.Manifest) { m.Format = 2 }, false},
		{"bad node id", func(m *backup.Manifest) { m.NodeID = "../x" }, false},
		{"empty version", func(m *backup.Manifest) { m.SiloVersion = "" }, true},
		{"version of 64 characters", func(m *backup.Manifest) { m.SiloVersion = strings.Repeat("v", 64) }, true},
		{"version of 65 characters", func(m *backup.Manifest) { m.SiloVersion = strings.Repeat("v", 65) }, false},
		{"version with an escape character", func(m *backup.Manifest) { m.SiloVersion = "v1\x1b[31m" }, false},
		{"version with a space", func(m *backup.Manifest) { m.SiloVersion = "v 1" }, false},
		{"version with a newline", func(m *backup.Manifest) { m.SiloVersion = "v1\n" }, false},
		{"namespace of another node", func(m *backup.Manifest) { m.Namespace.Key = "namespace/node-b/" + stamp0 + ".json" }, false},
		{"namespace outside the directory", func(m *backup.Manifest) { m.Namespace.Key = "elsewhere/" + stamp0 + ".json" }, false},
		{"namespace with bad stamp", func(m *backup.Manifest) { m.Namespace.Key = "namespace/node-a/latest.json" }, false},
		{"namespace nested", func(m *backup.Manifest) { m.Namespace.Key = "namespace/node-a/x/" + stamp0 + ".json" }, false},
		{"extent of another node", func(m *backup.Manifest) { m.Extents[0].Key = "extents/node-b/" + stamp0 + "/dm9s.json" }, false},
		{"extent without stamp directory", func(m *backup.Manifest) { m.Extents[0].Key = "extents/node-a/dm9s.json" }, false},
		{"extent with bad stamp", func(m *backup.Manifest) { m.Extents[0].Key = "extents/node-a/yesterday/dm9s.json" }, false},
		{"extent with bad file name", func(m *backup.Manifest) { m.Extents[0].Key = "extents/node-a/" + stamp0 + "/a.b.json" }, false},
		{"extent traversal", func(m *backup.Manifest) { m.Extents[0].Key = "extents/node-a/" + stamp0 + "/../../x.json" }, false},
		{"chunk traversal", func(m *backup.Manifest) { m.Chunks[0].ID = "../../etc/passwd" }, false},
		{"empty chunk id", func(m *backup.Manifest) { m.Chunks[0].ID = "" }, false},

		{"namespace without checksum", func(m *backup.Manifest) { m.Namespace.SHA256 = "" }, false},
		{"extent without checksum", func(m *backup.Manifest) { m.Extents[0].SHA256 = "" }, false},
		{"chunk without checksum", func(m *backup.Manifest) { m.Chunks[1].SHA256 = "" }, false},
		{"uppercase checksum", setSum(strings.ToUpper(validSum)), false},
		{"short checksum", setSum(validSum[:63]), false},
		{"long checksum", setSum(validSum + "a"), false},
		{"non-hex checksum", setSum(strings.Repeat("g", 64)), false},
		{"checksum with trailing newline", setSum(validSum + "\n"), false},

		{"empty namespace", func(m *backup.Manifest) { m.Namespace.Size = 0 }, true},
		{"namespace at the limit", func(m *backup.Manifest) { m.Namespace.Size = backup.MaxMetadataBytes }, true},
		{"namespace over the limit", func(m *backup.Manifest) { m.Namespace.Size = backup.MaxMetadataBytes + 1 }, false},
		{"negative namespace size", func(m *backup.Manifest) { m.Namespace.Size = -1 }, false},
		{"empty extent", func(m *backup.Manifest) { m.Extents[0].Size = 0 }, true},
		{"extent at the limit", func(m *backup.Manifest) { m.Extents[0].Size = backup.MaxMetadataBytes }, true},
		{"extent over the limit", func(m *backup.Manifest) { m.Extents[0].Size = backup.MaxMetadataBytes + 1 }, false},
		{"negative extent size", func(m *backup.Manifest) { m.Extents[0].Size = -1 }, false},
		{"chunk of size 0", func(m *backup.Manifest) { m.Chunks[0].Size = 0 }, false},
		{"negative chunk size", func(m *backup.Manifest) { m.Chunks[0].Size = -5 }, false},
		{"chunk of size 1", func(m *backup.Manifest) { m.Chunks[0].Size = 1 }, true},
		{"chunk at the limit", func(m *backup.Manifest) { m.Chunks[0].Size = backup.MaxChunkBytes }, true},
		{"chunk over the limit", func(m *backup.Manifest) { m.Chunks[0].Size = backup.MaxChunkBytes + 1 }, false},

		{"duplicate chunk ids", func(m *backup.Manifest) { m.Chunks[1].ID = m.Chunks[0].ID }, false},
		{"duplicate extent keys", func(m *backup.Manifest) { m.Extents = append(m.Extents, m.Extents[0]) }, false},
		{"two distinct extents", func(m *backup.Manifest) {
			m.Extents = append(m.Extents, backup.ObjectRef{Key: "extents/node-a/" + stamp0 + "/b3Ro.json", Size: 1, SHA256: validSum})
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := validManifest()
			c.mutate(&m)
			err := m.Validate()
			if (err == nil) != c.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, c.ok)
			}
			if err != nil && !errors.Is(err, backup.ErrInvalidManifest) {
				t.Fatalf("error does not wrap ErrInvalidManifest: %v", err)
			}
		})
	}
}

func TestManifestTotalBytes(t *testing.T) {
	m := validManifest()
	if got := m.TotalBytes(); got != 10+5+3+4 {
		t.Fatalf("TotalBytes() = %d, want 22", got)
	}
	m.Extents, m.Chunks = nil, nil
	if got := m.TotalBytes(); got != 10 {
		t.Fatalf("TotalBytes() without extents or chunks = %d, want 10", got)
	}
}

func TestListManifests(t *testing.T) {
	tgt := newMemTarget()
	const s1, s2, s3 = "2026-10-26T12:00:00.000Z", "2026-10-27T12:00:00.000Z", "2026-10-28T12:00:00.000Z"
	putManifest(t, tgt, "node-a", s3, []string{"c3"}, t0)
	putManifest(t, tgt, "node-a", s1, []string{"c1"}, t0)
	putManifest(t, tgt, "node-a", s2, []string{"c2"}, t0)
	putManifest(t, tgt, "node-b", s2, []string{"c9"}, t0)
	tgt.set("manifests/node-a/latest.json", []byte("not even json"), t0)
	tgt.set("manifests/node-a/notes.txt", []byte("x"), t0)
	tgt.set("manifests/node-a/garbage.json", []byte("x"), t0)
	tgt.set("manifests/loose.json", []byte("x"), t0)

	got, err := backup.ListManifests(context.Background(), tgt)
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("nodes = %d, want 2: %v", len(got), got)
	}
	var names []string
	for _, info := range got["node-a"] {
		names = append(names, info.Name)
		if info.Key != backup.ManifestKey("node-a", info.Name) || info.Manifest == nil || info.Manifest.NodeID != "node-a" {
			t.Fatalf("bad info: %+v", info)
		}
	}
	if !reflect.DeepEqual(names, []string{s1, s2, s3}) {
		t.Fatalf("node-a order = %v", names)
	}
	if len(got["node-b"]) != 1 || got["node-b"][0].Manifest.Chunks[0].ID != "c9" {
		t.Fatalf("node-b = %+v", got["node-b"])
	}
}

func TestListManifestsFailures(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name   string
		setup  func(tgt *memTarget)
		wantIs error
	}{
		{"undecodable manifest", func(tgt *memTarget) {
			tgt.set(backup.ManifestKey("node-a", stamp0), []byte("{nope"), t0)
		}, backup.ErrInvalidManifest},
		{"invalid manifest", func(tgt *memTarget) {
			tgt.set(backup.ManifestKey("node-a", stamp0), []byte(`{"format":9,"node_id":"node-a"}`), t0)
		}, backup.ErrInvalidManifest},
		{"list fails", func(tgt *memTarget) { tgt.failList("manifests/", boom) }, boom},
		{"get fails", func(tgt *memTarget) {
			putManifest(t, tgt, "node-a", stamp0, nil, t0)
			tgt.failGet("manifests/node-a/*", boom)
		}, boom},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tgt := newMemTarget()
			c.setup(tgt)
			if _, err := backup.ListManifests(context.Background(), tgt); !errors.Is(err, c.wantIs) {
				t.Fatalf("err = %v, want %v", err, c.wantIs)
			}
		})
	}
}

func TestLoadManifest(t *testing.T) {
	tgt := newMemTarget()
	want := putManifest(t, tgt, "node-a", stamp0, []string{"c1"}, t0)
	tgt.set(backup.ManifestKey("node-a", "latest"), tgt.bytesOf(backup.ManifestKey("node-a", stamp0)), t0)

	for _, name := range []string{"latest", "latest.json", stamp0, stamp0 + ".json"} {
		t.Run(name, func(t *testing.T) {
			m, err := backup.LoadManifest(context.Background(), tgt, "node-a", name)
			if err != nil {
				t.Fatalf("LoadManifest: %v", err)
			}
			if m.Namespace != want.Namespace || !reflect.DeepEqual(m.Chunks, want.Chunks) || !reflect.DeepEqual(m.Extents, want.Extents) {
				t.Fatalf("manifest = %+v", m)
			}
		})
	}
}

func TestLoadManifestFailures(t *testing.T) {
	boom := errors.New("boom")
	other := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02T15:04:05.000Z")
	cases := []struct {
		name    string
		node    string
		mname   string
		setup   func(tgt *memTarget)
		wantIs  error
		wantMsg string
	}{
		{name: "missing", node: "node-a", mname: other, wantIs: backup.ErrManifestNotFound, wantMsg: "siloctl backup list"},
		{name: "missing latest", node: "node-a", mname: "latest", wantIs: backup.ErrManifestNotFound},
		{name: "node mismatch", node: "node-a", mname: stamp0, setup: func(tgt *memTarget) {
			putManifest(t, tgt, "node-b", stamp0, nil, t0)
			tgt.set(backup.ManifestKey("node-a", stamp0), tgt.bytesOf(backup.ManifestKey("node-b", stamp0)), t0)
		}, wantIs: backup.ErrInvalidManifest, wantMsg: "belongs to node"},
		{name: "get error", node: "node-a", mname: stamp0, setup: func(tgt *memTarget) { tgt.failGet("manifests/*", boom) }, wantIs: boom},
		{name: "invalid node", node: "../x", mname: "latest", wantIs: backup.ErrInvalidNodeID},
		{name: "invalid name", node: "node-a", mname: "../x", wantIs: backup.ErrInvalidManifestName},
		{name: "damaged json", node: "node-a", mname: stamp0, setup: func(tgt *memTarget) {
			tgt.set(backup.ManifestKey("node-a", stamp0), []byte("{"), t0)
		}, wantIs: backup.ErrInvalidManifest, wantMsg: "not valid JSON"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tgt := newMemTarget()
			if c.setup != nil {
				c.setup(tgt)
			}
			_, err := backup.LoadManifest(context.Background(), tgt, c.node, c.mname)
			if !errors.Is(err, c.wantIs) {
				t.Fatalf("err = %v, want %v", err, c.wantIs)
			}
			if c.wantMsg != "" && !strings.Contains(err.Error(), c.wantMsg) {
				t.Fatalf("err %q does not mention %q", err, c.wantMsg)
			}
		})
	}
}

func TestManifestStamp(t *testing.T) {
	m := validManifest()
	m.StartedAt = t0.In(time.FixedZone("x", 3600))
	if got := m.Stamp(); got != stamp0 {
		t.Fatalf("Stamp() = %q, want %q", got, stamp0)
	}
}
