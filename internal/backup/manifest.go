package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crypto"
)

// ManifestFormat is the manifest schema version this build writes and the
// highest it can restore.
const ManifestFormat = 1

// LatestManifest names the per-node copy of the newest manifest.
const LatestManifest = "latest"

// stampLayout names each run's objects. It is RFC 3339 in UTC with a fixed
// millisecond fraction, so names sort lexically in time order and two runs a
// second apart do not collide.
const stampLayout = "2006-01-02T15:04:05.000Z"

// Object-name prefixes on the target.
const (
	chunksPrefix    = "chunks/"
	manifestsPrefix = "manifests/"
	runsPrefix      = "runs/"
	namespaceDir    = "namespace"
	extentsDir      = "extents"
)

// Read limits for objects fetched from a target. Anyone who can write to the
// bucket can plant an object of any size, and silod reads every node's
// manifests during retention, so every read is capped well above what silo
// itself writes.
const (
	// MaxManifestBytes fits a manifest of about two million chunks (each
	// entry is an id, a size and a 64-character hash).
	MaxManifestBytes int64 = 256 << 20
	// MaxMetadataBytes bounds a namespace snapshot or one volume's extent
	// map. A 40 GiB volume at 256 KiB chunks has 160k extents, around 15 MB
	// of JSON, so 1 GiB leaves room for very large volumes.
	MaxMetadataBytes int64 = 1 << 30
	// MaxChunkBytes bounds one chunk envelope. silo has no hard upper chunk
	// size (SILO_CHUNK_SIZE and per-volume sizes default to 256 KiB), so
	// this allows 64 MiB of data plus the encryption overhead, far above any
	// practical setting. Export refuses to back up a larger chunk rather
	// than write one restore could not read back.
	MaxChunkBytes = 64<<20 + int64(crypto.OverheadBytes)
)

var (
	// ErrInvalidNodeID rejects node ids that cannot be used safely inside an
	// object name or a file path.
	ErrInvalidNodeID = errors.New("backup: invalid node id")
	// ErrInvalidManifestName rejects manifest names other than "latest" or a
	// run timestamp.
	ErrInvalidManifestName = errors.New("backup: invalid manifest name")
	// ErrManifestNotFound means the requested manifest is not on the target.
	ErrManifestNotFound = errors.New("backup: manifest not found")
	// ErrInvalidManifest means a manifest was found but its content cannot be
	// trusted for a restore.
	ErrInvalidManifest = errors.New("backup: invalid manifest")
	// ErrChecksumMismatch means an object's size or SHA-256 differs from what
	// the manifest recorded.
	ErrChecksumMismatch = errors.New("backup: object does not match the manifest")
)

var (
	nodeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	stampPattern  = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)
	// extentObjectPattern is the file part of an extent object: the base64url
	// volume id plus ".json".
	extentObjectPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+\.json$`)
	// versionPattern keeps the version printable: siloctl shows it in a
	// terminal, so escape sequences must not get through.
	versionPattern = regexp.MustCompile(`^[A-Za-z0-9.+_-]{0,64}$`)
	sha256Pattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// ValidateNodeID accepts 1-128 ASCII letters, digits, dots, dashes and
// underscores, starting with a letter or digit. Node ids become object-name
// segments and restore paths, so anything else is refused.
func ValidateNodeID(id string) error {
	if !nodeIDPattern.MatchString(id) {
		return fmt.Errorf("%w %q: use 1-128 letters, digits, dots, dashes or underscores, starting with a letter or digit (check SILO_NODE_ID)", ErrInvalidNodeID, id)
	}
	return nil
}

// ParseManifestName normalises a manifest name given by an operator: "latest"
// or a run timestamp such as 2026-10-28T12:00:00.000Z, each with or without a
// trailing ".json".
func ParseManifestName(name string) (string, error) {
	n := strings.TrimSuffix(name, ".json")
	if n == LatestManifest || stampPattern.MatchString(n) {
		return n, nil
	}
	return "", fmt.Errorf("%w %q: use \"latest\" or a timestamp from 'siloctl backup list', such as 2026-10-28T12:00:00.000Z", ErrInvalidManifestName, name)
}

// ManifestKey is the object name of a node's manifest. name must already have
// passed ParseManifestName.
func ManifestKey(nodeID, name string) string {
	return manifestsPrefix + nodeID + "/" + name + ".json"
}

// ObjectRef names one object a manifest depends on, with the size and
// SHA-256 (lowercase hex) it had when the run wrote it.
type ObjectRef struct {
	Key    string `json:"key"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// ChunkRef is one chunk a manifest depends on, stored as chunks/<ID>.
type ChunkRef struct {
	ID     string `json:"id"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Manifest describes one complete backup run of one node. It is written only
// after every object it references has been uploaded and confirmed present,
// so a manifest on the target always names a restorable state. Manifests are
// not signed: the checksums detect damage and mix-ups, not a deliberate
// forgery by someone who can write to the bucket.
type Manifest struct {
	Format      int       `json:"format"`
	SiloVersion string    `json:"silo_version"`
	NodeID      string    `json:"node_id"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	// Namespace is the namespace snapshot.
	Namespace ObjectRef `json:"namespace"`
	// Extents are the per-volume extent maps.
	Extents []ObjectRef `json:"extents"`
	// Chunks are every chunk the node held, sorted by id.
	Chunks         []ChunkRef `json:"chunks"`
	ChunkCount     int        `json:"chunk_count"`
	ChunkBytes     int64      `json:"chunk_bytes"`
	ChunksUploaded int        `json:"chunks_uploaded"`
	ChunksSkipped  int        `json:"chunks_skipped"`
	BytesUploaded  int64      `json:"bytes_uploaded"`
}

// Stamp is the run timestamp the manifest's objects are named after, and the
// name 'siloctl backup restore --manifest' accepts for it.
func (m *Manifest) Stamp() string { return m.StartedAt.UTC().Format(stampLayout) }

// TotalBytes is the size of everything a restore of m downloads.
func (m *Manifest) TotalBytes() int64 {
	total := m.Namespace.Size
	for _, e := range m.Extents {
		total += e.Size
	}
	for _, c := range m.Chunks {
		total += c.Size
	}
	return total
}

// Validate checks that a manifest read back from a target only references
// objects this node's runs would have written, each with a plausible size and
// a well-formed checksum, and nothing twice. Restore and verify call it before
// touching any referenced name, so a tampered manifest cannot point a restore
// outside the data directory or at another node's objects.
func (m *Manifest) Validate() error {
	if m.Format < 1 || m.Format > ManifestFormat {
		return fmt.Errorf("%w: format %d is not supported by this siloctl (it supports up to %d); use a siloctl at least as new as the silod that wrote the backup", ErrInvalidManifest, m.Format, ManifestFormat)
	}
	if !versionPattern.MatchString(m.SiloVersion) {
		return fmt.Errorf("%w: the silo version field holds characters silo never writes; the manifest is damaged or was not written by silo", ErrInvalidManifest)
	}
	if err := ValidateNodeID(m.NodeID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidManifest, err)
	}
	nsDir := namespaceDir + "/" + m.NodeID + "/"
	if rest, ok := strings.CutPrefix(m.Namespace.Key, nsDir); !ok || !isStampJSON(rest) {
		return fmt.Errorf("%w: namespace object %q is not under %s", ErrInvalidManifest, m.Namespace.Key, nsDir)
	}
	if err := validateRef(m.Namespace.Key, m.Namespace.Size, m.Namespace.SHA256, 0, MaxMetadataBytes); err != nil {
		return err
	}
	if err := m.validateExtents(); err != nil {
		return err
	}
	return m.validateChunks()
}

func (m *Manifest) validateExtents() error {
	extDir := extentsDir + "/" + m.NodeID + "/"
	seen := make(map[string]bool, len(m.Extents))
	for _, e := range m.Extents {
		stamp, file, ok := strings.Cut(strings.TrimPrefix(e.Key, extDir), "/")
		if !strings.HasPrefix(e.Key, extDir) || !ok || !stampPattern.MatchString(stamp) || !extentObjectPattern.MatchString(file) {
			return fmt.Errorf("%w: extent object %q is not under %s", ErrInvalidManifest, e.Key, extDir)
		}
		if seen[e.Key] {
			return fmt.Errorf("%w: extent object %q is listed twice", ErrInvalidManifest, e.Key)
		}
		seen[e.Key] = true
		if err := validateRef(e.Key, e.Size, e.SHA256, 0, MaxMetadataBytes); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manifest) validateChunks() error {
	seen := make(map[string]bool, len(m.Chunks))
	for _, c := range m.Chunks {
		if err := chunkstore.ValidateID(c.ID); err != nil {
			return fmt.Errorf("%w: chunk id %q is not valid", ErrInvalidManifest, c.ID)
		}
		if seen[c.ID] {
			return fmt.Errorf("%w: chunk %s is listed twice", ErrInvalidManifest, c.ID)
		}
		seen[c.ID] = true
		if err := validateRef(chunkKey(c.ID), c.Size, c.SHA256, 1, MaxChunkBytes); err != nil {
			return err
		}
	}
	return nil
}

// validateRef checks one recorded size and checksum.
func validateRef(key string, size int64, sum string, minSize, maxSize int64) error {
	if size < minSize || size > maxSize {
		return fmt.Errorf("%w: %s has an impossible size %d (allowed %d to %d bytes)", ErrInvalidManifest, key, size, minSize, maxSize)
	}
	if !sha256Pattern.MatchString(sum) {
		return fmt.Errorf("%w: %s has no valid SHA-256 checksum", ErrInvalidManifest, key)
	}
	return nil
}

// isStampJSON reports whether s is exactly "<run stamp>.json".
func isStampJSON(s string) bool {
	stamp, ok := strings.CutSuffix(s, ".json")
	return ok && stampPattern.MatchString(stamp)
}

// ManifestInfo is one entry of ListManifests: the manifest plus where it was
// found.
type ManifestInfo struct {
	// Name is the run timestamp ("2026-10-28T12:00:00.000Z").
	Name     string
	Key      string
	Manifest *Manifest
}

// ListManifests returns every timestamped manifest on the target, grouped by
// node id and sorted oldest first within each node. The latest.json copies are
// left out because they duplicate the newest entry. A manifest that cannot be
// read or decoded fails the call: an operator choosing a restore point needs to
// know the catalog is incomplete.
func ListManifests(ctx context.Context, target blobstore.Target) (map[string][]ManifestInfo, error) {
	objs, err := target.List(ctx, manifestsPrefix)
	if err != nil {
		return nil, fmt.Errorf("backup: could not list the manifests on %s (%w)", target.Name(), err)
	}
	out := map[string][]ManifestInfo{}
	for _, o := range objs {
		node, name, ok := parseManifestKey(o.Name)
		if !ok || name == LatestManifest {
			continue
		}
		m, err := readManifest(ctx, target, o.Name)
		if err != nil {
			return nil, err
		}
		out[node] = append(out[node], ManifestInfo{Name: name, Key: o.Name, Manifest: m})
	}
	for node := range out {
		sort.Slice(out[node], func(i, j int) bool { return out[node][i].Name < out[node][j].Name })
	}
	return out, nil
}

// LoadManifest fetches and validates one manifest of one node. name is
// "latest" or a run timestamp (see ParseManifestName).
func LoadManifest(ctx context.Context, target blobstore.Target, nodeID, name string) (*Manifest, error) {
	if err := ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	n, err := ParseManifestName(name)
	if err != nil {
		return nil, err
	}
	key := ManifestKey(nodeID, n)
	m, err := readManifest(ctx, target, key)
	if err != nil {
		return nil, err
	}
	if m.NodeID != nodeID {
		return nil, fmt.Errorf("%w: %s says it belongs to node %q, not %q; the backup prefix may be shared by mistake", ErrInvalidManifest, key, m.NodeID, nodeID)
	}
	return m, nil
}

// readManifest gets, decodes and validates the manifest stored under key.
func readManifest(ctx context.Context, target blobstore.Target, key string) (*Manifest, error) {
	raw, err := target.Get(ctx, key, MaxManifestBytes)
	if errors.Is(err, blobstore.ErrNotFound) {
		return nil, fmt.Errorf("%w: %s on %s; run 'siloctl backup list --target %s' to see the available backups", ErrManifestNotFound, key, target.Name(), target.Name())
	}
	if err != nil {
		return nil, fmt.Errorf("backup: could not read %s (%w)", key, err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%w: %s is not valid JSON (%w); the object may be damaged, pick an older backup", ErrInvalidManifest, key, err)
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return &m, nil
}

// parseManifestKey splits manifests/<node>/<name>.json. ok is false for any
// other shape, including names that fail validation.
func parseManifestKey(key string) (node, name string, ok bool) {
	rest, found := strings.CutPrefix(key, manifestsPrefix)
	if !found {
		return "", "", false
	}
	node, file, found := strings.Cut(rest, "/")
	if !found || ValidateNodeID(node) != nil || !strings.HasSuffix(file, ".json") {
		return "", "", false
	}
	name, err := ParseManifestName(file)
	if err != nil {
		return "", "", false
	}
	return node, name, true
}

// chunkKey is the object name of a chunk.
func chunkKey(id string) string { return chunksPrefix + id }

// namespaceKey and extentKey name a run's per-node objects. Including the run
// stamp keeps every retained manifest restorable to its own point in time;
// overwriting one fixed object would make older manifests describe the newest
// namespace.
func namespaceKey(nodeID, stamp string) string {
	return path.Join(namespaceDir, nodeID, stamp+".json")
}

func extentKey(nodeID, stamp, encodedVolume string) string {
	return path.Join(extentsDir, nodeID, stamp, encodedVolume+".json")
}

// runMarkerKey names the marker that announces a node's run in progress.
func runMarkerKey(nodeID, stamp string) string {
	return runsPrefix + nodeID + "/" + stamp + ".json"
}

// stampTime parses a run stamp; ok is false for anything else.
func stampTime(stamp string) (time.Time, bool) {
	if !stampPattern.MatchString(stamp) {
		return time.Time{}, false
	}
	t, err := time.Parse(stampLayout, stamp)
	return t, err == nil
}
