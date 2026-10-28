// Package backup exports a silod node's durable state (its encrypted chunks,
// its namespace snapshot and the extent maps it holds) to a blobstore.Target,
// and restores it again. Chunks are copied as-is, still AES-GCM encrypted under
// the cluster key, so a backup is no less protected than the live data.
//
// Each run is incremental: chunks never change once written, so a chunk whose
// object already exists on the target is skipped. Chunk objects are written
// once and never replaced: replicas of a chunk on different nodes hold
// different ciphertext (each node encrypts with its own nonce), so letting a
// second node overwrite the object would invalidate the checksum the first
// node's manifest recorded. A run finishes by writing a manifest that lists
// everything it depends on with sizes and SHA-256 checksums, only after every
// object has been uploaded and confirmed present, so any manifest on the
// target is a complete restore point. Retention prunes old manifests and the
// chunks no remaining manifest references.
//
// The namespace is a CRDT replicated to every node, so any node's snapshot
// describes the whole cluster; chunks and extent maps are node-local, so a full
// cluster backup is the union of every node's runs, which may share a prefix.
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crdt"
)

// ChunkSource enumerates and reads the node's encrypted chunks.
// *chunkstore.FileStore satisfies it.
type ChunkSource interface {
	List(ctx context.Context) ([]string, error)
	RawChunk(ctx context.Context, id string) ([]byte, error)
}

// NamespaceSnapshot serialises the node's namespace replica.
// *namespace.Namespace satisfies it.
type NamespaceSnapshot interface {
	Snapshot() ([]byte, error)
}

// ExtentSource enumerates the volumes whose extent maps this node holds and
// serialises each. *extentmap.Store satisfies it. Extent maps do not ride the
// gossiped namespace, so a backup must capture them separately or a restore
// would yield empty volumes.
type ExtentSource interface {
	Volumes() []string
	Snapshot(volumeID string) []crdt.MapEntry[uint64, string]
}

// extentBackup is the on-target JSON shape of one volume's exported extent map.
type extentBackup struct {
	VolumeID string                          `json:"volume_id"`
	Entries  []crdt.MapEntry[uint64, string] `json:"entries"`
}

// DefaultConcurrency is how many chunk transfers an export runs at once.
const DefaultConcurrency = 4

// markerHeartbeat is how often a running export refreshes its run marker.
// It must stay well below staleRunAfter in prune.go.
const markerHeartbeat = 5 * time.Minute

// Result summarises one export run.
type Result struct {
	StartedAt  time.Time
	FinishedAt time.Time
	// Stamp is the run timestamp; Manifest is the object name of the
	// manifest the run wrote.
	Stamp    string
	Manifest string
	// Chunks and ChunkBytes cover every chunk the manifest lists.
	Chunks     int
	ChunkBytes int64
	// Uploaded chunks were sent this run; Skipped ones were already on the
	// target.
	Uploaded      int
	Skipped       int
	BytesUploaded int64
}

// Exporter writes one node's state to a Target. It is safe for concurrent use,
// but runs against the same target should not overlap: the subsystem
// serialises them.
type Exporter struct {
	chunks      ChunkSource
	ns          NamespaceSnapshot
	extents     ExtentSource
	nodeID      string
	version     string
	concurrency int
	heartbeat   time.Duration
	now         func() time.Time
}

// ExporterOption configures an Exporter.
type ExporterOption func(*Exporter)

// WithExtentSource makes the exporter also back up the extent maps this node
// holds. Without it only the namespace and chunks are exported, which is
// correct only while extent maps still live in the namespace.
func WithExtentSource(src ExtentSource) ExporterOption {
	return func(e *Exporter) { e.extents = src }
}

// WithVersion records the silo version in every manifest. Characters outside
// letters, digits and ".+_-" are replaced, because restore refuses manifests
// whose version could carry terminal escape sequences.
func WithVersion(v string) ExporterOption {
	return func(e *Exporter) { e.version = v }
}

// WithConcurrency sets how many chunks are transferred in parallel. Values
// below one keep DefaultConcurrency.
func WithConcurrency(n int) ExporterOption {
	return func(e *Exporter) { e.concurrency = n }
}

// WithClock replaces time.Now for run timestamps.
func WithClock(now func() time.Time) ExporterOption {
	return func(e *Exporter) { e.now = now }
}

// WithHeartbeat sets how often the run marker is refreshed during a run.
// Values below one keep the default of five minutes.
func WithHeartbeat(d time.Duration) ExporterOption {
	return func(e *Exporter) { e.heartbeat = d }
}

// NewExporter builds an exporter for a node's local state. nodeID must pass
// ValidateNodeID; Export refuses to run otherwise.
func NewExporter(chunks ChunkSource, ns NamespaceSnapshot, nodeID string, opts ...ExporterOption) *Exporter {
	e := &Exporter{chunks: chunks, ns: ns, nodeID: nodeID, version: "dev", concurrency: DefaultConcurrency, heartbeat: markerHeartbeat, now: time.Now}
	for _, opt := range opts {
		opt(e)
	}
	if e.concurrency < 1 {
		e.concurrency = DefaultConcurrency
	}
	if e.heartbeat <= 0 {
		e.heartbeat = markerHeartbeat
	}
	e.version = sanitizeVersion(e.version)
	return e
}

// sanitizeVersion maps a build version onto the characters Manifest.Validate
// accepts, so a self-built binary with an odd version string still writes
// restorable manifests.
func sanitizeVersion(v string) string {
	out := []rune(v)
	for i, r := range out {
		if !versionRune(r) {
			out[i] = '_'
		}
	}
	if len(out) > 64 {
		out = out[:64]
	}
	return string(out)
}

func versionRune(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune(".+_-", r)
}

// Export runs one incremental backup:
//
//  1. announce the run with a marker under runs/<node>/, refreshed while the
//     run lasts, so other nodes' retention holds off deleting chunks;
//  2. snapshot the namespace and extent maps and upload them under this run's
//     timestamp;
//  3. list the chunks already on the target once, upload the local chunks
//     that are missing, and confirm every chunk the manifest will list is
//     present;
//  4. write manifests/<node>/<timestamp>.json, then copy it to latest.json,
//     and remove the marker.
//
// The snapshots are taken before the local chunks are listed so that every
// chunk they reference is still on disk when it is read: the chunk GC never
// removes a referenced chunk. A failure at any step aborts the run without
// writing a manifest, so a partial backup is never presented as a restore
// point.
func (e *Exporter) Export(ctx context.Context, target blobstore.Target) (Result, error) {
	if err := ValidateNodeID(e.nodeID); err != nil {
		return Result{}, err
	}
	started := e.now().UTC()
	stamp := started.Format(stampLayout)
	m := &Manifest{Format: ManifestFormat, SiloVersion: e.version, NodeID: e.nodeID, StartedAt: started}

	stopMarker, err := e.announceRun(ctx, target, stamp, started)
	if err != nil {
		return Result{}, err
	}
	defer stopMarker()

	if err := e.exportMetadata(ctx, target, stamp, m); err != nil {
		return Result{}, err
	}
	if err := e.exportChunks(ctx, target, m); err != nil {
		return Result{}, err
	}
	if err := confirmChunks(ctx, target, m); err != nil {
		return Result{}, err
	}

	m.FinishedAt = e.now().UTC()
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		// Not reachable in tests: Manifest holds only strings, ints and
		// times, which always encode.
		return Result{}, fmt.Errorf("backup: could not encode the manifest (%w); this is a bug, please report it", err)
	}
	key := ManifestKey(e.nodeID, stamp)
	if err := target.Put(ctx, key, raw); err != nil {
		return Result{}, fmt.Errorf("backup: could not write the manifest %s (%w)", key, err)
	}
	if err := target.Put(ctx, ManifestKey(e.nodeID, LatestManifest), raw); err != nil {
		return Result{}, fmt.Errorf("backup: wrote %s but could not update latest.json (%w); the backup is usable by its timestamp, the next run retries", key, err)
	}
	return Result{
		StartedAt:     started,
		FinishedAt:    m.FinishedAt,
		Stamp:         stamp,
		Manifest:      key,
		Chunks:        m.ChunkCount,
		ChunkBytes:    m.ChunkBytes,
		Uploaded:      m.ChunksUploaded,
		Skipped:       m.ChunksSkipped,
		BytesUploaded: m.BytesUploaded,
	}, nil
}

// runMarker is the JSON body of a run marker. Retention judges freshness by
// the object's modification time on the target, not by these fields, so a
// node with a wrong clock cannot make its marker look fresh forever; the
// fields are for operators reading the bucket.
type runMarker struct {
	NodeID      string    `json:"node_id"`
	Stamp       string    `json:"stamp"`
	StartedAt   time.Time `json:"started_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
}

// announceRun removes this node's leftover markers (runs never overlap on one
// node, so any existing marker belongs to a run that crashed), writes a fresh
// one and keeps it fresh until the returned stop function runs. stop deletes
// the marker even when ctx is already cancelled.
func (e *Exporter) announceRun(ctx context.Context, target blobstore.Target, stamp string, started time.Time) (func(), error) {
	old, err := target.List(ctx, runsPrefix+e.nodeID+"/")
	if err != nil {
		return nil, fmt.Errorf("backup: could not list this node's run markers on %s (%w)", target.Name(), err)
	}
	for _, o := range old {
		if err := target.Delete(ctx, o.Name); err != nil {
			return nil, fmt.Errorf("backup: could not remove the leftover run marker %s (%w)", o.Name, err)
		}
	}
	key := runMarkerKey(e.nodeID, stamp)
	beat := func(c context.Context) error {
		b, _ := json.Marshal(runMarker{NodeID: e.nodeID, Stamp: stamp, StartedAt: started, HeartbeatAt: e.now().UTC()})
		return target.Put(c, key, b)
	}
	if err := beat(ctx); err != nil {
		return nil, fmt.Errorf("backup: could not write the run marker %s (%w)", key, err)
	}

	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(e.heartbeat)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				// A missed heartbeat only makes the marker look older;
				// the next one or the run's end corrects it.
				_ = beat(hbCtx)
			}
		}
	}()
	return func() {
		cancel()
		<-done
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer stop()
		// A marker left behind turns stale and is removed by another
		// node's retention or by this node's next run.
		_ = target.Delete(cleanup, key)
	}, nil
}

// exportMetadata uploads the namespace snapshot and every extent map.
func (e *Exporter) exportMetadata(ctx context.Context, target blobstore.Target, stamp string, m *Manifest) error {
	snap, err := e.ns.Snapshot()
	if err != nil {
		return fmt.Errorf("backup: could not snapshot the namespace (%w); check silod's logs for namespace errors", err)
	}
	if err := checkMetadataSize("the namespace snapshot", len(snap)); err != nil {
		// Reaching this in a test needs a 1 GiB snapshot;
		// TestCheckMetadataSize covers the check itself.
		return err
	}
	m.Namespace = ObjectRef{Key: namespaceKey(e.nodeID, stamp), Size: int64(len(snap)), SHA256: sha256Hex(snap)}
	if err := target.Put(ctx, m.Namespace.Key, snap); err != nil {
		return fmt.Errorf("backup: could not write the namespace snapshot (%w)", err)
	}
	if e.extents == nil {
		return nil
	}
	vols := e.extents.Volumes()
	sort.Strings(vols)
	m.Extents = make([]ObjectRef, 0, len(vols))
	for _, vol := range vols {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("backup: stopped before the extent maps were saved (%w)", err)
		}
		b, err := json.Marshal(extentBackup{VolumeID: vol, Entries: e.extents.Snapshot(vol)})
		if err != nil {
			// Not reachable in tests: entries are uint64 keys and string
			// values, which always encode.
			return fmt.Errorf("backup: could not encode the extent map of volume %s (%w); this is a bug, please report it", vol, err)
		}
		if err := checkMetadataSize("the extent map of volume "+vol, len(b)); err != nil {
			// As for the namespace: covered by TestCheckMetadataSize.
			return err
		}
		ref := ObjectRef{Key: extentKey(e.nodeID, stamp, base64.RawURLEncoding.EncodeToString([]byte(vol))), Size: int64(len(b)), SHA256: sha256Hex(b)}
		if err := target.Put(ctx, ref.Key, b); err != nil {
			return fmt.Errorf("backup: could not write the extent map of volume %s (%w)", vol, err)
		}
		m.Extents = append(m.Extents, ref)
	}
	return nil
}

// checkMetadataSize refuses to upload a metadata object that restore would
// reject for its size.
func checkMetadataSize(what string, n int) error {
	if int64(n) > MaxMetadataBytes {
		return fmt.Errorf("backup: %s is %d bytes, more than the %d a restore accepts; please report this", what, n, MaxMetadataBytes)
	}
	return nil
}

// chunkPlan sorts the node's chunks into what a run must do with each.
type chunkPlan struct {
	known  []ChunkRef // on the target, checksum known from a manifest
	hash   []string   // on the target, checksum unknown: download to hash
	upload []string   // not on the target
}

// exportChunks uploads the local chunks the target does not have yet and
// records every chunk, with its checksum, in m.
func (e *Exporter) exportChunks(ctx context.Context, target blobstore.Target, m *Manifest) error {
	ids, err := e.chunks.List(ctx)
	if err != nil {
		return fmt.Errorf("backup: could not list the local chunks (%w); check that the data directory is readable", err)
	}
	// One listing for the whole run: per-chunk existence checks would cost a
	// request per chunk, while a listing returns 1000 names per request.
	present, err := listChunks(ctx, target)
	if err != nil {
		return err
	}
	plan, err := planChunks(ids, present, knownChecksums(ctx, target))
	if err != nil {
		return err
	}

	var mu sync.Mutex
	kept := append(make([]ChunkRef, 0, len(ids)), plan.known...)
	m.ChunksSkipped = len(plan.known)
	add := func(ref ChunkRef, uploaded bool) {
		mu.Lock()
		defer mu.Unlock()
		kept = append(kept, ref)
		if uploaded {
			m.ChunksUploaded++
			m.BytesUploaded += ref.Size
		} else {
			m.ChunksSkipped++
		}
	}

	err = forEach(ctx, e.concurrency, plan.hash, func(ctx context.Context, id string) error {
		ref, err := hashRemoteChunk(ctx, target, id)
		if err != nil {
			return err
		}
		add(ref, false)
		return nil
	})
	if err != nil {
		return err
	}
	err = forEach(ctx, e.concurrency, plan.upload, func(ctx context.Context, id string) error {
		ref, uploaded, skip, err := e.uploadChunk(ctx, target, id)
		if err != nil || skip {
			return err
		}
		add(ref, uploaded)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].ID < kept[j].ID })
	m.Chunks = kept
	m.ChunkCount = len(kept)
	for _, c := range kept {
		m.ChunkBytes += c.Size
	}
	return nil
}

// planChunks decides per local chunk whether it is already stored with a
// known checksum, stored with an unknown one, or missing. A zero-size object
// is never a valid envelope, so it counts as missing and uploadChunk replaces
// it.
func planChunks(ids []string, present map[string]int64, known map[string]ChunkRef) (chunkPlan, error) {
	sort.Strings(ids)
	var p chunkPlan
	for _, id := range ids {
		if err := chunkstore.ValidateID(id); err != nil {
			return chunkPlan{}, fmt.Errorf("backup: the chunk store returned an invalid chunk id %q; this is a bug, please report it", id)
		}
		size, ok := present[id]
		switch {
		case !ok || size <= 0:
			p.upload = append(p.upload, id)
		case known[id].Size == size:
			p.known = append(p.known, known[id])
		default:
			p.hash = append(p.hash, id)
		}
	}
	return p, nil
}

// uploadChunk reads one local chunk and writes it if the target does not have
// it yet. skip is true when the chunk vanished locally after the listing
// (removed by the chunk GC; the snapshots above do not reference it, so
// leaving it out keeps the manifest consistent). When another node stored the
// chunk first, its copy is kept and hashed instead.
func (e *Exporter) uploadChunk(ctx context.Context, target blobstore.Target, id string) (ref ChunkRef, uploaded, skip bool, err error) {
	data, err := e.chunks.RawChunk(ctx, id)
	if errors.Is(err, chunkstore.ErrNotFound) {
		return ChunkRef{}, false, true, nil
	}
	if err != nil {
		return ChunkRef{}, false, false, fmt.Errorf("backup: could not read chunk %s (%w); check the data directory's disk for errors", id, err)
	}
	if int64(len(data)) > MaxChunkBytes {
		return ChunkRef{}, false, false, fmt.Errorf("backup: chunk %s is %d bytes, more than the %d a restore accepts; lower the chunk size of that volume and please report this", id, len(data), MaxChunkBytes)
	}
	created, err := target.PutIfAbsent(ctx, chunkKey(id), data)
	if err != nil {
		return ChunkRef{}, false, false, fmt.Errorf("backup: could not upload chunk %s (%w)", id, err)
	}
	if !created {
		ref, err := hashRemoteChunk(ctx, target, id)
		if err != nil || ref.Size > 0 {
			return ref, false, false, err
		}
		// An empty object is never a valid envelope and no manifest can
		// reference it, so replacing it breaks nothing.
		if err := target.Delete(ctx, chunkKey(id)); err != nil {
			return ChunkRef{}, false, false, fmt.Errorf("backup: could not replace the empty object for chunk %s (%w)", id, err)
		}
		if _, err := target.PutIfAbsent(ctx, chunkKey(id), data); err != nil {
			return ChunkRef{}, false, false, fmt.Errorf("backup: could not upload chunk %s (%w)", id, err)
		}
	}
	return ChunkRef{ID: id, Size: int64(len(data)), SHA256: sha256Hex(data)}, true, false, nil
}

// hashRemoteChunk downloads a chunk object to learn its checksum.
func hashRemoteChunk(ctx context.Context, target blobstore.Target, id string) (ChunkRef, error) {
	data, err := target.Get(ctx, chunkKey(id), MaxChunkBytes)
	if err != nil {
		return ChunkRef{}, fmt.Errorf("backup: could not read chunk %s back from the target to record its checksum (%w)", id, err)
	}
	return ChunkRef{ID: id, Size: int64(len(data)), SHA256: sha256Hex(data)}, nil
}

// knownChecksums collects chunk checksums from every node's latest.json, so a
// run does not download chunks just to hash them. Chunk objects are write-once
// and a chunk referenced by a kept manifest is never pruned, so a checksum in
// a latest.json still describes the object. Damaged or oversized manifests
// are skipped: this is only a shortcut, and an unknown checksum falls back to
// downloading the chunk.
func knownChecksums(ctx context.Context, target blobstore.Target) map[string]ChunkRef {
	out := map[string]ChunkRef{}
	objs, err := target.List(ctx, manifestsPrefix)
	if err != nil {
		return out
	}
	for _, o := range objs {
		if _, name, ok := parseManifestKey(o.Name); !ok || name != LatestManifest {
			continue
		}
		raw, err := target.Get(ctx, o.Name, MaxManifestBytes)
		if err != nil {
			continue
		}
		var m struct {
			Chunks []ChunkRef `json:"chunks"`
		}
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		for _, c := range m.Chunks {
			if c.Size > 0 && sha256Pattern.MatchString(c.SHA256) {
				out[c.ID] = c
			}
		}
	}
	return out
}

// confirmChunks lists the target again right before the manifest is written
// and fails the run if a chunk the manifest would list is gone or changed
// size. That closes the window where another node's retention deleted a chunk
// this run had seen and skipped.
func confirmChunks(ctx context.Context, target blobstore.Target, m *Manifest) error {
	present, err := listChunks(ctx, target)
	if err != nil {
		return err
	}
	for _, c := range m.Chunks {
		if present[c.ID] != c.Size {
			return fmt.Errorf("backup: chunk %s disappeared from %s during the run, most likely removed by another node's retention; no restore point was written and the next run uploads it again", c.ID, target.Name())
		}
	}
	return nil
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// listChunks maps every chunk id on the target to its object size.
func listChunks(ctx context.Context, target blobstore.Target) (map[string]int64, error) {
	objs, err := target.List(ctx, chunksPrefix)
	if err != nil {
		return nil, fmt.Errorf("backup: could not list the chunks already on %s (%w)", target.Name(), err)
	}
	out := make(map[string]int64, len(objs))
	for _, o := range objs {
		if id, ok := chunkIDFromKey(o.Name); ok {
			out[id] = o.Size
		}
	}
	return out, nil
}

// chunkIDFromKey extracts the id from chunks/<id>, rejecting anything nested
// or invalid so a foreign object under the prefix is never mistaken for one.
func chunkIDFromKey(key string) (string, bool) {
	id, ok := strings.CutPrefix(key, chunksPrefix)
	if !ok || chunkstore.ValidateID(id) != nil {
		return "", false
	}
	return id, true
}

// forEach calls fn for every item with at most n calls in flight. The first
// error cancels the remaining work and is returned once every started call has
// returned, so no goroutine outlives forEach.
func forEach(parent context.Context, n int, items []string, fn func(context.Context, string) error) error {
	if len(items) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)
	work := make(chan string)
	for range min(n, len(items)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range work {
				if err := fn(ctx, item); err != nil {
					once.Do(func() { firstErr = err; cancel() })
				}
			}
		}()
	}
feed:
	for _, item := range items {
		select {
		case work <- item:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	if err := parent.Err(); err != nil {
		return fmt.Errorf("backup: stopped before all chunks were processed (%w)", err)
	}
	return nil
}
