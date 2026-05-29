// Package namespace is silo's coordinator-free filesystem namespace: a tree
// of directories and inodes built from CRDTs and stamped with hybrid
// logical clocks. Every node keeps its own replica; replicas exchange state
// and Merge converges them deterministically, surfacing concurrent
// same-name creates as conflicts rather than silently dropping one.
package namespace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

// DefaultGCInterval is how often RunGC sweeps tombstones when no interval is
// given. Tombstones live roughly between retention and retention+interval,
// so a sweep well under the retention window keeps that overshoot small.
const DefaultGCInterval = time.Hour

// Sentinel errors the gRPC layer matches with errors.Is to choose a status
// code; the wrapped message stays the human-facing instruction.
var (
	// ErrExists means a name is already taken in its parent directory.
	ErrExists = errors.New("path already exists")
	// ErrNotExist means a path component was not found.
	ErrNotExist = errors.New("path does not exist")
	// ErrInvalidPath means a path is malformed (empty, root, or "..").
	ErrInvalidPath = errors.New("invalid path")
	// ErrNotDir means a path component that must be a directory is a file.
	ErrNotDir = errors.New("not a directory")
	// ErrNotVolume means a path that must be a block volume is something else.
	ErrNotVolume = errors.New("not a volume")
	// ErrLeaseHeld means a lease operation was attempted by someone other than
	// the current holder — the caller has been fenced and must re-acquire.
	ErrLeaseHeld = errors.New("lease held by another writer")
)

// DefaultExtentSize is the copy-on-write unit for a block volume when none is
// given: the chunk size a single block write rewrites. 64 KiB keeps random
// write amplification modest without exploding the extent count of a large
// volume; override per volume at creation.
const DefaultExtentSize int64 = 64 * 1024

// rootID is the well-known id of the root directory. Every replica shares
// it so the trees rooted at the same inode converge without coordination.
const rootID = "root"

// InodeType distinguishes directories from files.
type InodeType uint8

const (
	// Dir is a directory inode; its children live in an OR-Set.
	Dir InodeType = iota
	// File is a leaf inode whose data is an append-ordered chunk manifest.
	File
	// Volume is a leaf inode whose data is an extent map (offset region to
	// chunk id), the backing store for a block device.
	Volume
)

// String renders the type for display.
func (t InodeType) String() string {
	switch t {
	case Dir:
		return "dir"
	case Volume:
		return "volume"
	default:
		return "file"
	}
}

// Entry is a directory child: a name bound to an inode id. It is the
// element of a directory's OR-Set; the add tag is the claim HLC.
type Entry struct {
	Name  string
	Inode string
}

// Inode is a directory or file. ACL is a last-writer-wins register;
// children is non-nil only for directories, manifest only for files. The
// manifest is an OR-Set of chunk ids tagged with the HLC at which the
// writer appended them, so reading it back in tag order reconstructs the
// byte stream in write order.
type Inode struct {
	ID       string
	Type     InodeType
	ACL      crdt.LWWRegister[string]
	children *crdt.ORSet[Entry]
	manifest *crdt.ORSet[string]
	// extents is non-nil only for volumes: an LWW-map from extent index to the
	// chunk id currently backing that region. ExtentSize is the bytes per
	// extent, fixed at creation. Overwriting a region rebinds its extent to a
	// fresh chunk (chunks are immutable), so a volume is copy-on-write.
	extents    *crdt.LWWMap[uint64, string]
	ExtentSize int64
	// lease is a volume's single-writer claim: a last-writer-wins register
	// whose value is the holder id ("" = vacant) and whose timestamp is the
	// acquisition HLC. That HLC is the fencing token — totally ordered, so the
	// newest claim always wins and stale holders are unambiguously fenced.
	lease crdt.LWWRegister[string]
}

// ResolvedEntry is one listed directory child after conflict resolution.
// When two replicas claimed the same name concurrently, the highest-HLC
// claim keeps the bare Name and the others are surfaced with a
// ".conflict-<hlc>" suffix and Conflict set, so nothing is lost.
type ResolvedEntry struct {
	Name     string
	Inode    string
	Type     InodeType
	Conflict bool
}

// Lease is a volume's single-writer claim. Holder is the writer id ("" means
// vacant); At is the acquisition HLC, which is both the last-writer-wins
// resolver and the fencing token — a write is honoured only while its At is the
// newest a data node has seen for the volume.
type Lease struct {
	Holder string        `json:"holder"`
	At     hlc.Timestamp `json:"at"`
}

// marshalNamespace is the serialization seam. Production uses json.Marshal;
// tests override it to exercise the persistence error paths.
var marshalNamespace = json.Marshal

// Namespace is a single replica of the cluster namespace. It is safe for
// concurrent use; every method takes an internal lock. When opened with a
// path it persists local mutations to disk and reloads them on the next
// Open; an empty path means in-memory only.
type Namespace struct {
	clock  *hlc.Clock
	path   string
	logger *slog.Logger

	// peerClock, when set, is called with the highest foreign timestamp seen
	// in each peer snapshot so a skew monitor can compare it to local time.
	peerClock func(node string, wall int64)

	mu     sync.Mutex
	inodes map[string]*Inode
}

// Option configures a Namespace at construction.
type Option func(*Namespace)

// WithPeerClockObserver registers a callback invoked, on each peer snapshot
// merged over the wire, with the highest timestamp issued by another node —
// the hook a clock-skew monitor uses to compare peer clocks against this one.
func WithPeerClockObserver(observe func(node string, wall int64)) Option {
	return func(n *Namespace) { n.peerClock = observe }
}

// New builds an in-memory namespace whose clock stamps local mutations. The
// root directory is seeded with the shared well-known id. Use Open to back
// the namespace with a file on disk.
func New(clock *hlc.Clock, opts ...Option) *Namespace {
	n := &Namespace{
		clock: clock,
		inodes: map[string]*Inode{
			rootID: {ID: rootID, Type: Dir, children: crdt.NewORSet[Entry]()},
		},
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// Open builds a namespace backed by the file at path: it loads any state
// previously persisted there and persists local mutations going forward. A
// missing file is a fresh start; a corrupt file is logged and ignored
// (the namespace re-converges from peers over gossip). A nil logger
// defaults to slog.Default.
func Open(clock *hlc.Clock, path string, logger *slog.Logger, opts ...Option) (*Namespace, error) {
	if logger == nil {
		logger = slog.Default()
	}
	ns := New(clock, opts...)
	ns.path = path
	ns.logger = logger

	ns.mu.Lock()
	defer ns.mu.Unlock()
	if err := ns.loadLocked(); err != nil {
		return nil, err
	}
	return ns, nil
}

// loadLocked merges any persisted state from n.path into the namespace.
func (n *Namespace) loadLocked() error {
	raw, err := os.ReadFile(n.path) // #nosec G304 -- path is operator config under DataDir, not request input
	if errors.Is(err, os.ErrNotExist) {
		return nil // fresh node; state arrives via local mutations or gossip
	}
	if err != nil {
		return fmt.Errorf("namespace: could not read the state file at %s (%w); check the data directory is readable", n.path, err)
	}
	if len(raw) == 0 {
		return nil
	}
	var w wireNamespace
	if err := json.Unmarshal(raw, &w); err != nil {
		// A corrupt cache file must not stop the node booting — it
		// re-learns the namespace from peers over gossip.
		n.logger.Warn("namespace: state file is corrupt; ignoring it and recovering from peers", "path", n.path, "error", err)
		return nil
	}
	n.mergeInodesLocked(fromWire(w).inodes)
	return nil
}

// persistLocked writes the current state to disk for namespaces opened with
// a path. It is best-effort: a failure is logged, not returned, because the
// mutation already applied in memory and will reach peers over gossip — the
// on-disk copy is a recovery cache, not the source of truth.
func (n *Namespace) persistLocked() {
	if n.path == "" {
		return
	}
	b, err := n.snapshotLocked()
	if err != nil {
		n.logger.Warn("namespace: could not serialise state to persist it", "error", err)
		return
	}
	if err := writeFileAtomic(n.path, b); err != nil {
		n.logger.Warn("namespace: could not persist state to disk; it will be recovered from peers on restart", "path", n.path, "error", err)
	}
}

// writeFileAtomic writes data via a temp file + rename so a crash leaves
// either the old state or the new, never a torn file.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Mkdir creates a directory at path; the parent must already exist.
func (n *Namespace) Mkdir(path string) (string, error) { return n.create(path, Dir) }

// Touch creates an empty file at path; the parent must already exist.
func (n *Namespace) Touch(path string) (string, error) { return n.create(path, File) }

func (n *Namespace) create(path string, typ InodeType) (string, error) {
	segs, err := splitPath(path)
	if err != nil {
		return "", err
	}
	if len(segs) == 0 {
		return "", fmt.Errorf("namespace: cannot create the root directory: %w", ErrInvalidPath)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	parent, err := n.resolveDirLocked(segs[:len(segs)-1])
	if err != nil {
		return "", err
	}
	name := segs[len(segs)-1]
	if _, exists := n.primaryChildLocked(parent, name); exists {
		return "", fmt.Errorf("namespace: %q already exists; remove it first or pick another name: %w", path, ErrExists)
	}

	ts := n.clock.Now()
	id := "inode-" + ts.String()
	inode := &Inode{ID: id, Type: typ}
	if typ == Dir {
		inode.children = crdt.NewORSet[Entry]()
	} else {
		inode.manifest = crdt.NewORSet[string]()
	}
	n.inodes[id] = inode
	parent.children.Add(Entry{Name: name, Inode: id}, ts)
	n.persistLocked()
	return id, nil
}

// Remove tombstones every entry with path's leaf name in its parent. The
// inode it pointed at is left for the scrubber/GC to reap once unreachable.
func (n *Namespace) Remove(path string) error {
	segs, err := splitPath(path)
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		return fmt.Errorf("namespace: cannot remove the root directory: %w", ErrInvalidPath)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	parent, err := n.resolveDirLocked(segs[:len(segs)-1])
	if err != nil {
		return err
	}
	name := segs[len(segs)-1]
	at := n.clock.Now()
	removed := false
	for _, e := range parent.children.Elements() {
		if e.Name == name {
			parent.children.Remove(e, at)
			removed = true
		}
	}
	if !removed {
		return fmt.Errorf("namespace: %q does not exist: %w", path, ErrNotExist)
	}
	n.persistLocked()
	return nil
}

// List returns the children of the directory at path, conflict-resolved and
// sorted by display name.
func (n *Namespace) List(path string) ([]ResolvedEntry, error) {
	segs, err := splitPath(path)
	if err != nil {
		return nil, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	dir, err := n.resolveDirLocked(segs)
	if err != nil {
		return nil, err
	}

	byName := map[string][]Entry{}
	for _, e := range dir.children.Elements() {
		byName[e.Name] = append(byName[e.Name], e)
	}

	var out []ResolvedEntry
	for name, entries := range byName {
		// Highest claim HLC first; that one keeps the bare name.
		sort.Slice(entries, func(i, j int) bool {
			ti, _ := dir.children.LiveTag(entries[i])
			tj, _ := dir.children.LiveTag(entries[j])
			return ti.After(tj)
		})
		for i, e := range entries {
			// A locally-created entry always has its inode, but a corrupt or
			// hostile peer payload merged via MergeBytes can carry a
			// directory entry that references an inode it never sent. Treat
			// such a dangling entry as a file rather than dereferencing nil.
			typ := File
			if in := n.inodes[e.Inode]; in != nil {
				typ = in.Type
			}
			re := ResolvedEntry{Name: name, Inode: e.Inode, Type: typ}
			if i > 0 {
				tag, _ := dir.children.LiveTag(e)
				re.Name = name + ".conflict-" + tag.String()
				re.Conflict = true
			}
			out = append(out, re)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// AppendChunk records that chunkID belongs to the file at path, tagged with
// the current HLC so the manifest reads back in write order. The file must
// already exist (a writer creates it first). Concurrent appends from
// different writers converge because the manifest is an OR-Set.
func (n *Namespace) AppendChunk(path, chunkID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	file, err := n.resolveFileLocked(path)
	if err != nil {
		return err
	}
	file.manifest.Add(chunkID, n.clock.Now())
	n.persistLocked()
	return nil
}

// Manifest returns the chunk ids of the file at path in write order (sorted
// by the HLC each was appended at), which is the order a reader concatenates
// them to reconstruct the byte stream.
func (n *Namespace) Manifest(path string) ([]string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	file, err := n.resolveFileLocked(path)
	if err != nil {
		return nil, err
	}
	ids := file.manifest.Elements()
	sort.Slice(ids, func(i, j int) bool {
		ti, _ := file.manifest.LiveTag(ids[i])
		tj, _ := file.manifest.LiveTag(ids[j])
		return ti.Before(tj)
	})
	return ids, nil
}

// resolveFileLocked resolves path to a file inode, erroring if it is the
// root, is missing, or is a directory (or references a missing inode after
// a corrupt merge).
func (n *Namespace) resolveFileLocked(path string) (*Inode, error) {
	segs, err := splitPath(path)
	if err != nil {
		return nil, err
	}
	if len(segs) == 0 {
		return nil, fmt.Errorf("namespace: the root is not a file: %w", ErrNotDir)
	}
	parent, err := n.resolveDirLocked(segs[:len(segs)-1])
	if err != nil {
		return nil, err
	}
	name := segs[len(segs)-1]
	id, ok := n.primaryChildLocked(parent, name)
	if !ok {
		return nil, fmt.Errorf("namespace: %q does not exist: %w", path, ErrNotExist)
	}
	inode := n.inodes[id]
	if inode == nil || inode.Type != File {
		return nil, fmt.Errorf("namespace: %q is not a file: %w", path, ErrNotDir)
	}
	return inode, nil
}

// CreateVolume creates a block volume at path with the given extent size (the
// copy-on-write unit, in bytes); a non-positive size falls back to
// DefaultExtentSize. The parent directory must already exist. Returns the new
// inode id.
func (n *Namespace) CreateVolume(path string, extentSize int64) (string, error) {
	if extentSize <= 0 {
		extentSize = DefaultExtentSize
	}
	segs, err := splitPath(path)
	if err != nil {
		return "", err
	}
	if len(segs) == 0 {
		return "", fmt.Errorf("namespace: cannot create the root directory as a volume: %w", ErrInvalidPath)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	parent, err := n.resolveDirLocked(segs[:len(segs)-1])
	if err != nil {
		return "", err
	}
	name := segs[len(segs)-1]
	if _, exists := n.primaryChildLocked(parent, name); exists {
		return "", fmt.Errorf("namespace: %q already exists; remove it first or pick another name: %w", path, ErrExists)
	}

	ts := n.clock.Now()
	id := "inode-" + ts.String()
	n.inodes[id] = &Inode{ID: id, Type: Volume, extents: crdt.NewLWWMap[uint64, string](), ExtentSize: extentSize}
	parent.children.Add(Entry{Name: name, Inode: id}, ts)
	n.persistLocked()
	return id, nil
}

// WriteExtent rebinds the extent at index of the volume at path to chunkID,
// stamped with the current HLC so the latest write wins after a merge. The
// caller has already stored chunkID; this records where it lives in the
// volume's address space.
func (n *Namespace) WriteExtent(path string, index uint64, chunkID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	vol, err := n.resolveVolumeLocked(path)
	if err != nil {
		return err
	}
	vol.extents.Set(index, chunkID, n.clock.Now())
	n.persistLocked()
	return nil
}

// Extents returns the volume's current extent-to-chunk bindings as a map from
// extent index to chunk id. Unmapped extents are absent (they read as zeros).
func (n *Namespace) Extents(path string) (map[uint64]string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	vol, err := n.resolveVolumeLocked(path)
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]string, vol.extents.Len())
	for _, e := range vol.extents.Entries() {
		out[e.Key] = e.Value
	}
	return out, nil
}

// ExtentSize returns the volume's copy-on-write unit in bytes.
func (n *Namespace) ExtentSize(path string) (int64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	vol, err := n.resolveVolumeLocked(path)
	if err != nil {
		return 0, err
	}
	return vol.ExtentSize, nil
}

// resolveVolumeLocked resolves path to a volume inode, erroring if it is the
// root, is missing, or is not a volume.
func (n *Namespace) resolveVolumeLocked(path string) (*Inode, error) {
	segs, err := splitPath(path)
	if err != nil {
		return nil, err
	}
	if len(segs) == 0 {
		return nil, fmt.Errorf("namespace: the root is not a volume: %w", ErrNotVolume)
	}
	parent, err := n.resolveDirLocked(segs[:len(segs)-1])
	if err != nil {
		return nil, err
	}
	name := segs[len(segs)-1]
	id, ok := n.primaryChildLocked(parent, name)
	if !ok {
		return nil, fmt.Errorf("namespace: %q does not exist: %w", path, ErrNotExist)
	}
	inode := n.inodes[id]
	if inode == nil || inode.Type != Volume {
		return nil, fmt.Errorf("namespace: %q is not a volume: %w", path, ErrNotVolume)
	}
	return inode, nil
}

// AcquireLease claims the volume at path for holder, stealing it from any
// current holder: the claim is stamped with a fresh HLC and, because the lease
// is last-writer-wins, the newest claim wins cluster-wide while older holders
// are fenced. Deciding when a takeover is appropriate (e.g. after a holder
// stops renewing) is the caller's policy; this is the mechanism.
func (n *Namespace) AcquireLease(path, holder string) (Lease, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	vol, err := n.resolveVolumeLocked(path)
	if err != nil {
		return Lease{}, err
	}
	at := n.clock.Now()
	vol.lease = crdt.Set(holder, at)
	n.persistLocked()
	return Lease{Holder: holder, At: at}, nil
}

// RenewLease refreshes holder's claim with a newer HLC so a peer's
// takeover-after-timeout does not fire. It fails with ErrLeaseHeld if holder no
// longer holds the lease, so the caller learns it has been fenced and must
// stop writing.
func (n *Namespace) RenewLease(path, holder string) (Lease, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	vol, err := n.resolveVolumeLocked(path)
	if err != nil {
		return Lease{}, err
	}
	if vol.lease.Value != holder {
		return Lease{}, fmt.Errorf("namespace: %q is held by %s, not %q; re-acquire before writing: %w", path, leaseHolderName(vol.lease.Value), holder, ErrLeaseHeld)
	}
	at := n.clock.Now()
	vol.lease = crdt.Set(holder, at)
	n.persistLocked()
	return Lease{Holder: holder, At: at}, nil
}

// ReleaseLease relinquishes holder's claim, leaving the volume vacant so a peer
// can acquire it cleanly. It fails with ErrLeaseHeld if holder does not
// currently hold the lease.
func (n *Namespace) ReleaseLease(path, holder string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	vol, err := n.resolveVolumeLocked(path)
	if err != nil {
		return err
	}
	if vol.lease.Value != holder {
		return fmt.Errorf("namespace: %q is held by %s, not %q: %w", path, leaseHolderName(vol.lease.Value), holder, ErrLeaseHeld)
	}
	vol.lease = crdt.Set("", n.clock.Now())
	n.persistLocked()
	return nil
}

// Lease returns the volume's current single-writer claim; a zero Holder means
// the volume is vacant.
func (n *Namespace) Lease(path string) (Lease, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	vol, err := n.resolveVolumeLocked(path)
	if err != nil {
		return Lease{}, err
	}
	return Lease{Holder: vol.lease.Value, At: vol.lease.TS}, nil
}

func leaseHolderName(holder string) string {
	if holder == "" {
		return "nobody (vacant)"
	}
	return strconv.Quote(holder)
}

// Merge folds another replica's state into this one. It is commutative and
// idempotent: inode ACLs merge last-writer-wins and directory children
// merge as OR-Sets, so replicas that have exchanged state converge on an
// identical tree, conflicts and all.
func (n *Namespace) Merge(other *Namespace) {
	snap := other.snapshot()

	n.mu.Lock()
	defer n.mu.Unlock()
	n.mergeInodesLocked(snap)
}

// mergeInodesLocked folds a snapshot of inodes into the table: new inodes
// are adopted, shared ones merge their ACL (LWW) and children (OR-Set).
func (n *Namespace) mergeInodesLocked(snap map[string]*Inode) {
	for id, oi := range snap {
		mine, ok := n.inodes[id]
		if !ok {
			n.inodes[id] = oi // already a deep copy from snapshot
			continue
		}
		mine.ACL = mine.ACL.Merge(oi.ACL)
		mine.lease = mine.lease.Merge(oi.lease)
		if mine.children != nil && oi.children != nil {
			mine.children.Merge(oi.children)
		}
		if mine.manifest != nil && oi.manifest != nil {
			mine.manifest.Merge(oi.manifest)
		}
		if mine.extents != nil && oi.extents != nil {
			mine.extents.Merge(oi.extents)
		}
	}
}

// snapshot returns a deep copy of every inode, safe to merge into another
// replica without holding this one's lock.
func (n *Namespace) snapshot() map[string]*Inode {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string]*Inode, len(n.inodes))
	for id, in := range n.inodes {
		cp := &Inode{ID: in.ID, Type: in.Type, ACL: in.ACL, ExtentSize: in.ExtentSize, lease: in.lease}
		if in.children != nil {
			cp.children = in.children.Clone()
		}
		if in.manifest != nil {
			cp.manifest = in.manifest.Clone()
		}
		if in.extents != nil {
			cp.extents = in.extents.Clone()
		}
		out[id] = cp
	}
	return out
}

// wireNamespace is the JSON shape exchanged between replicas during
// anti-entropy. It carries full inode state — ACL register plus the
// directory OR-Set's add/remove tags — which is enough for Merge to
// reconcile two replicas.
type wireNamespace struct {
	Inodes []wireInode `json:"inodes"`
}

type wireInode struct {
	ID       string                          `json:"id"`
	Type     InodeType                       `json:"type"`
	ACLValue string                          `json:"acl_value,omitempty"`
	ACLTS    hlc.Timestamp                   `json:"acl_ts"`
	Adds     []crdt.ElementTags[Entry]       `json:"adds,omitempty"`
	Removes  []crdt.ElementTombstones[Entry] `json:"removes,omitempty"`

	ManifestAdds    []crdt.ElementTags[string]       `json:"manifest_adds,omitempty"`
	ManifestRemoves []crdt.ElementTombstones[string] `json:"manifest_removes,omitempty"`

	ExtentSize int64                           `json:"extent_size,omitempty"`
	Extents    []crdt.MapEntry[uint64, string] `json:"extents,omitempty"`

	LeaseHolder string         `json:"lease_holder,omitempty"`
	LeaseTS     *hlc.Timestamp `json:"lease_ts,omitempty"`
}

// Snapshot serializes the whole namespace for an anti-entropy exchange.
func (n *Namespace) Snapshot() ([]byte, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapshotLocked()
}

func (n *Namespace) snapshotLocked() ([]byte, error) {
	w := wireNamespace{Inodes: make([]wireInode, 0, len(n.inodes))}
	for _, in := range n.inodes {
		wi := wireInode{ID: in.ID, Type: in.Type, ACLValue: in.ACL.Value, ACLTS: in.ACL.TS}
		if in.children != nil {
			wi.Adds, wi.Removes = in.children.Export()
		}
		if in.manifest != nil {
			wi.ManifestAdds, wi.ManifestRemoves = in.manifest.Export()
		}
		if in.extents != nil {
			wi.ExtentSize = in.ExtentSize
			wi.Extents = in.extents.Entries()
		}
		if !in.lease.TS.IsZero() {
			ts := in.lease.TS
			wi.LeaseHolder = in.lease.Value
			wi.LeaseTS = &ts
		}
		w.Inodes = append(w.Inodes, wi)
	}
	return marshalNamespace(w)
}

// MergeBytes decodes a peer's snapshot and merges it. Because Merge is
// commutative and idempotent, applying a peer's state any number of times
// and in any order converges both replicas.
func (n *Namespace) MergeBytes(b []byte) error {
	var w wireNamespace
	if err := json.Unmarshal(b, &w); err != nil {
		return fmt.Errorf("namespace: could not decode a peer's state (%w); both nodes must run the same silo version", err)
	}
	n.observePeerClocks(w)
	n.Merge(fromWire(w))
	return nil
}

// observePeerClocks reports the highest timestamp issued by another node in a
// peer snapshot to the registered observer. It is the skew-monitor seam:
// scanning the wire form (rather than the merged CRDT) keeps it on the receive
// path and out of the local mutation path. Timestamps this node issued itself
// are skipped — comparing our clock to our own past says nothing about skew.
func (n *Namespace) observePeerClocks(w wireNamespace) {
	if n.peerClock == nil || n.clock == nil {
		return
	}
	self := n.clock.Node()
	var top hlc.Timestamp
	consider := func(ts hlc.Timestamp) {
		if ts.Node == "" || ts.Node == self {
			return
		}
		if top.Before(ts) {
			top = ts
		}
	}
	for _, wi := range w.Inodes {
		consider(wi.ACLTS)
		for _, a := range wi.Adds {
			for _, t := range a.Tags {
				consider(t)
			}
		}
		for _, a := range wi.ManifestAdds {
			for _, t := range a.Tags {
				consider(t)
			}
		}
		for _, r := range wi.Removes {
			for _, tb := range r.Tombstones {
				consider(tb.Add)
				consider(tb.At)
			}
		}
		for _, r := range wi.ManifestRemoves {
			for _, tb := range r.Tombstones {
				consider(tb.Add)
				consider(tb.At)
			}
		}
		for _, e := range wi.Extents {
			consider(e.TS)
		}
		if wi.LeaseTS != nil {
			consider(*wi.LeaseTS)
		}
	}
	if !top.IsZero() {
		n.peerClock(top.Node, top.Wall)
	}
}

// fromWire rebuilds a clock-less namespace from its wire form. It is only
// ever used as a Merge source, which never touches the clock.
func fromWire(w wireNamespace) *Namespace {
	ns := &Namespace{inodes: make(map[string]*Inode, len(w.Inodes))}
	for _, wi := range w.Inodes {
		in := &Inode{
			ID:   wi.ID,
			Type: wi.Type,
			ACL:  crdt.LWWRegister[string]{Value: wi.ACLValue, TS: wi.ACLTS},
		}
		switch wi.Type {
		case Dir:
			in.children = crdt.NewORSet[Entry]()
			in.children.Import(wi.Adds, wi.Removes)
		case Volume:
			in.ExtentSize = wi.ExtentSize
			in.extents = crdt.NewLWWMap[uint64, string]()
			in.extents.Import(wi.Extents)
			if wi.LeaseTS != nil {
				in.lease = crdt.Set(wi.LeaseHolder, *wi.LeaseTS)
			}
		default:
			in.manifest = crdt.NewORSet[string]()
			in.manifest.Import(wi.ManifestAdds, wi.ManifestRemoves)
		}
		ns.inodes[wi.ID] = in
	}
	return ns
}

// GC reclaims tombstones older than retention across every directory,
// returning the number of tag entries reclaimed. A tombstone must outlive
// retention so its removal reaches every replica before the memory is
// freed; reclaiming sooner would let a replica that never saw the removal
// resurrect the entry on the next merge.
func (n *Namespace) GC(retention time.Duration) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	cutoff := hlc.Timestamp{Wall: n.clock.Now().Wall - retention.Nanoseconds()}
	reclaimed := 0
	for _, in := range n.inodes {
		if in.children != nil {
			reclaimed += in.children.GC(cutoff)
		}
		if in.manifest != nil {
			reclaimed += in.manifest.GC(cutoff)
		}
	}
	return reclaimed
}

// RunGC sweeps tombstones every interval until ctx is cancelled. A
// non-positive interval falls back to DefaultGCInterval. Intended to run in
// its own goroutine for the life of the daemon.
func (n *Namespace) RunGC(ctx context.Context, retention, interval time.Duration, logger *slog.Logger) {
	if interval <= 0 {
		interval = DefaultGCInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if reclaimed := n.GC(retention); reclaimed > 0 {
				logger.Info("namespace tombstone GC reclaimed entries", "count", reclaimed)
			}
		}
	}
}

// resolveDirLocked walks segments from the root, following the primary
// claimant at each step, and returns the directory they name.
func (n *Namespace) resolveDirLocked(segments []string) (*Inode, error) {
	cur := n.inodes[rootID]
	for _, name := range segments {
		childID, ok := n.primaryChildLocked(cur, name)
		if !ok {
			return nil, fmt.Errorf("namespace: %q does not exist: %w", name, ErrNotExist)
		}
		child := n.inodes[childID]
		if child == nil || child.Type != Dir {
			return nil, fmt.Errorf("namespace: %q is not a directory: %w", name, ErrNotDir)
		}
		cur = child
	}
	return cur, nil
}

// primaryChildLocked returns the inode id of the highest-HLC live claim for
// name in dir, which is the entry a path lookup follows.
func (n *Namespace) primaryChildLocked(dir *Inode, name string) (string, bool) {
	var (
		bestInode string
		bestTag   hlc.Timestamp
		found     bool
	)
	for _, e := range dir.children.Elements() {
		if e.Name != name {
			continue
		}
		// Elements only returns present entries, so the tag is always live.
		tag, _ := dir.children.LiveTag(e)
		if !found || tag.After(bestTag) {
			bestInode, bestTag, found = e.Inode, tag, true
		}
	}
	return bestInode, found
}

// splitPath normalizes a slash path into its segments, rejecting parent
// traversal so a name can never escape its directory.
func splitPath(p string) ([]string, error) {
	out := make([]string, 0)
	for _, seg := range strings.Split(p, "/") {
		switch seg {
		case "", ".":
			continue
		case "..":
			return nil, fmt.Errorf("namespace: %q contains \"..\"; parent traversal is not supported: %w", p, ErrInvalidPath)
		default:
			out = append(out, seg)
		}
	}
	return out, nil
}
