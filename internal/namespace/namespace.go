// Package namespace is silo's coordinator-free filesystem namespace: a tree
// of directories and inodes built from CRDTs and stamped with hybrid
// logical clocks. Every node keeps its own replica; replicas exchange state
// and Merge converges them deterministically, surfacing concurrent
// same-name creates as conflicts rather than silently dropping one.
package namespace

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

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
)

// rootID is the well-known id of the root directory. Every replica shares
// it so the trees rooted at the same inode converge without coordination.
const rootID = "root"

// InodeType distinguishes directories from files.
type InodeType uint8

const (
	// Dir is a directory inode; its children live in an OR-Set.
	Dir InodeType = iota
	// File is a leaf inode.
	File
)

// String renders the type for display.
func (t InodeType) String() string {
	if t == Dir {
		return "dir"
	}
	return "file"
}

// Entry is a directory child: a name bound to an inode id. It is the
// element of a directory's OR-Set; the add tag is the claim HLC.
type Entry struct {
	Name  string
	Inode string
}

// Inode is a directory or file. ACL is a last-writer-wins register;
// children is non-nil only for directories.
type Inode struct {
	ID       string
	Type     InodeType
	ACL      crdt.LWWRegister[string]
	children *crdt.ORSet[Entry]
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

// Namespace is a single replica of the cluster namespace. It is safe for
// concurrent use; every method takes an internal lock.
type Namespace struct {
	clock *hlc.Clock

	mu     sync.Mutex
	inodes map[string]*Inode
}

// New builds an empty namespace whose clock stamps local mutations. The
// root directory is seeded with the shared well-known id.
func New(clock *hlc.Clock) *Namespace {
	return &Namespace{
		clock: clock,
		inodes: map[string]*Inode{
			rootID: {ID: rootID, Type: Dir, children: crdt.NewORSet[Entry]()},
		},
	}
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
	}
	n.inodes[id] = inode
	parent.children.Add(Entry{Name: name, Inode: id}, ts)
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
	removed := false
	for _, e := range parent.children.Elements() {
		if e.Name == name {
			parent.children.Remove(e)
			removed = true
		}
	}
	if !removed {
		return fmt.Errorf("namespace: %q does not exist: %w", path, ErrNotExist)
	}
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
			// Every live entry's inode is present: inodes are only ever
			// added, never deleted, until tombstone GC arrives.
			re := ResolvedEntry{Name: name, Inode: e.Inode, Type: n.inodes[e.Inode].Type}
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

// Merge folds another replica's state into this one. It is commutative and
// idempotent: inode ACLs merge last-writer-wins and directory children
// merge as OR-Sets, so replicas that have exchanged state converge on an
// identical tree, conflicts and all.
func (n *Namespace) Merge(other *Namespace) {
	snap := other.snapshot()

	n.mu.Lock()
	defer n.mu.Unlock()
	for id, oi := range snap {
		mine, ok := n.inodes[id]
		if !ok {
			n.inodes[id] = oi // already a deep copy from snapshot
			continue
		}
		mine.ACL = mine.ACL.Merge(oi.ACL)
		if mine.children != nil && oi.children != nil {
			mine.children.Merge(oi.children)
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
		cp := &Inode{ID: in.ID, Type: in.Type, ACL: in.ACL}
		if in.children != nil {
			cp.children = in.children.Clone()
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
	ID       string                    `json:"id"`
	Type     InodeType                 `json:"type"`
	ACLValue string                    `json:"acl_value,omitempty"`
	ACLTS    hlc.Timestamp             `json:"acl_ts"`
	Adds     []crdt.ElementTags[Entry] `json:"adds,omitempty"`
	Removes  []crdt.ElementTags[Entry] `json:"removes,omitempty"`
}

// Snapshot serializes the whole namespace for an anti-entropy exchange.
func (n *Namespace) Snapshot() ([]byte, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	w := wireNamespace{Inodes: make([]wireInode, 0, len(n.inodes))}
	for _, in := range n.inodes {
		wi := wireInode{ID: in.ID, Type: in.Type, ACLValue: in.ACL.Value, ACLTS: in.ACL.TS}
		if in.children != nil {
			wi.Adds, wi.Removes = in.children.Export()
		}
		w.Inodes = append(w.Inodes, wi)
	}
	return json.Marshal(w)
}

// MergeBytes decodes a peer's snapshot and merges it. Because Merge is
// commutative and idempotent, applying a peer's state any number of times
// and in any order converges both replicas.
func (n *Namespace) MergeBytes(b []byte) error {
	var w wireNamespace
	if err := json.Unmarshal(b, &w); err != nil {
		return fmt.Errorf("namespace: could not decode a peer's state (%w); both nodes must run the same silo version", err)
	}
	n.Merge(fromWire(w))
	return nil
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
		if wi.Type == Dir {
			in.children = crdt.NewORSet[Entry]()
			in.children.Import(wi.Adds, wi.Removes)
		}
		ns.inodes[wi.ID] = in
	}
	return ns
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
