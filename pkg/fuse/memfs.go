package fuse

import "sync"

// MemFS is an in-memory Filesystem: the reference backend the library ships
// with, and what the protocol tests run against. It is safe for concurrent use.
// File handles are the node id itself — MemFS keeps no per-open state — so Open
// is cheap and Release is a no-op.
type MemFS struct {
	mu     sync.Mutex
	nodes  map[uint64]*memNode
	nextID uint64
}

type memNode struct {
	attr     Attr
	data     []byte            // files
	children map[string]uint64 // directories
}

func (n *memNode) isDir() bool { return n.attr.Mode&ModeDir != 0 }

// NewMemFS returns an empty in-memory filesystem with just a root directory.
func NewMemFS() *MemFS {
	root := &memNode{
		attr:     Attr{Ino: RootNodeID, Mode: ModeDir | 0o755, Nlink: 2},
		children: map[string]uint64{},
	}
	return &MemFS{nodes: map[uint64]*memNode{RootNodeID: root}, nextID: RootNodeID + 1}
}

// Lookup implements Filesystem.
func (m *MemFS) Lookup(parent uint64, name string) (uint64, Attr, Errno) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.nodes[parent]
	if !ok || !p.isDir() {
		return 0, Attr{}, ENOTDIR
	}
	id, ok := p.children[name]
	if !ok {
		return 0, Attr{}, ENOENT
	}
	return id, m.nodes[id].attr, OK
}

// Forget implements Filesystem.
func (m *MemFS) Forget(uint64, uint64) {}

// Getattr implements Filesystem.
func (m *MemFS) Getattr(nodeID uint64) (Attr, Errno) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[nodeID]
	if !ok {
		return Attr{}, ENOENT
	}
	return n.attr, OK
}

// Setattr implements Filesystem.
func (m *MemFS) Setattr(nodeID uint64, in SetattrIn) (Attr, Errno) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[nodeID]
	if !ok {
		return Attr{}, ENOENT
	}
	if in.Valid&SetattrSize != 0 && !n.isDir() {
		n.data = resize(n.data, int(in.Size)) //nolint:gosec // truncate size is bounded by the file
		n.attr.Size = in.Size
	}
	if in.Valid&SetattrMode != 0 {
		n.attr.Mode = (n.attr.Mode & ModeMask) | (in.Mode &^ ModeMask)
	}
	return n.attr, OK
}

// Mkdir implements Filesystem.
func (m *MemFS) Mkdir(parent uint64, name string, mode uint32) (uint64, Attr, Errno) {
	return m.create(parent, name, ModeDir|(mode&^ModeMask), true)
}

// Create implements Filesystem.
func (m *MemFS) Create(parent uint64, name string, _ uint32, mode uint32) (uint64, Attr, uint64, Errno) {
	id, attr, errno := m.create(parent, name, ModeReg|(mode&^ModeMask), false)
	return id, attr, id, errno
}

func (m *MemFS) create(parent uint64, name string, mode uint32, dir bool) (uint64, Attr, Errno) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.nodes[parent]
	if !ok || !p.isDir() {
		return 0, Attr{}, ENOTDIR
	}
	if _, exists := p.children[name]; exists {
		return 0, Attr{}, EEXIST
	}
	id := m.nextID
	m.nextID++
	nlink := uint32(1)
	if dir {
		nlink = 2
	}
	node := &memNode{attr: Attr{Ino: id, Mode: mode, Nlink: nlink}}
	if dir {
		node.children = map[string]uint64{}
	}
	m.nodes[id] = node
	p.children[name] = id
	return id, node.attr, OK
}

// Rmdir implements Filesystem.
func (m *MemFS) Rmdir(parent uint64, name string) Errno {
	return m.remove(parent, name, true)
}

// Unlink implements Filesystem.
func (m *MemFS) Unlink(parent uint64, name string) Errno {
	return m.remove(parent, name, false)
}

func (m *MemFS) remove(parent uint64, name string, dir bool) Errno {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.nodes[parent]
	if !ok || !p.isDir() {
		return ENOTDIR
	}
	id, ok := p.children[name]
	if !ok {
		return ENOENT
	}
	n := m.nodes[id]
	if dir {
		if !n.isDir() {
			return ENOTDIR
		}
		if len(n.children) != 0 {
			return ENOTEMPTY
		}
	} else if n.isDir() {
		return EISDIR
	}
	delete(p.children, name)
	delete(m.nodes, id)
	return OK
}

// Open implements Filesystem.
func (m *MemFS) Open(nodeID uint64, _ uint32) (uint64, Errno) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nodes[nodeID]; !ok {
		return 0, ENOENT
	}
	return nodeID, OK
}

// Opendir implements Filesystem.
func (m *MemFS) Opendir(nodeID uint64, _ uint32) (uint64, Errno) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[nodeID]
	if !ok {
		return 0, ENOENT
	}
	if !n.isDir() {
		return 0, ENOTDIR
	}
	return nodeID, OK
}

// Read implements Filesystem.
func (m *MemFS) Read(nodeID, _, offset uint64, size uint32) ([]byte, Errno) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[nodeID]
	if !ok {
		return nil, ENOENT
	}
	if n.isDir() {
		return nil, EISDIR
	}
	if offset >= uint64(len(n.data)) {
		return nil, OK // EOF: a zero-length read
	}
	end := offset + uint64(size)
	if end > uint64(len(n.data)) {
		end = uint64(len(n.data))
	}
	out := make([]byte, end-offset)
	copy(out, n.data[offset:end])
	return out, OK
}

// Write implements Filesystem.
func (m *MemFS) Write(nodeID, _, offset uint64, data []byte) (uint32, Errno) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[nodeID]
	if !ok {
		return 0, ENOENT
	}
	if n.isDir() {
		return 0, EISDIR
	}
	end := int(offset) + len(data) //nolint:gosec // chunk lengths are bounded
	if end > len(n.data) {
		n.data = resize(n.data, end)
	}
	copy(n.data[offset:], data)
	n.attr.Size = uint64(len(n.data))
	return uint32(len(data)), OK //nolint:gosec // name length is bounded
}

// Flush implements Filesystem.
func (m *MemFS) Flush(uint64, uint64) Errno { return OK }

// Release implements Filesystem.
func (m *MemFS) Release(uint64, uint64) Errno { return OK }

// ReadDir implements Filesystem.
func (m *MemFS) ReadDir(nodeID uint64) ([]DirEntry, Errno) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[nodeID]
	if !ok {
		return nil, ENOENT
	}
	if !n.isDir() {
		return nil, ENOTDIR
	}
	out := make([]DirEntry, 0, len(n.children))
	for name, id := range n.children {
		out = append(out, DirEntry{Ino: id, Name: name, Mode: m.nodes[id].attr.Mode})
	}
	return out, OK
}

// resize grows or shrinks a byte slice to n, zero-filling any growth.
func resize(b []byte, n int) []byte {
	if n <= len(b) {
		return b[:n]
	}
	return append(b, make([]byte, n-len(b))...)
}

// Compile-time check that MemFS satisfies the backend contract.
var _ Filesystem = (*MemFS)(nil)
