// Package placement maps chunk ids to an ordered set of storage nodes via
// a consistent hash ring. Every node builds the ring from the same
// membership view, so all nodes independently agree on where a chunk
// lives — no central allocator and no coordination on the write path.
package placement

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// DefaultVNodes is the number of virtual points placed on the ring per
// real node. More virtual nodes smooths the key distribution across nodes
// at the cost of ring memory and build time; 128 keeps the load imbalance
// between nodes comfortably low for the small clusters silo targets.
const DefaultVNodes = 128

// Ring is an immutable consistent-hash ring over a fixed set of node ids.
// It is safe for concurrent use precisely because it never changes after
// New returns: when membership changes, build a fresh Ring and swap it in
// (e.g. via an atomic.Pointer) rather than mutating one in place.
type Ring struct {
	points []point  // sorted ascending by hash, then node id
	nodes  []string // distinct node ids, sorted; the ring's membership
}

type point struct {
	hash uint64
	node string
}

// New builds a ring with vnodes virtual points for each unique, non-empty
// node id. Empty and duplicate ids are ignored so the ring membership is
// always well-defined. A vnodes value <= 0 means DefaultVNodes, so callers
// can pass 0 to accept the default.
func New(nodeIDs []string, vnodes int) *Ring {
	return NewWeighted(nodeIDs, nil, vnodes)
}

// NewWeighted builds a ring where each node gets weight*vnodes virtual points,
// so a node with double the weight owns roughly double the key space. weights
// maps node id to a positive weight; a node missing from the map (or with a
// weight < 1) gets weight 1. With weights nil or all-ones the ring is byte-for-
// byte identical to New(nodeIDs, vnodes) — capacity-aware placement is a strict
// generalisation that does not disturb a homogeneous cluster.
//
// This is how silo rebalances for capacity: heavier-capacity nodes are given
// more weight, so the deterministic ring assigns them more chunks, and the
// re-replication scrubber moves data to match the new ring. No chunk is ever
// stored off-ring, so reads still resolve placement from the ring alone.
func NewWeighted(nodeIDs []string, weights map[string]int, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVNodes
	}

	seen := make(map[string]struct{}, len(nodeIDs))
	nodes := make([]string, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		nodes = append(nodes, id)
	}
	sort.Strings(nodes)

	points := make([]point, 0, len(nodes)*vnodes)
	for _, id := range nodes {
		w := weights[id]
		if w < 1 {
			w = 1
		}
		for i := 0; i < w*vnodes; i++ {
			points = append(points, point{hash: hashVNode(id, i), node: id})
		}
	}
	sort.Slice(points, func(i, j int) bool { return lessPoint(points[i], points[j]) })

	return &Ring{points: points, nodes: nodes}
}

// Replicas returns up to n distinct node ids responsible for key, in
// priority order (primary first). It walks the ring clockwise from the
// key's hash, collecting distinct owners. When n exceeds the ring size it
// returns every node once; when the ring is empty or n <= 0 it returns nil.
func (r *Ring) Replicas(key string, n int) []string {
	if n <= 0 || len(r.points) == 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}

	h := hashKey(key)
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if start == len(r.points) {
		start = 0 // key hashes past the last point; wrap to the ring's head
	}

	out := make([]string, 0, n)
	seen := make(map[string]struct{}, n)
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		node := r.points[(start+i)%len(r.points)].node
		if _, dup := seen[node]; dup {
			continue
		}
		seen[node] = struct{}{}
		out = append(out, node)
	}
	return out
}

// Nodes returns the ring's node ids in sorted order. The returned slice is
// a copy, so callers cannot mutate the ring through it.
func (r *Ring) Nodes() []string {
	out := make([]string, len(r.nodes))
	copy(out, r.nodes)
	return out
}

// Len reports the number of distinct nodes in the ring.
func (r *Ring) Len() int { return len(r.nodes) }

// lessPoint orders ring points by hash, breaking the astronomically rare
// hash collision by node id so every node builds an identically-ordered
// ring and therefore agrees on placement.
func lessPoint(a, b point) bool {
	if a.hash != b.hash {
		return a.hash < b.hash
	}
	return a.node < b.node
}

func hashVNode(node string, i int) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(node))
	_, _ = h.Write([]byte{'#'})
	_, _ = h.Write([]byte(strconv.Itoa(i)))
	return h.Sum64()
}

func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}
