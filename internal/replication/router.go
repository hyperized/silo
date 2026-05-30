package replication

import (
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/placement"
)

// maxCapacityWeight caps how many times more ring space the biggest node can
// own relative to the smallest, bounding both ring memory and the blast radius
// of a single huge node.
const maxCapacityWeight = 16

// Router is the production Placement: it derives the consistent-hash ring
// from the live membership view and resolves node ids to their gossiped
// data addresses. The ring is rebuilt only when the set of live node ids
// changes, so the common case (a steady cluster) is a cheap lookup. Safe
// for concurrent use.
type Router struct {
	members *membership.Membership

	mu   sync.Mutex
	sig  string
	ring *placement.Ring
}

// NewRouter builds a Router over a membership table. The first Replicas
// call materialises the ring.
func NewRouter(members *membership.Membership) *Router {
	return &Router{members: members}
}

// Replicas resolves chunkID to up to n replica node ids over the current
// ring, rebuilding it first if membership changed since the last call.
func (r *Router) Replicas(chunkID string, n int) []string {
	return r.currentRing().Replicas(chunkID, n)
}

// SelfID returns the local node id.
func (r *Router) SelfID() string { return r.members.SelfID() }

// DataAddr returns the node's gossiped gRPC data address. ok is false when
// the node is unknown or has not advertised an address yet, so the caller
// can skip it rather than dial an empty target.
func (r *Router) DataAddr(nodeID string) (string, bool) {
	n, ok := r.members.Lookup(nodeID)
	if !ok || n.DataAddress == "" {
		return "", false
	}
	return n.DataAddress, true
}

// currentRing returns the ring for the present set of live nodes, weighted by
// their advertised capacity, rebuilding it only when the membership or the
// derived weights change.
func (r *Router) currentRing() *placement.Ring {
	nodes := r.aliveNodes()
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	weights := capacityWeights(nodes)
	sig := ringSignature(ids, weights)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ring == nil || sig != r.sig {
		r.ring = placement.NewWeighted(ids, weights, placement.DefaultVNodes)
		r.sig = sig
	}
	return r.ring
}

// aliveNodes is the local node plus every Alive peer, sorted by id. The local
// node is always included: it is Alive from its own perspective and is a valid
// replica target. Sorting makes the ring signature stable.
func (r *Router) aliveNodes() []membership.Node {
	peers := r.members.AlivePeers()
	nodes := make([]membership.Node, 0, len(peers)+1)
	nodes = append(nodes, r.members.Self())
	nodes = append(nodes, peers...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

// capacityWeights derives per-node ring weights from advertised capacity,
// scaled so the smallest-capacity node weighs 1 and larger nodes weigh
// proportionally more (capped at maxCapacityWeight). It returns nil — meaning
// equal weights, an unweighted ring identical to the old behaviour — unless at
// least two nodes have advertised capacity and the weights actually differ, so
// a homogeneous cluster (or one that hasn't reported capacity yet) is never
// reshuffled.
func capacityWeights(nodes []membership.Node) map[string]int {
	var minCap int64
	positive := 0
	for _, n := range nodes {
		if n.CapacityBytes > 0 {
			positive++
			if minCap == 0 || n.CapacityBytes < minCap {
				minCap = n.CapacityBytes
			}
		}
	}
	if positive < 2 || minCap == 0 {
		return nil
	}

	weights := make(map[string]int, len(nodes))
	allOne := true
	for _, n := range nodes {
		w := 1
		if n.CapacityBytes > 0 {
			// n.CapacityBytes >= minCap > 0, so this rounds to at least 1.
			w = int((n.CapacityBytes + minCap/2) / minCap)
			if w > maxCapacityWeight {
				w = maxCapacityWeight
			}
		}
		weights[n.ID] = w
		if w != 1 {
			allOne = false
		}
	}
	if allOne {
		return nil
	}
	return weights
}

// ringSignature is a stable key for the (ids, weights) pair, so the ring is
// rebuilt when membership or any weight changes but not otherwise.
func ringSignature(ids []string, weights map[string]int) string {
	var b strings.Builder
	for _, id := range ids {
		b.WriteString(id)
		if w := weights[id]; w > 1 {
			b.WriteByte(':')
			b.WriteString(strconv.Itoa(w))
		}
		b.WriteByte(',')
	}
	return b.String()
}
