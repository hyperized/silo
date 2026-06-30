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
	members  *membership.Membership
	steering bool

	mu   sync.Mutex
	sig  string
	ring *placement.Ring
}

// RouterOption configures a Router.
type RouterOption func(*Router)

// WithPressureSteering enables pressure-aware placement: a chunk's replica set
// prefers nodes that are not signalling DiskPressure, so a near-full node stops
// receiving new chunks (the kubelet "NoSchedule" analog). It is bounded so a
// quorum of the chunk's natural ring replicas is always retained — see steer —
// which keeps reads correct and lets the scrubber heal across pressure changes.
// Disabled returns the plain capacity-weighted ring.
func WithPressureSteering(on bool) RouterOption {
	return func(r *Router) { r.steering = on }
}

// NewRouter builds a Router over a membership table. The first Replicas
// call materialises the ring.
func NewRouter(members *membership.Membership, opts ...RouterOption) *Router {
	r := &Router{members: members}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Replicas resolves chunkID to up to n replica node ids over the current ring,
// rebuilding it first if membership changed since the last call. With pressure
// steering on, nodes signalling DiskPressure are demoted out of the set when a
// quorum of natural replicas can still be retained. The same resolution is used
// by reads, writes, deletes, and the scrubber, so all paths agree on where a
// chunk lives — the steered set is symmetric, not a write-only override.
func (r *Router) Replicas(chunkID string, n int) []string {
	ring := r.currentRing()
	if !r.steering {
		return ring.Replicas(chunkID, n)
	}
	pressured := r.pressuredSet()
	if len(pressured) == 0 {
		return ring.Replicas(chunkID, n) // fast path: a healthy cluster is unchanged
	}
	return steer(ring.Replicas(chunkID, ring.Len()), n, pressured)
}

// pressuredSet is the set of cluster members currently signalling DiskPressure.
func (r *Router) pressuredSet() map[string]struct{} {
	set := make(map[string]struct{})
	for _, n := range r.members.Members() {
		if n.Pressured {
			set[n.ID] = struct{}{}
		}
	}
	return set
}

// steer applies pressure-aware placement to a chunk's ring order. ordered is the
// full ring priority order for the key; its first n entries are the natural
// (unsteered) replica set. steer returns n nodes that prefer non-pressured ones
// but always retain a quorum (n/2+1) of the natural replicas.
//
// That quorum is the safety invariant: any two steered sets for the same key —
// computed under different pressure views or at different times — each share a
// quorum with the fixed natural set, so they overlap in at least one node. Since
// a chunk is written to (and healed across) the full steered set, a read whose
// steered set differs still shares a holder with it and finds the chunk. It also
// bounds how far a pressured node is shed: at most n-quorum of a chunk's
// replicas move, so a near-full node is relieved without ever stranding data.
func steer(ordered []string, n int, pressured map[string]struct{}) []string {
	if n > len(ordered) {
		n = len(ordered)
	}
	if n <= 0 {
		return nil
	}
	quorum := n/2 + 1
	natural := ordered[:n]

	result := make([]string, 0, n)
	seen := make(map[string]struct{}, n)
	add := func(id string) bool {
		if _, dup := seen[id]; dup {
			return false
		}
		seen[id] = struct{}{}
		result = append(result, id)
		return true
	}
	isPressured := func(id string) bool { _, ok := pressured[id]; return ok }

	// 1. Non-pressured natural replicas: ideal — natural ring position, healthy.
	naturalKept := 0
	for _, id := range natural {
		if !isPressured(id) && add(id) {
			naturalKept++
		}
	}
	// 2. Backfill pressured naturals until a quorum of naturals is retained, so
	//    the steered set keeps a quorum overlap with the unsteered set.
	for _, id := range natural {
		if naturalKept >= quorum {
			break
		}
		if isPressured(id) && add(id) {
			naturalKept++
		}
	}
	// 3. Fill remaining slots from spillover (beyond the natural set), preferring
	//    non-pressured nodes — this is where a shed chunk lands.
	for _, id := range ordered[n:] {
		if len(result) >= n {
			break
		}
		if !isPressured(id) {
			add(id)
		}
	}
	// 4. Last resort (cluster mostly pressured): take whatever remains in ring
	//    order so the set is never smaller than n when n nodes exist.
	for _, id := range ordered {
		if len(result) >= n {
			break
		}
		add(id)
	}
	return result
}

// MetaReplicas resolves volumeID to up to n replica node ids for its extent
// map, always over the plain capacity-weighted ring — pressure steering is
// deliberately not applied. A volume's extent map is a small, long-lived
// metadata object whose replica set must stay stable: shedding it off a
// near-full node (as steering does for bulk chunks) would migrate the whole
// map for negligible space relief and churn the set on every pressure change.
// The same un-steered resolution is used by the extent-map write, serve, and
// repair paths so they all agree on where a volume's map lives.
func (r *Router) MetaReplicas(volumeID string, n int) []string {
	return r.currentRing().Replicas(volumeID, n)
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
