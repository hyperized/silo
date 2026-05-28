package replication

import (
	"sort"
	"strings"
	"sync"

	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/placement"
)

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

// currentRing returns the ring for the present set of live node ids,
// rebuilding it only when that set has changed.
func (r *Router) currentRing() *placement.Ring {
	ids := r.aliveIDs()
	sig := strings.Join(ids, ",")

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ring == nil || sig != r.sig {
		r.ring = placement.New(ids, placement.DefaultVNodes)
		r.sig = sig
	}
	return r.ring
}

// aliveIDs is the local node plus every Alive peer, sorted. The local node
// is always included: it is Alive from its own perspective and is a valid
// replica target. Sorting makes the signature stable regardless of map
// iteration order.
func (r *Router) aliveIDs() []string {
	peers := r.members.AlivePeers()
	ids := make([]string, 0, len(peers)+1)
	ids = append(ids, r.members.SelfID())
	for _, p := range peers {
		ids = append(ids, p.ID)
	}
	sort.Strings(ids)
	return ids
}
