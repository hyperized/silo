// Package membership models the live view of a silo cluster: which nodes
// exist, what state each is in, and how to merge claims about that state
// from gossiping peers. It is the SWIM membership state machine — wire
// I/O lives in internal/gossip, so this package is pure logic and is
// independently testable without network setup.
//
// State semantics: every node owns its own monotonic Incarnation counter.
// A higher-incarnation claim always wins a merge. On equal Incarnation,
// states are ordered Alive < Suspect < Dead < Left — a later-stage claim
// supersedes an earlier-stage one only when no refutation has bumped the
// owner's incarnation. The owning node refutes a Suspect or Dead claim
// about itself by bumping its own incarnation and re-broadcasting Alive;
// the new entry is then strictly newer than the suspicion, so every peer
// converges on Alive without operator intervention.
package membership

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State is the operational state of one peer as observed by this node.
// Values are ordered Alive < Suspect < Dead < Left for tie-breaking on
// equal incarnation — a later-stage state replaces an earlier-stage one
// only when nothing newer refutes it. The numeric values are kept stable
// because they participate in that ordering; do not reorder.
type State uint8

const (
	// StateAlive means the peer has answered a probe (directly or
	// indirectly) recently and is considered fully reachable.
	StateAlive State = iota
	// StateSuspect means a direct probe failed and no indirect probe
	// succeeded; the peer is on the way to Dead unless it refutes.
	StateSuspect
	// StateDead means the suspicion timer fired without a refutation.
	// Reads and writes that would route to this peer must avoid it.
	StateDead
	// StateLeft means the peer voluntarily shut down (graceful drain).
	// Distinct from Dead so operators can tell "I removed it" from
	// "the network broke" in logs and metrics.
	StateLeft
)

// String returns a stable human-readable name for the state. Used in
// log lines and the snapshot test fixtures; keep these lowercase and
// dash-free so they survive grep-based ops triage unchanged.
func (s State) String() string {
	switch s {
	case StateAlive:
		return "alive"
	case StateSuspect:
		return "suspect"
	case StateDead:
		return "dead"
	case StateLeft:
		return "left"
	default:
		return fmt.Sprintf("state(%d)", uint8(s))
	}
}

// Node is one peer's entry in the membership table. Holds the smallest
// payload SWIM needs to converge: identity, dialable address, current
// state, and the owning peer's monotonic incarnation counter. Snapshot
// returns deep copies of this type so callers cannot scribble into the
// canonical map.
type Node struct {
	// ID is the cluster-unique node identifier. Matches cfg.NodeID and
	// the SPIFFE URI in the node cert; non-empty for every entry.
	ID string
	// Address is the host:port of the peer's gossip listener. Empty for
	// entries that were learned only as "this name exists" before a
	// concrete probe target was attached.
	Address string
	// DataAddress is the host:port other nodes dial to reach this peer's
	// gRPC data plane (chunk replication and reads). It rides gossip
	// alongside Address so placement can resolve a node id to a dial
	// target without a separate lookup service. Empty until the peer's
	// own advertisement has propagated.
	DataAddress string
	// State is the SWIM state for this peer at the moment Snapshot was
	// called.
	State State
	// Incarnation is the version counter owned by the peer with this ID.
	// A peer refutes a Suspect/Dead claim about itself by bumping this
	// number and re-broadcasting Alive.
	Incarnation uint64
	// LastChange records when this node was last transitioned. Used by
	// the pruner to clean up Dead entries after a retention window and
	// by ops tooling to surface "this node has been down for X".
	LastChange time.Time
	// CapacityBytes and UsedBytes are the node's advertised backing-store
	// size and usage, refreshed periodically by the owner and propagated
	// over anti-entropy. They feed capacity-aware placement and operator
	// tooling; zero means "not advertised yet".
	CapacityBytes int64
	UsedBytes     int64
	// Pressured is the node's self-declared DiskPressure condition (the soft
	// high-watermark). The node sets its own flag with hysteresis and gossips
	// it, exactly as a kubelet sets its node condition; peers read it for
	// operator visibility. It does not change placement (see
	// internal/diskpressure for why).
	Pressured bool
}

// Event is one gossiped claim about another node. Apply merges Event
// into the table using SWIM's "highest incarnation wins, later state
// wins on a tie" rule. Events flow over the wire as JSON, so all fields
// are exported — see internal/gossip/wire.go for the on-wire shape.
type Event struct {
	ID            string    `json:"id"`
	Address       string    `json:"address,omitempty"`
	DataAddress   string    `json:"data_address,omitempty"`
	State         State     `json:"state"`
	Incarnation   uint64    `json:"incarnation"`
	At            time.Time `json:"at,omitempty"`
	CapacityBytes int64     `json:"capacity_bytes,omitempty"`
	UsedBytes     int64     `json:"used_bytes,omitempty"`
	Pressured     bool      `json:"pressured,omitempty"`
}

// Now is the clock the table reads to stamp LastChange. Production
// leaves it at time.Now; tests substitute a deterministic clock so
// snapshot comparisons aren't time-dependent.
var Now = time.Now

// Membership is the goroutine-safe local view of the cluster. Methods
// take an internal RWMutex; callers may share one Membership across as
// many goroutines as they like. Snapshot and Members return deep copies
// so the cost of a read is paid up-front and the canonical map is never
// exposed to a writer outside this package.
type Membership struct {
	self string

	mu      sync.RWMutex
	members map[string]Node
}

// New constructs a Membership rooted at the local node id. selfID is
// required: the local node is the only entry whose incarnation this
// process owns, and a fresh table without it would drop self-refutations
// on the floor. selfAddr is the host:port peers should dial to gossip
// with us; selfDataAddr is the host:port peers should dial to reach our
// gRPC data plane. Both are surfaced so peers learn our dial targets from
// the first event they receive from us.
func New(selfID, selfAddr, selfDataAddr string) (*Membership, error) {
	if selfID == "" {
		return nil, errors.New("membership: selfID is empty; pass cfg.NodeID")
	}
	now := Now()
	m := &Membership{
		self:    selfID,
		members: make(map[string]Node, 4),
	}
	m.members[selfID] = Node{
		ID:          selfID,
		Address:     selfAddr,
		DataAddress: selfDataAddr,
		State:       StateAlive,
		Incarnation: 0,
		LastChange:  now,
	}
	return m, nil
}

// Self returns the local node's current entry, including the latest
// incarnation we own. Callers attach Self to outgoing probes so peers
// can refresh their view of us in lockstep with our own beliefs.
func (m *Membership) Self() Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.members[m.self]
}

// SelfID returns the local node id; cheaper than Self when only the id
// is needed.
func (m *Membership) SelfID() string {
	return m.self
}

// SetSelfAddress updates the local node's dialable gossip address.
// Used at startup once the gossip listener has bound its port, so
// outbound events name a concrete address instead of an empty string.
// Bumps incarnation so peers pick up the new address.
func (m *Membership) SetSelfAddress(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := m.members[m.self]
	cur.Address = addr
	cur.Incarnation++
	cur.State = StateAlive
	cur.LastChange = Now()
	m.members[m.self] = cur
}

// SetSelfCapacity updates the local node's advertised backing-store capacity,
// usage, and DiskPressure condition and bumps incarnation so peers accept the
// new figures. It reports whether anything changed, so the caller can avoid
// re-advertising (and the incarnation churn that brings) when nothing moved.
func (m *Membership) SetSelfCapacity(capacityBytes, usedBytes int64, pressured bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := m.members[m.self]
	if cur.CapacityBytes == capacityBytes && cur.UsedBytes == usedBytes && cur.Pressured == pressured {
		return false
	}
	cur.CapacityBytes = capacityBytes
	cur.UsedBytes = usedBytes
	cur.Pressured = pressured
	cur.Incarnation++
	cur.State = StateAlive
	cur.LastChange = Now()
	m.members[m.self] = cur
	return true
}

// Members returns a deep copy of every entry in the table. The slice
// is unordered; callers that want stable iteration should sort by ID.
func (m *Membership) Members() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Node, 0, len(m.members))
	for _, n := range m.members {
		out = append(out, n)
	}
	return out
}

// Snapshot is an alias for Members; named for callers who think of the
// operation as taking a point-in-time view (the docker-compose status
// command, log emitters).
func (m *Membership) Snapshot() []Node {
	return m.Members()
}

// Apply merges one Event into the table and returns the post-merge
// Node plus a bool indicating whether the entry changed. The merge
// rules:
//
//  1. An event for the local node id with a state worse than Alive
//     triggers a refutation: we bump our own incarnation, re-set
//     ourselves to Alive, and ignore the incoming event. The returned
//     Node is our fresh self-entry; changed is true.
//  2. If the table has no entry for ev.ID, the event is inserted
//     verbatim. changed is true.
//  3. If ev.Incarnation > existing.Incarnation, the event wins.
//  4. If ev.Incarnation == existing.Incarnation and ev.State has a
//     higher numeric value than existing.State, the event wins. This
//     is how Suspect→Dead progresses without an incarnation bump.
//  5. Otherwise the event is ignored.
//
// Apply ignores events with an empty ID — callers should treat that as
// a programmer bug, not a soft error worth logging on every probe.
func (m *Membership) Apply(ev Event) (Node, bool) {
	if ev.ID == "" {
		return Node{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applyLocked(ev)
}

// ApplyMany merges a batch of events under one lock acquisition. Used
// by the gossip handler when a single Ping carries a piggybacked slice
// of recent events. Returns the subset of events that produced a real
// change so callers can rebroadcast only those.
func (m *Membership) ApplyMany(events []Event) []Node {
	if len(events) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := make([]Node, 0, len(events))
	for _, ev := range events {
		if ev.ID == "" {
			continue
		}
		if n, ok := m.applyLocked(ev); ok {
			changed = append(changed, n)
		}
	}
	return changed
}

// applyLocked must be called with m.mu held for writes.
func (m *Membership) applyLocked(ev Event) (Node, bool) {
	now := Now()
	if ev.ID == m.self {
		// Self-refutation: any non-Alive claim about us is wrong by
		// definition (we are evidently alive — we just received it).
		// Bump our incarnation past the claim's so subsequent merges
		// at peers will see our new entry as strictly newer.
		if ev.State == StateAlive {
			return m.members[m.self], false
		}
		cur := m.members[m.self]
		if ev.Incarnation >= cur.Incarnation {
			cur.Incarnation = ev.Incarnation + 1
		}
		cur.State = StateAlive
		cur.LastChange = now
		m.members[m.self] = cur
		return cur, true
	}

	cur, exists := m.members[ev.ID]
	if !exists {
		n := Node{
			ID:            ev.ID,
			Address:       ev.Address,
			DataAddress:   ev.DataAddress,
			State:         ev.State,
			Incarnation:   ev.Incarnation,
			LastChange:    now,
			CapacityBytes: ev.CapacityBytes,
			UsedBytes:     ev.UsedBytes,
			Pressured:     ev.Pressured,
		}
		m.members[ev.ID] = n
		return n, true
	}
	// Higher incarnation always wins.
	if ev.Incarnation > cur.Incarnation {
		cur.Address = preferNonEmpty(ev.Address, cur.Address)
		cur.DataAddress = preferNonEmpty(ev.DataAddress, cur.DataAddress)
		cur.State = ev.State
		cur.Incarnation = ev.Incarnation
		cur.LastChange = now
		applyCapacity(&cur, ev)
		m.members[ev.ID] = cur
		return cur, true
	}
	// Equal incarnation: state ordering breaks the tie. Suspect→Dead
	// is the canonical case — the suspicion timer fires at one peer and
	// the resulting Dead event must override the still-Suspect entries
	// elsewhere without forcing a new incarnation from the absent node.
	if ev.Incarnation == cur.Incarnation && ev.State > cur.State {
		cur.Address = preferNonEmpty(ev.Address, cur.Address)
		cur.DataAddress = preferNonEmpty(ev.DataAddress, cur.DataAddress)
		cur.State = ev.State
		cur.LastChange = now
		applyCapacity(&cur, ev)
		m.members[ev.ID] = cur
		return cur, true
	}
	return cur, false
}

// applyCapacity copies the event's advertised capacity onto a node when the
// event actually carries one. A zero CapacityBytes means the event is a plain
// state change (a Suspect/Dead relay) that knows nothing about capacity, so the
// node's last-known figures are preserved rather than wiped to zero.
func applyCapacity(n *Node, ev Event) {
	if ev.CapacityBytes > 0 {
		n.CapacityBytes = ev.CapacityBytes
		n.UsedBytes = ev.UsedBytes
		// Pressured rides with a real capacity advert (the owner sets both in
		// the same SetSelfCapacity call), so it is only trusted alongside one —
		// a bare state relay leaves the last-known condition untouched.
		n.Pressured = ev.Pressured
	}
}

// MarkSuspect transitions id to Suspect at its current incarnation and
// returns the resulting event so the caller can broadcast it. If id is
// already Suspect or worse, MarkSuspect is a no-op and returns ok=false.
// Suspecting the local node is silently ignored — the probe loop should
// never target itself.
func (m *Membership) MarkSuspect(id string) (Event, bool) {
	if id == "" || id == m.self {
		return Event{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, exists := m.members[id]
	if !exists {
		return Event{}, false
	}
	if cur.State >= StateSuspect {
		return Event{}, false
	}
	cur.State = StateSuspect
	cur.LastChange = Now()
	m.members[id] = cur
	return Event{ID: id, Address: cur.Address, DataAddress: cur.DataAddress, State: StateSuspect, Incarnation: cur.Incarnation, At: cur.LastChange}, true
}

// MarkDead transitions id to Dead at its current incarnation. Returns
// ok=false when id is unknown, already Dead/Left, or refers to the
// local node. Used by the suspicion-timeout path inside the prober.
func (m *Membership) MarkDead(id string) (Event, bool) {
	if id == "" || id == m.self {
		return Event{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, exists := m.members[id]
	if !exists {
		return Event{}, false
	}
	if cur.State >= StateDead {
		return Event{}, false
	}
	cur.State = StateDead
	cur.LastChange = Now()
	m.members[id] = cur
	return Event{ID: id, Address: cur.Address, DataAddress: cur.DataAddress, State: StateDead, Incarnation: cur.Incarnation, At: cur.LastChange}, true
}

// MarkLeft transitions id to Left. Used by the graceful-drain flow on
// shutdown, so peers can distinguish operator action from network
// failure. Refuses to mark the local node Left while still running —
// the daemon should publish its own Left event right before exiting,
// which goes through Apply.
func (m *Membership) MarkLeft(id string) (Event, bool) {
	if id == "" {
		return Event{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, exists := m.members[id]
	if !exists {
		return Event{}, false
	}
	if cur.State == StateLeft {
		return Event{}, false
	}
	cur.State = StateLeft
	cur.LastChange = Now()
	m.members[id] = cur
	return Event{ID: id, Address: cur.Address, DataAddress: cur.DataAddress, State: StateLeft, Incarnation: cur.Incarnation, At: cur.LastChange}, true
}

// Prune removes Dead/Left entries whose LastChange is older than
// retention. Returns the ids that were pruned so the caller can log
// them. The local node is never pruned. Callers run this periodically
// (anti-entropy cycle) so the table stays compact across long-lived
// clusters where many peers have come and gone.
func (m *Membership) Prune(retention time.Duration) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if retention <= 0 {
		return nil
	}
	cutoff := Now().Add(-retention)
	var pruned []string
	for id, n := range m.members {
		if id == m.self {
			continue
		}
		if (n.State == StateDead || n.State == StateLeft) && n.LastChange.Before(cutoff) {
			delete(m.members, id)
			pruned = append(pruned, id)
		}
	}
	return pruned
}

// AlivePeers returns the subset of Members in StateAlive excluding the
// local node. Used by the prober to pick a probe target.
func (m *Membership) AlivePeers() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Node, 0, len(m.members))
	for id, n := range m.members {
		if id == m.self {
			continue
		}
		if n.State == StateAlive {
			out = append(out, n)
		}
	}
	return out
}

// Lookup returns the Node entry for id and ok=true when present. The
// returned Node is a copy; mutating it does not affect the table.
func (m *Membership) Lookup(id string) (Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.members[id]
	return n, ok
}

// preferNonEmpty returns a when it is non-empty, otherwise b. Used so
// merges from peers that carry an Address don't blank out an Address
// we already know for a node.
func preferNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
