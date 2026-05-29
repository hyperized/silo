// Package crdt provides the conflict-free replicated data types the silo
// namespace is built from: a last-writer-wins register and an
// observed-remove set, both tagged with hybrid logical clocks so merges
// are deterministic across nodes.
//
// These types are plain data structures and are NOT internally
// synchronized — the namespace layer that composes them serializes access
// behind its own lock. Keeping them lock-free lets the namespace merge a
// whole delta atomically rather than field by field.
package crdt

import "github.com/hyperized/silo/internal/hlc"

// LWWRegister is a last-writer-wins register: a value plus the HLC at which
// it was set. Merge keeps the value with the greater timestamp; because HLC
// timestamps are totally ordered (the node id breaks ties), every node
// converges on the same winner regardless of merge order.
type LWWRegister[T any] struct {
	Value T
	TS    hlc.Timestamp
}

// Set returns a register holding value as of ts.
func Set[T any](value T, ts hlc.Timestamp) LWWRegister[T] {
	return LWWRegister[T]{Value: value, TS: ts}
}

// Merge returns whichever register was written later. A zero-timestamp
// register (never set) loses to any real write.
func (r LWWRegister[T]) Merge(o LWWRegister[T]) LWWRegister[T] {
	if o.TS.After(r.TS) {
		return o
	}
	return r
}

// LWWMap is a last-writer-wins map: each key holds a value tagged with the
// HLC at which it was set, and Merge keeps the higher-HLC value per key.
// Because HLC timestamps are totally ordered, every replica converges on the
// same value for each key regardless of merge order. There is no delete — a
// volume's extent map only ever rebinds an extent to a newer chunk — so an
// erased key would have to be modelled as a sentinel value if ever needed.
type LWWMap[K comparable, V any] struct {
	entries map[K]LWWRegister[V]
}

// NewLWWMap returns an empty last-writer-wins map.
func NewLWWMap[K comparable, V any]() *LWWMap[K, V] {
	return &LWWMap[K, V]{entries: map[K]LWWRegister[V]{}}
}

// Set binds key to value as of ts, keeping it only when ts is newer than the
// key's current timestamp — so a replayed or out-of-order write never moves a
// key backward.
func (m *LWWMap[K, V]) Set(key K, value V, ts hlc.Timestamp) {
	if cur, ok := m.entries[key]; !ok || ts.After(cur.TS) {
		m.entries[key] = LWWRegister[V]{Value: value, TS: ts}
	}
}

// Get returns the current value for key and whether it is set.
func (m *LWWMap[K, V]) Get(key K) (V, bool) {
	r, ok := m.entries[key]
	return r.Value, ok
}

// Len returns the number of bound keys.
func (m *LWWMap[K, V]) Len() int { return len(m.entries) }

// MapEntry is the serializable shape of one key's winning value and timestamp.
type MapEntry[K comparable, V any] struct {
	Key   K             `json:"key"`
	Value V             `json:"value"`
	TS    hlc.Timestamp `json:"ts"`
}

// Entries returns every key's value and timestamp in unspecified order; it is
// also the map's serializable form (feed it back through Import).
func (m *LWWMap[K, V]) Entries() []MapEntry[K, V] {
	out := make([]MapEntry[K, V], 0, len(m.entries))
	for k, r := range m.entries {
		out = append(out, MapEntry[K, V]{Key: k, Value: r.Value, TS: r.TS})
	}
	return out
}

// Merge folds o into m, keeping the higher-HLC value per key. It is
// commutative, associative, and idempotent.
func (m *LWWMap[K, V]) Merge(o *LWWMap[K, V]) {
	for k, r := range o.entries {
		m.Set(k, r.Value, r.TS)
	}
}

// Import folds serialized entries into the map with the same per-key LWW
// semantics as Merge, so reconstructing a peer's map and importing it
// converges exactly as a direct Merge would.
func (m *LWWMap[K, V]) Import(entries []MapEntry[K, V]) {
	for _, e := range entries {
		m.Set(e.Key, e.Value, e.TS)
	}
}

// Clone returns a deep copy that shares no maps with m.
func (m *LWWMap[K, V]) Clone() *LWWMap[K, V] {
	c := NewLWWMap[K, V]()
	c.Merge(m)
	return c
}

// ORSet is an observed-remove set. Each Add tags an element with a
// globally-unique HLC; Remove tombstones exactly the tags observed at the
// moment of removal. An element is present while it holds at least one add
// tag that has not been tombstoned, so an Add made concurrently with a
// Remove — carrying a tag the remover never saw — survives ("add wins").
// Merge is the union of add and remove tags, making it commutative,
// associative, and idempotent.
type ORSet[T comparable] struct {
	adds map[T]map[hlc.Timestamp]struct{}
	// removes maps each tombstoned add tag to the time the removal happened,
	// so GC can reclaim a tombstone only once it has had time to propagate.
	removes map[T]map[hlc.Timestamp]hlc.Timestamp
}

// NewORSet returns an empty observed-remove set.
func NewORSet[T comparable]() *ORSet[T] {
	return &ORSet[T]{
		adds:    map[T]map[hlc.Timestamp]struct{}{},
		removes: map[T]map[hlc.Timestamp]hlc.Timestamp{},
	}
}

// Add records elem under tag, which must be globally unique — an HLC from
// the node's clock. Re-adding after a Remove (with a fresh tag) revives the
// element.
func (s *ORSet[T]) Add(elem T, tag hlc.Timestamp) {
	if s.adds[elem] == nil {
		s.adds[elem] = map[hlc.Timestamp]struct{}{}
	}
	s.adds[elem][tag] = struct{}{}
}

// Remove tombstones every add tag currently observed for elem, stamping the
// removal at. Tags not yet observed here (a concurrent Add elsewhere) are
// untouched and keep elem present after merge. The removal time is the
// later of any existing tombstone and at, so GC never reclaims earlier than
// the most recent removal.
func (s *ORSet[T]) Remove(elem T, at hlc.Timestamp) {
	observed := s.adds[elem]
	if len(observed) == 0 {
		return
	}
	if s.removes[elem] == nil {
		s.removes[elem] = map[hlc.Timestamp]hlc.Timestamp{}
	}
	for tag := range observed {
		if prev, ok := s.removes[elem][tag]; !ok || at.After(prev) {
			s.removes[elem][tag] = at
		}
	}
}

// Contains reports whether elem currently has a live (non-tombstoned) tag.
func (s *ORSet[T]) Contains(elem T) bool {
	for tag := range s.adds[elem] {
		if _, removed := s.removes[elem][tag]; !removed {
			return true
		}
	}
	return false
}

// LiveTag returns the greatest non-tombstoned tag for elem and whether elem
// is present. The greatest tag is the element's effective claim time, which
// lets callers order competing claims deterministically.
func (s *ORSet[T]) LiveTag(elem T) (hlc.Timestamp, bool) {
	var best hlc.Timestamp
	found := false
	for tag := range s.adds[elem] {
		if _, removed := s.removes[elem][tag]; removed {
			continue
		}
		if !found || tag.After(best) {
			best, found = tag, true
		}
	}
	return best, found
}

// Elements returns the present elements in unspecified order; callers that
// need a stable order should sort the result.
func (s *ORSet[T]) Elements() []T {
	out := make([]T, 0, len(s.adds))
	for elem := range s.adds {
		if s.Contains(elem) {
			out = append(out, elem)
		}
	}
	return out
}

// Merge folds o into s: the union of add tags, and of remove tombstones
// keeping the later removal time per tag. It is idempotent and
// order-independent, so applying the same delta twice or in any order
// converges to the same state.
func (s *ORSet[T]) Merge(o *ORSet[T]) {
	for elem, tags := range o.adds {
		if s.adds[elem] == nil {
			s.adds[elem] = map[hlc.Timestamp]struct{}{}
		}
		for tag := range tags {
			s.adds[elem][tag] = struct{}{}
		}
	}
	for elem, tombs := range o.removes {
		s.mergeTombstones(elem, tombs)
	}
}

// mergeTombstones folds a tag→removal-time map into s.removes[elem], keeping
// the later removal time when a tag is tombstoned on both sides.
func (s *ORSet[T]) mergeTombstones(elem T, tombs map[hlc.Timestamp]hlc.Timestamp) {
	if s.removes[elem] == nil {
		s.removes[elem] = map[hlc.Timestamp]hlc.Timestamp{}
	}
	for tag, at := range tombs {
		if prev, ok := s.removes[elem][tag]; !ok || at.After(prev) {
			s.removes[elem][tag] = at
		}
	}
}

// GC reclaims tombstoned add tags whose removal happened at or before
// cutoff, dropping the element entirely once it has no tags left. Callers
// pass a cutoff of (now - retention) so a tombstone survives long enough to
// reach every replica before its memory is reclaimed. Returns the number of
// tags reclaimed.
func (s *ORSet[T]) GC(cutoff hlc.Timestamp) int {
	reclaimed := 0
	for elem, tombs := range s.removes {
		for tag, at := range tombs {
			if at.After(cutoff) {
				continue // too recent; may not have propagated yet
			}
			delete(s.adds[elem], tag)
			delete(tombs, tag)
			reclaimed++
		}
		if len(tombs) == 0 {
			delete(s.removes, elem)
		}
		if len(s.adds[elem]) == 0 {
			delete(s.adds, elem)
		}
	}
	return reclaimed
}

// Clone returns a deep copy that shares no maps with s.
func (s *ORSet[T]) Clone() *ORSet[T] {
	c := NewORSet[T]()
	c.Merge(s)
	return c
}

// ElementTags pairs an element with its add-tag set — the flat,
// serializable shape of the adds half of the set.
type ElementTags[T comparable] struct {
	Elem T               `json:"elem"`
	Tags []hlc.Timestamp `json:"tags"`
}

// Tombstone is a removed add tag and the time the removal happened.
type Tombstone struct {
	Add hlc.Timestamp `json:"add"`
	At  hlc.Timestamp `json:"at"`
}

// ElementTombstones pairs an element with its tombstones — the serializable
// shape of the removes half, carrying removal times so GC stays consistent
// across replicas.
type ElementTombstones[T comparable] struct {
	Elem       T           `json:"elem"`
	Tombstones []Tombstone `json:"tombstones"`
}

// Export returns the set's adds and tombstones as flat slices suitable for
// serialization. Order is unspecified.
func (s *ORSet[T]) Export() (adds []ElementTags[T], removes []ElementTombstones[T]) {
	adds = make([]ElementTags[T], 0, len(s.adds))
	for elem, tags := range s.adds {
		et := ElementTags[T]{Elem: elem, Tags: make([]hlc.Timestamp, 0, len(tags))}
		for tag := range tags {
			et.Tags = append(et.Tags, tag)
		}
		adds = append(adds, et)
	}
	removes = make([]ElementTombstones[T], 0, len(s.removes))
	for elem, tombs := range s.removes {
		et := ElementTombstones[T]{Elem: elem, Tombstones: make([]Tombstone, 0, len(tombs))}
		for tag, at := range tombs {
			et.Tombstones = append(et.Tombstones, Tombstone{Add: tag, At: at})
		}
		removes = append(removes, et)
	}
	return adds, removes
}

// Import folds serialized adds and tombstones into the set with the same
// union semantics as Merge, so reconstructing a peer's set and importing it
// converges exactly as a direct Merge would.
func (s *ORSet[T]) Import(adds []ElementTags[T], removes []ElementTombstones[T]) {
	for _, et := range adds {
		if s.adds[et.Elem] == nil {
			s.adds[et.Elem] = map[hlc.Timestamp]struct{}{}
		}
		for _, tag := range et.Tags {
			s.adds[et.Elem][tag] = struct{}{}
		}
	}
	for _, et := range removes {
		tombs := make(map[hlc.Timestamp]hlc.Timestamp, len(et.Tombstones))
		for _, tomb := range et.Tombstones {
			tombs[tomb.Add] = tomb.At
		}
		s.mergeTombstones(et.Elem, tombs)
	}
}
