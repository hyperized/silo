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

// ORSet is an observed-remove set. Each Add tags an element with a
// globally-unique HLC; Remove tombstones exactly the tags observed at the
// moment of removal. An element is present while it holds at least one add
// tag that has not been tombstoned, so an Add made concurrently with a
// Remove — carrying a tag the remover never saw — survives ("add wins").
// Merge is the union of add and remove tags, making it commutative,
// associative, and idempotent.
type ORSet[T comparable] struct {
	adds    map[T]map[hlc.Timestamp]struct{}
	removes map[T]map[hlc.Timestamp]struct{}
}

// NewORSet returns an empty observed-remove set.
func NewORSet[T comparable]() *ORSet[T] {
	return &ORSet[T]{
		adds:    map[T]map[hlc.Timestamp]struct{}{},
		removes: map[T]map[hlc.Timestamp]struct{}{},
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

// Remove tombstones every add tag currently observed for elem. Tags not yet
// observed here (a concurrent Add elsewhere) are untouched and keep elem
// present after merge.
func (s *ORSet[T]) Remove(elem T) {
	observed := s.adds[elem]
	if len(observed) == 0 {
		return
	}
	if s.removes[elem] == nil {
		s.removes[elem] = map[hlc.Timestamp]struct{}{}
	}
	for tag := range observed {
		s.removes[elem][tag] = struct{}{}
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

// Merge folds o into s as the union of all add and remove tags. It is
// idempotent and order-independent, so applying the same delta twice or in
// any order converges to the same state.
func (s *ORSet[T]) Merge(o *ORSet[T]) {
	for elem, tags := range o.adds {
		if s.adds[elem] == nil {
			s.adds[elem] = map[hlc.Timestamp]struct{}{}
		}
		for tag := range tags {
			s.adds[elem][tag] = struct{}{}
		}
	}
	for elem, tags := range o.removes {
		if s.removes[elem] == nil {
			s.removes[elem] = map[hlc.Timestamp]struct{}{}
		}
		for tag := range tags {
			s.removes[elem][tag] = struct{}{}
		}
	}
}

// Clone returns a deep copy that shares no maps with s.
func (s *ORSet[T]) Clone() *ORSet[T] {
	c := NewORSet[T]()
	c.Merge(s)
	return c
}

// ElementTags pairs an element with its tag set. It is the flat,
// serializable shape of the set used to ferry state to a peer.
type ElementTags[T comparable] struct {
	Elem T               `json:"elem"`
	Tags []hlc.Timestamp `json:"tags"`
}

// Export returns the set's adds and removes as flat slices suitable for
// serialization. Order is unspecified.
func (s *ORSet[T]) Export() (adds, removes []ElementTags[T]) {
	return flatten(s.adds), flatten(s.removes)
}

func flatten[T comparable](m map[T]map[hlc.Timestamp]struct{}) []ElementTags[T] {
	out := make([]ElementTags[T], 0, len(m))
	for elem, tags := range m {
		et := ElementTags[T]{Elem: elem, Tags: make([]hlc.Timestamp, 0, len(tags))}
		for tag := range tags {
			et.Tags = append(et.Tags, tag)
		}
		out = append(out, et)
	}
	return out
}

// Import folds serialized adds and removes into the set with the same union
// semantics as Merge, so reconstructing a peer's set and importing it
// converges exactly as a direct Merge would.
func (s *ORSet[T]) Import(adds, removes []ElementTags[T]) {
	absorb(s.adds, adds)
	absorb(s.removes, removes)
}

func absorb[T comparable](m map[T]map[hlc.Timestamp]struct{}, in []ElementTags[T]) {
	for _, et := range in {
		if m[et.Elem] == nil {
			m[et.Elem] = map[hlc.Timestamp]struct{}{}
		}
		for _, tag := range et.Tags {
			m[et.Elem][tag] = struct{}{}
		}
	}
}
