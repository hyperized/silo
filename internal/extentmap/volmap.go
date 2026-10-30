package extentmap

import (
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

// Unmapped is the chunk id that records an extent as unmapped. A trim writes it
// through the same last-writer-wins path as any binding, so the unmap carries a
// fresh HLC stamp and beats every older write on every replica. Deleting the
// key instead would lose that race: a replica that still holds the old binding
// would merge it straight back, and the trimmed data would reappear.
//
// The entry stays in the map for good. It costs as much as the binding it
// replaced and bounds the map at one entry per extent, as before trim existed.
const Unmapped = ""

// volMap is one volume's last-writer-wins map plus a running count of its
// mapped extents. Every mutation goes through set, so the count cannot drift
// from the entries, and Len stays O(1) on volumes with millions of extents
// instead of walking the map under the store lock on every status request.
// The Store's mutex guards it.
type volMap struct {
	lww    *crdt.LWWMap[uint64, string]
	mapped int
}

func newVolMap() *volMap {
	return &volMap{lww: crdt.NewLWWMap[uint64, string]()}
}

// set applies one LWW update and adjusts the mapped count by what actually won,
// which may be the binding that was already there.
func (v *volMap) set(index uint64, chunkID string, ts hlc.Timestamp) {
	prev, had := v.lww.Get(index)
	v.lww.Set(index, chunkID, ts)
	cur, _ := v.lww.Get(index)
	if had && prev != Unmapped {
		v.mapped--
	}
	if cur != Unmapped {
		v.mapped++
	}
}

// importEntries folds entries in with the same per-key LWW rule as set.
func (v *volMap) importEntries(entries []crdt.MapEntry[uint64, string]) {
	for _, e := range entries {
		v.set(e.Key, e.Value, e.TS)
	}
}

// get returns the chunk backing index, reporting an unmap marker as unmapped.
func (v *volMap) get(index uint64) (string, bool) {
	id, ok := v.lww.Get(index)
	if !ok || id == Unmapped {
		return "", false
	}
	return id, true
}
