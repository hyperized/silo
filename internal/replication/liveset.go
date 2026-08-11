package replication

// liveSet builds the cluster's live (keep) chunk set from the two reference
// kinds and reports how many live volumes' extent maps this node could not see.
//
// The namespace half is complete everywhere — it is fully replicated. The
// extent-map half is sharded: a node holds only the maps of the volumes it is a
// metadata replica of (or has warmed). A non-zero unaccounted count therefore
// means this node has references it cannot enumerate, and absence from the keep
// set does not prove a chunk is unreferenced.
//
// Both the chunk GC and the scrubber need exactly this answer — the GC to know
// what it may reclaim, the scrubber to know what is still worth healing — so the
// build lives here rather than in either of them. They differ only in what they
// do when the view is incomplete: the GC abstains, the scrubber falls back to
// healing everything.
func liveSet(ns NamespaceRefSource, ext ExtentRefSource) (keep map[string]struct{}, unaccounted int64) {
	// LiveChunkRefs hands back a fresh map, so writing the extent refs into it is
	// safe — it is not the namespace's own state.
	keep, liveVols := ns.LiveChunkRefs()
	held := make(map[string]struct{})
	for _, v := range ext.Volumes() {
		held[v] = struct{}{}
		for _, e := range ext.Snapshot(v) {
			keep[e.Value] = struct{}{}
		}
	}
	for v := range liveVols {
		if _, ok := held[v]; !ok {
			unaccounted++
		}
	}
	return keep, unaccounted
}
