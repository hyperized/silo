// Package clusterproto defines silo's cluster wire-protocol version and the
// compatibility policy nodes use to negotiate during a rolling upgrade.
//
// This version is deliberately separate from the build's semver (the
// -ldflags-injected string shown by `siloctl version`). Two builds with
// different semver but the same Protocol speak the same wire format and
// interoperate freely; that is what makes a rolling upgrade safe. Bump
// Protocol only on an incompatible change to the gossip or replication wire
// formats, and raise MinCompatible only once every node in a supported upgrade
// window already speaks the newer version.
package clusterproto

// Protocol is the cluster wire-protocol version this build speaks. Nodes
// advertise it on every gossip message so peers can classify each other.
const Protocol uint32 = 1

// MinCompatible is the oldest Protocol this build will still interoperate with.
// It equals Protocol until we ship a v2 that stays backward-compatible with v1,
// at which point MinCompatible stays at 1 while Protocol becomes 2 — letting a
// v1 and v2 node share a cluster through the upgrade. Raising MinCompatible is
// the deliberate act of dropping support for an old protocol, so it changes
// only when the operator's supported upgrade path guarantees no older node
// remains.
const MinCompatible uint32 = 1

// Compatibility classifies a peer's advertised protocol version relative to
// this node.
type Compatibility int

const (
	// Compatible means the peer's protocol is within [MinCompatible, Protocol]
	// and the two nodes can interoperate.
	Compatible Compatibility = iota
	// PeerNewer means the peer advertises a protocol ahead of ours. We still
	// process its messages on a best-effort basis (the wire format is designed
	// to tolerate unknown fields), but this node should be upgraded.
	PeerNewer
	// PeerTooOld means the peer's protocol is below MinCompatible. We cannot
	// safely interpret its state, so the caller fences it rather than risk
	// merging a misread view.
	PeerTooOld
)

// String renders the classification for logs and tests.
func (c Compatibility) String() string {
	switch c {
	case Compatible:
		return "compatible"
	case PeerNewer:
		return "peer-newer"
	case PeerTooOld:
		return "peer-too-old"
	default:
		return "unknown"
	}
}

// Classify compares a peer's advertised protocol version against this build's
// support window. A peer that advertises 0 predates explicit versioning; that
// era was protocol v1, so it is treated as v1 rather than as "unknown" — which
// keeps the first cluster to adopt versioning from fencing its own not-yet-
// upgraded nodes. Once MinCompatible rises above 1, that same unversioned peer
// correctly becomes PeerTooOld.
func Classify(peer uint32) Compatibility {
	return classify(peer, MinCompatible, Protocol)
}

// classify is the version-window logic with the floor and ceiling injected, so
// the PeerTooOld branch is testable while MinCompatible is still 1 (the only
// value below it, 0, is rehomed to v1, so no live input reaches that branch at
// launch — but it must be correct for the first MinCompatible bump).
func classify(peer, minCompatible, current uint32) Compatibility {
	if peer == 0 {
		peer = 1
	}
	switch {
	case peer < minCompatible:
		return PeerTooOld
	case peer > current:
		return PeerNewer
	default:
		return Compatible
	}
}
