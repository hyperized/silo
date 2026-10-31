package clusterstatus

import (
	"math"
	"path"
	"time"

	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/replication"
)

// SkewReporter is the slice of the clock-skew monitor the builder reads.
// *clockskew.Monitor satisfies it.
type SkewReporter interface {
	PeerSkews() map[string]time.Duration
	Alerts() uint64
}

// GossipHealth is the slice of the gossip subsystem the builder reads.
// *gossip.Subsystem satisfies it.
type GossipHealth interface {
	Isolated() bool
	RejoinAttempts() uint64
}

// GCReporter is the slice of the chunk GC the builder reads.
// *replication.ChunkGC satisfies it.
type GCReporter interface {
	Status() replication.ChunkGCStatus
}

// MapDamage reports whether this node's copy of a volume's extent map was
// found damaged and not yet rebuilt. *extentmap.Store satisfies it.
type MapDamage interface {
	Lost(volumeID string) bool
}

// ReplicaResolver resolves a volume's extent-map replica set. *replication.Router
// satisfies it.
type ReplicaResolver interface {
	MetaReplicas(volumeID string, n int) []string
}

// AttachedLister lists the NBD exports clients have open on this node.
// *nbd.Server satisfies it.
type AttachedLister interface {
	AttachedExports() []string
}

// WithClockSkew adds per-peer and worst-case clock skew and the skew alert
// count. Without it the skew fields stay zero.
func WithClockSkew(s SkewReporter) Option { return func(b *Builder) { b.skew = s } }

// WithGossipHealth adds the isolation flag and rejoin attempts.
func WithGossipHealth(g GossipHealth) Option { return func(b *Builder) { b.gossip = g } }

// WithGC adds the chunk GC's state. Without it the gc message is all zero,
// which reads as a disabled GC that never ran.
func WithGC(r GCReporter) Option { return func(b *Builder) { b.gc = r } }

// WithMapDamage flags volumes whose local extent map is awaiting a rebuild.
func WithMapDamage(d MapDamage) Option { return func(b *Builder) { b.damage = d } }

// WithReplicaSet lists each volume's extent-map replica set, resolved for the
// replication factor given by WithReplicationFactor.
func WithReplicaSet(r ReplicaResolver) Option { return func(b *Builder) { b.replicas = r } }

// WithAttached flags volumes an NBD client has open on this node.
func WithAttached(a AttachedLister) Option { return func(b *Builder) { b.attached = a } }

// WithReplicationFactor reports the configured SILO_REPLICATION. It also sizes
// the replica sets WithReplicaSet resolves; at zero no replica set is listed.
func WithReplicationFactor(n int) Option { return func(b *Builder) { b.replication = n } }

// WithStartTime reports when the process started.
func WithStartTime(t time.Time) Option { return func(b *Builder) { b.started = t } }

// peerSkews returns the per-peer readings, or nil without a skew source.
func (b *Builder) peerSkews() map[string]time.Duration {
	if b.skew == nil {
		return nil
	}
	return b.skew.PeerSkews()
}

// health assembles the responding node's own signals. members and skews are
// the same reads the node table used, so the two agree within one snapshot.
func (b *Builder) health(members []membership.Node, skews map[string]time.Duration) *statusv1.ClusterHealth {
	h := &statusv1.ClusterHealth{
		ReplicationFactor: toInt32(b.replication),
		AliveNodes:        toInt32(aliveCount(members)),
		ClockSkewMs:       worstSkew(skews).Milliseconds(),
	}
	if b.skew != nil {
		h.ClockSkewAlerts = b.skew.Alerts()
	}
	if b.gossip != nil {
		h.Isolated = b.gossip.Isolated()
		h.RejoinAttempts = b.gossip.RejoinAttempts()
	}
	if !b.started.IsZero() {
		h.StartedUnix = b.started.Unix()
	}
	return h
}

// toInt32 narrows a count for the wire, clamping instead of wrapping.
func toInt32(n int) int32 {
	switch {
	case n < 0:
		return 0
	case n > math.MaxInt32:
		return math.MaxInt32
	default:
		return int32(n)
	}
}

func aliveCount(members []membership.Node) int {
	n := 0
	for _, m := range members {
		if m.State == membership.StateAlive {
			n++
		}
	}
	return n
}

// worstSkew is the largest absolute reading. A peer far behind is as much a
// time-sync fault as one far ahead, so the sign is dropped here; the node
// table keeps it per peer.
func worstSkew(skews map[string]time.Duration) time.Duration {
	var worst time.Duration
	for _, d := range skews {
		worst = max(worst, d.Abs())
	}
	return worst
}

// gcStatus always returns a message so the UI reads enabled=false rather than
// a missing object.
func gcStatus(r GCReporter) *statusv1.GCStatus {
	if r == nil {
		return &statusv1.GCStatus{}
	}
	st := r.Status()
	out := &statusv1.GCStatus{
		Enabled:            st.Enabled,
		Running:            st.Running,
		LastDurationMs:     st.LastDuration.Milliseconds(),
		LastOrphansDeleted: st.LastDeleted,
		LastBytesFreed:     st.LastBytesFreed,
		LastError:          st.LastError,
		ConsecutiveSkips:   st.ConsecutiveSkips,
	}
	// time.Time{}.Unix() is a large negative number; the contract is 0.
	if !st.LastRun.IsZero() {
		out.LastRunUnix = st.LastRun.Unix()
	}
	return out
}

// attachedSet canonicalises the open export names to namespace paths. Clients
// may name an export "vol", "/vol" or "/a//vol"; the namespace resolves all of
// them to the volume listed as "/vol" or "/a/vol".
func (b *Builder) attachedSet() map[string]struct{} {
	if b.attached == nil {
		return nil
	}
	exports := b.attached.AttachedExports()
	out := make(map[string]struct{}, len(exports))
	for _, e := range exports {
		out[path.Clean("/"+e)] = struct{}{}
	}
	return out
}

// replicaSet resolves the volume's extent-map replica set, or nil when no
// resolver or replication factor is configured.
func (b *Builder) replicaSet(inode string) []string {
	if b.replicas == nil || b.replication <= 0 {
		return nil
	}
	return b.replicas.MetaReplicas(inode, b.replication)
}

func (b *Builder) mapDamaged(inode string) bool {
	return b.damage != nil && b.damage.Lost(inode)
}
