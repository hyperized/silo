package gossip

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperized/silo/internal/metrics"

	"github.com/hyperized/silo/internal/clusterproto"
	"github.com/hyperized/silo/internal/membership"
)

// Defaults are surfaced as constants because they are read by both the
// subsystem constructor and the silod config validator (which exposes
// them in error messages). Picked to match well-cited SWIM defaults
// while keeping convergence under five seconds in the three-node
// docker-compose setup.
const (
	// DefaultProbeInterval is how often the prober probes one Alive peer.
	DefaultProbeInterval = time.Second
	// DefaultProbeTimeout is how long a direct probe waits for an Ack
	// before kicking the indirect-probe fan-out.
	DefaultProbeTimeout = 500 * time.Millisecond
	// DefaultIndirectK is how many peers we ask to probe a target on our
	// behalf when our direct probe fails. SWIM picks k≈3 because that
	// is enough to mask a transient direct-path failure without enlarging
	// every probe round into a flood.
	DefaultIndirectK = 3
	// DefaultSuspectTimeout is how long a Suspect entry has to be
	// refuted before it transitions to Dead. Short enough that failed
	// peers are caught fast, long enough that a slow GC or paused VM
	// has a fair chance to refute.
	DefaultSuspectTimeout = 5 * time.Second
	// DefaultDeadRetention is how long Dead entries are kept before
	// being pruned. Long enough that gossip rumours about the dead
	// peer have time to settle across the cluster before the table
	// forgets it (otherwise a slow peer learns about the death from
	// one peer, only to be re-introduced by another that hadn't
	// converged yet).
	DefaultDeadRetention = time.Minute
	// DefaultAntiEntropyInterval is how often we sync our full table
	// with one random peer. Anti-entropy is the safety net under
	// piggybacked gossip — anything that fell off the piggyback bus
	// is repaired here.
	DefaultAntiEntropyInterval = 30 * time.Second
	// DefaultTimeout caps every TCP read and write on the gossip wire.
	DefaultTimeout = 2 * time.Second
	// DefaultPiggybackCap bounds how many recent events we attach to
	// each outbound message. Large enough to converge a small cluster
	// in one probe cycle, small enough that a single Ping fits in one
	// TCP segment.
	DefaultPiggybackCap = 32
)

// Options bundles the tunables a Subsystem needs. Zero values become
// the defaults above, so callers can pass an empty Options and get a
// production-shaped subsystem.
type Options struct {
	// Addr is the host:port the listener binds to. Often 0.0.0.0:PORT
	// in containerised deployments so the listener accepts on every
	// interface.
	Addr string
	// Advertise is the dialable host:port other peers should reach us
	// at. Empty means "use whatever the listener resolved to" which is
	// only correct when Addr is already a concrete IP. When the bind
	// is 0.0.0.0:PORT (the typical container case), peers receiving an
	// "address: 0.0.0.0:PORT" event would try to dial their own
	// loopback — set Advertise to the routable hostname:PORT instead
	// (e.g. silo-a:7100 in docker-compose, or the Pod IP in k8s).
	Advertise           string
	Seeds               []string
	ProbeInterval       time.Duration
	ProbeTimeout        time.Duration
	IndirectK           int
	SuspectTimeout      time.Duration
	DeadRetention       time.Duration
	AntiEntropyInterval time.Duration
	Timeout             time.Duration
	PiggybackCap        int

	// ServerTLS is the mTLS config used when accepting inbound gossip
	// connections — symmetric with ClientTLS because every peer is both
	// a client and a server in SWIM. Required; the subsystem refuses to
	// start without it because clear-text gossip would leak cluster
	// topology to anyone on the wire.
	ServerTLS *tls.Config
	// ClientTLS is the mTLS config used when dialing peers. Must use
	// the same node cert as ServerTLS so peers can pin our identity
	// either way.
	ClientTLS *tls.Config

	// Extension, if set, rides the anti-entropy exchange to reconcile a
	// subsystem's own state (the CRDT namespace). gossip ferries its opaque
	// bytes during each sync without interpreting them. Optional.
	Extension SyncExtension
}

// SyncExtension lets a subsystem piggyback its reconcilable state on the
// gossip anti-entropy exchange. gossip calls LocalState to obtain the bytes
// to send a peer and MergeRemote to fold in the bytes a peer sent back.
// Implementations must be safe for concurrent use.
type SyncExtension interface {
	LocalState() ([]byte, error)
	MergeRemote([]byte) error
}

// Subsystem is the gossip lifecycle wrapper, shaped to fit silod's
// generic subsystem interface (Name/Start/Shutdown). One Subsystem
// per silod process; safe for concurrent use through its embedded
// Membership table and explicit mutex.
type Subsystem struct {
	opts    Options
	logger  *slog.Logger
	members *membership.Membership

	mu sync.Mutex
	ln net.Listener

	// piggybackMu guards the bounded slice of recent events that ride
	// on every outbound Ping/Ack/Sync. Recent-events feed back as
	// piggyback until they age out.
	piggybackMu sync.Mutex
	piggyback   []membership.Event

	// dialer is a seam so tests can substitute a fake TCP path.
	dialer dialer

	wg     sync.WaitGroup
	stopCh chan struct{}

	// lastSync is the unix-nano time of the last successful anti-entropy sync;
	// 0 until the first. Read by the metrics scrape to report sync lag.
	lastSync atomic.Int64

	// incompatibleMsgs counts messages dropped because the sender's protocol is
	// below clusterproto.MinCompatible (fenced). newerProtoMsgs counts messages
	// from a peer ahead of us (processed best-effort). Both feed the metrics
	// scrape so an operator can watch a rolling upgrade converge.
	incompatibleMsgs atomic.Int64
	newerProtoMsgs   atomic.Int64
}

// timeNow is the clock the gossip metrics read; overridable in tests.
var timeNow = time.Now

// classifyProto is the protocol-compatibility check applied to every inbound
// message. It is a seam so tests can force the fenced (PeerTooOld) path, which
// the launch support window (MinCompatible == Protocol == 1) cannot yet produce
// from real input — the first MinCompatible bump makes it reachable in
// production.
var classifyProto = clusterproto.Classify

// dialer abstracts the act of opening a TLS gossip connection. The
// default implementation calls tls.Dial; tests inject a stub that
// returns net.Pipe ends so they exercise the framing/state-machine
// logic without a real listener.
type dialer interface {
	Dial(addr string, cfg *tls.Config, timeout time.Duration) (net.Conn, error)
}

type tlsDialer struct{}

func (tlsDialer) Dial(addr string, cfg *tls.Config, timeout time.Duration) (net.Conn, error) {
	d := &net.Dialer{Timeout: timeout}
	conn, err := tls.DialWithDialer(d, "tcp", addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("gossip: could not dial %s (%w)", addr, err)
	}
	return conn, nil
}

// New builds a Subsystem rooted at members. Defaults are filled in
// for any zero-valued Options field. The caller is responsible for
// passing a Membership that was constructed with the right self-id
// and listener address — the typical flow constructs Membership in
// silod.Run, hands it to the gRPC chunk service for placement, and
// then to this constructor.
func New(members *membership.Membership, opts Options, logger *slog.Logger) (*Subsystem, error) {
	if members == nil {
		return nil, errors.New("gossip: membership is nil; construct one with membership.New(cfg.NodeID, cfg.GossipAddr, cfg.GRPCAdvertise)")
	}
	if logger == nil {
		return nil, errors.New("gossip: logger is nil; pass the slog logger built by observability.NewLogger")
	}
	if opts.Addr == "" {
		return nil, errors.New("gossip: Addr is empty; set SILO_GOSSIP_ADDR to a host:port silod can listen on, e.g. 0.0.0.0:7100 (the default)")
	}
	if opts.ServerTLS == nil || opts.ClientTLS == nil {
		return nil, errors.New("gossip: ServerTLS and ClientTLS are required; build them from clustertls.ServerConfig and clustertls.ClientConfig")
	}
	// Self-seed misconfiguration: an operator put this node's own
	// gossip address in SILO_SEEDS, which would have the prober trying
	// to gossip with itself. Refuse early with an instruction-shaped
	// error so this is caught at boot, not as silent log-line spam.
	selfID := members.SelfID()
	for _, seed := range opts.Seeds {
		if seed == "" {
			continue
		}
		if seed == opts.Addr || seed == selfID {
			return nil, fmt.Errorf("gossip: SILO_SEEDS contains this node's own identity (%q); set SILO_SEEDS to the host:port of OTHER silod nodes, e.g. 'silo-b:7100,silo-c:7100' on silo-a", seed)
		}
	}
	s := &Subsystem{
		opts:    withDefaults(opts),
		logger:  logger,
		members: members,
		dialer:  tlsDialer{},
		stopCh:  make(chan struct{}),
	}
	return s, nil
}

// withDefaults fills in zero-valued tunables with their package
// defaults. Pulled out so unit tests can build Options minimally.
func withDefaults(o Options) Options {
	if o.ProbeInterval <= 0 {
		o.ProbeInterval = DefaultProbeInterval
	}
	if o.ProbeTimeout <= 0 {
		o.ProbeTimeout = DefaultProbeTimeout
	}
	if o.IndirectK <= 0 {
		o.IndirectK = DefaultIndirectK
	}
	if o.SuspectTimeout <= 0 {
		o.SuspectTimeout = DefaultSuspectTimeout
	}
	if o.DeadRetention <= 0 {
		o.DeadRetention = DefaultDeadRetention
	}
	if o.AntiEntropyInterval <= 0 {
		o.AntiEntropyInterval = DefaultAntiEntropyInterval
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.PiggybackCap <= 0 {
		o.PiggybackCap = DefaultPiggybackCap
	}
	return o
}

// Name is silod's subsystem identifier.
func (s *Subsystem) Name() string { return "gossip" }

// Members returns the goroutine-safe membership table this subsystem
// reads and writes. Exposed so silod and ops tooling can read the
// snapshot without poking at private fields.
func (s *Subsystem) Members() *membership.Membership { return s.members }

// Drain marks this node as having voluntarily left the cluster and broadcasts
// that over gossip. Peers route placement around a Left node, and the scrubber
// on the survivors re-replicates the chunks this node held onto other nodes —
// so the volume's replication factor is restored without it.
//
// Drain is not shutdown: the node keeps running and keeps serving its chunks so
// survivors can rebuild from a quorum. Watch silo_replication_shortfall_chunks
// fall back to zero, then the node is safe to stop and remove. Returns whether a
// Left event was announced (false if the node had already left).
func (s *Subsystem) Drain() bool {
	ev, changed := s.members.MarkLeft(s.members.SelfID())
	if !changed {
		return false
	}
	s.remember(ev)
	s.logger.Info("node draining; announced Left over gossip so peers re-replicate its chunks", "node", ev.ID)
	return true
}

// MetricPrefix namespaces the gossip metrics.
func (s *Subsystem) MetricPrefix() string { return "silo_gossip" }

// CollectMetrics reports the membership the node sees, broken down by SWIM
// state, and how long ago this node last completed an anti-entropy sync (the
// gossip-lag signal — a value that keeps climbing means the node is isolated).
func (s *Subsystem) CollectMetrics() []metrics.Metric {
	counts := map[membership.State]int{}
	for _, n := range s.members.Members() {
		counts[n.State]++
	}
	out := make([]metrics.Metric, 0, 5)
	for _, st := range []membership.State{membership.StateAlive, membership.StateSuspect, membership.StateDead, membership.StateLeft} {
		out = append(out, metrics.Metric{
			Name:   "members",
			Help:   "Cluster members this node currently sees, by SWIM state.",
			Kind:   metrics.Gauge,
			Value:  float64(counts[st]),
			Labels: [][2]string{{"state", st.String()}},
		})
	}
	if last := s.lastSync.Load(); last > 0 {
		out = append(out, metrics.Metric{
			Name:  "last_sync_age_seconds",
			Help:  "Seconds since this node last completed an anti-entropy sync with a peer.",
			Kind:  metrics.Gauge,
			Value: timeNow().Sub(time.Unix(0, last)).Seconds(),
		})
	}
	out = append(out,
		metrics.Metric{
			Name:  "protocol_version",
			Help:  "Cluster wire-protocol version this node speaks (clusterproto.Protocol).",
			Kind:  metrics.Gauge,
			Value: float64(clusterproto.Protocol),
		},
		metrics.Metric{
			Name:  "incompatible_messages_total",
			Help:  "Gossip messages dropped because the sender's protocol is below the minimum this node supports.",
			Kind:  metrics.Counter,
			Value: float64(s.incompatibleMsgs.Load()),
		},
		metrics.Metric{
			Name:  "newer_protocol_messages_total",
			Help:  "Gossip messages received from a peer on a newer protocol than this node (processed best-effort).",
			Kind:  metrics.Counter,
			Value: float64(s.newerProtoMsgs.Load()),
		},
	)
	return out
}

// Addr returns the bound listener address, or "" before Start.
func (s *Subsystem) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Start binds the gossip listener and launches the prober, the
// anti-entropy loop, and the seed-join attempt. Returns nil on
// graceful Shutdown. Blocking call; silod runs it in a goroutine.
func (s *Subsystem) Start() error {
	ln, err := net.Listen("tcp", s.opts.Addr)
	if err != nil {
		return fmt.Errorf("gossip: could not bind listener at %q (%w); set SILO_GOSSIP_ADDR to a free host:port, e.g. 0.0.0.0:7100", s.opts.Addr, err)
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	bound := ln.Addr().String()
	advertise := s.opts.Advertise
	if advertise == "" {
		advertise = bound
	}
	s.members.SetSelfAddress(advertise)
	s.logger.Info("gossip listener started", "addr", bound, "advertise", advertise)

	tlsLn := tls.NewListener(ln, s.opts.ServerTLS)

	s.wg.Add(1)
	go s.acceptLoop(tlsLn)

	s.wg.Add(1)
	go s.probeLoop()

	s.wg.Add(1)
	go s.antiEntropyLoop()

	if len(s.opts.Seeds) > 0 {
		s.wg.Add(1)
		go s.joinLoop()
	}

	<-s.stopCh
	return nil
}

// Shutdown closes the listener, signals every goroutine to stop, and
// waits for them to drain. Bounded by the supplied context so silod
// always honours its overall shutdown budget; goroutines still in
// flight when the deadline expires are abandoned (their goroutines
// observe stopCh on the next tick and exit, but Shutdown itself
// returns the deadline error).
func (s *Subsystem) Shutdown(ctx context.Context) error {
	select {
	case <-s.stopCh:
		return nil
	default:
	}
	close(s.stopCh)
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("gossip: shutdown deadline expired (%w); increase the shutdown budget or investigate stuck probes", ctx.Err())
	}
}

// pickAlivePeer returns one random Alive peer, or "", "", false if
// none exist. Exposed for tests that drive the prober loop one tick at
// a time.
func (s *Subsystem) pickAlivePeer() (id, addr string, ok bool) {
	peers := s.members.AlivePeers()
	if len(peers) == 0 {
		return "", "", false
	}
	p := peers[rand.IntN(len(peers))] //nolint:gosec // SWIM picks targets by uniform random, no cryptographic strength needed
	return p.ID, p.Address, true
}

// pickHelpers returns up to k random Alive peers whose ID differs from
// target. Used by the indirect-probe path.
func (s *Subsystem) pickHelpers(target string, k int) []membership.Node {
	peers := s.members.AlivePeers()
	// Filter out the target.
	candidates := peers[:0:0]
	for _, p := range peers {
		if p.ID == target {
			continue
		}
		candidates = append(candidates, p)
	}
	if len(candidates) <= k {
		return candidates
	}
	// Fisher–Yates partial shuffle.
	for i := 0; i < k; i++ {
		j := i + rand.IntN(len(candidates)-i) //nolint:gosec // selection randomness, not cryptographic
		candidates[i], candidates[j] = candidates[j], candidates[i]
	}
	return candidates[:k]
}

// remember stores ev in the piggyback ring, trimming the oldest entry
// when the cap is exceeded. Callers feed every locally-observed state
// change in (own MarkSuspect/MarkDead) plus every event Apply
// reported as changed (so we re-broadcast peers' news).
func (s *Subsystem) remember(events ...membership.Event) {
	if len(events) == 0 {
		return
	}
	s.piggybackMu.Lock()
	defer s.piggybackMu.Unlock()
	s.piggyback = append(s.piggyback, events...)
	if len(s.piggyback) > s.opts.PiggybackCap {
		s.piggyback = s.piggyback[len(s.piggyback)-s.opts.PiggybackCap:]
	}
}

// piggybackSnapshot returns a copy of the current piggyback slice for
// attaching to an outbound message. The slice is returned by value so
// the caller can mutate or shorten it without affecting the canonical
// ring.
func (s *Subsystem) piggybackSnapshot() []membership.Event {
	s.piggybackMu.Lock()
	defer s.piggybackMu.Unlock()
	if len(s.piggyback) == 0 {
		return nil
	}
	out := make([]membership.Event, len(s.piggyback))
	copy(out, s.piggyback)
	return out
}

// selfEnvelope builds the sender fields for an outbound message from
// the current Self entry. Pulled into a helper so the four call sites
// (probe, ack, ping-req, sync) stay in lockstep when the envelope grows.
func (s *Subsystem) selfEnvelope(kind MessageKind, target string) *Message {
	self := s.members.Self()
	return &Message{
		Kind:              kind,
		SenderID:          self.ID,
		SenderAddress:     self.Address,
		SenderDataAddress: self.DataAddress,
		SenderIncarn:      self.Incarnation,
		SenderProto:       clusterproto.Protocol,
		Target:            target,
		Piggyback:         s.piggybackSnapshot(),
	}
}

// extLocalState returns the sync extension's bytes to attach to an outbound
// sync message, or nil when no extension is configured or it errors (a
// failed reconciliation must not break membership gossip).
func (s *Subsystem) extLocalState() []byte {
	if s.opts.Extension == nil {
		return nil
	}
	b, err := s.opts.Extension.LocalState()
	if err != nil {
		s.logger.Warn("gossip sync extension could not produce local state; skipping this round", "err", err)
		return nil
	}
	return b
}

// extMergeRemote folds a peer's extension bytes into the local subsystem.
func (s *Subsystem) extMergeRemote(b []byte) {
	if s.opts.Extension == nil || len(b) == 0 {
		return
	}
	if err := s.opts.Extension.MergeRemote(b); err != nil {
		s.logger.Warn("gossip sync extension could not merge a peer's state", "err", err)
	}
}

// applyIncoming merges an incoming message's piggyback and sender
// claim into our table, remembering changes so we re-broadcast them.
// Returns the set of changed Node entries so callers (the sync path)
// can log meaningful transitions.
func (s *Subsystem) applyIncoming(msg *Message) []membership.Node {
	// A peer presenting our own NodeID as their SenderID means two
	// silods share a node id — a hard misconfiguration we can detect
	// at runtime. Log loudly; do not merge the claim (which would
	// trigger our own self-refutation logic and oscillate forever).
	if msg.SenderID == s.members.SelfID() {
		s.logger.Error("gossip received a message from another peer claiming our own NodeID; two nodes share a SILO_NODE_ID and must not — give every silod a unique SILO_NODE_ID and restart",
			"node_id", msg.SenderID,
			"remote_address", msg.SenderAddress,
		)
		return nil
	}
	// Protocol handshake: classify the sender's wire-protocol version. A peer
	// below MinCompatible is fenced — we drop the whole message rather than
	// risk merging a view we cannot safely interpret. A peer ahead of us is
	// processed best-effort but flagged so the operator upgrades this node.
	switch classifyProto(msg.SenderProto) {
	case clusterproto.PeerTooOld:
		s.incompatibleMsgs.Add(1)
		s.logger.Error("gossip dropped a message from a peer on an unsupported protocol; upgrade or remove the node",
			"peer", msg.SenderID, "peer_protocol", msg.SenderProto,
			"min_compatible", clusterproto.MinCompatible, "our_protocol", clusterproto.Protocol)
		return nil
	case clusterproto.PeerNewer:
		s.newerProtoMsgs.Add(1)
		s.logger.Warn("gossip peer is on a newer protocol than this node; upgrade silod here to stay compatible",
			"peer", msg.SenderID, "peer_protocol", msg.SenderProto, "our_protocol", clusterproto.Protocol)
	case clusterproto.Compatible:
		// Same wire protocol — proceed.
	}
	changed := s.members.ApplyMany(msg.Piggyback)
	if msg.SenderID != "" {
		if n, ok := s.members.Apply(membership.Event{
			ID:          msg.SenderID,
			Address:     msg.SenderAddress,
			DataAddress: msg.SenderDataAddress,
			State:       membership.StateAlive,
			Incarnation: msg.SenderIncarn,
		}); ok {
			changed = append(changed, n)
		}
	}
	// Re-broadcast changes by feeding them back into the piggyback
	// ring as Events.
	if len(changed) > 0 {
		evs := make([]membership.Event, 0, len(changed))
		for _, n := range changed {
			evs = append(evs, membership.Event{
				ID:          n.ID,
				Address:     n.Address,
				DataAddress: n.DataAddress,
				State:       n.State,
				Incarnation: n.Incarnation,
				At:          n.LastChange,
			})
		}
		s.remember(evs...)
		for _, n := range changed {
			s.logger.Info("gossip state change", "node", n.ID, "state", n.State.String(), "incarnation", n.Incarnation)
		}
	}
	return changed
}
