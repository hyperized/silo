// Package silod composes the daemon's sub-components into a single Run
// lifecycle. cmd/silod is the thin process entry point.
package silod

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hyperized/silo/internal/bootstraptoken"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/clockskew"
	"github.com/hyperized/silo/internal/clustertls"
	"github.com/hyperized/silo/internal/config"
	"github.com/hyperized/silo/internal/crypto"
	"github.com/hyperized/silo/internal/exporter"
	"github.com/hyperized/silo/internal/gossip"
	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/metrics"
	"github.com/hyperized/silo/internal/namespace"
	"github.com/hyperized/silo/internal/nbd"
	"github.com/hyperized/silo/internal/observability"
	"github.com/hyperized/silo/internal/replication"
	"github.com/hyperized/silo/internal/transport"

	"google.golang.org/grpc/credentials"
)

// namespaceSyncExt adapts the CRDT namespace to gossip's SyncExtension so
// it reconciles across nodes on the existing anti-entropy exchange.
type namespaceSyncExt struct{ ns *namespace.Namespace }

func (e namespaceSyncExt) LocalState() ([]byte, error) { return e.ns.Snapshot() }
func (e namespaceSyncExt) MergeRemote(b []byte) error  { return e.ns.MergeBytes(b) }

// shutdownTimeout bounds graceful shutdown of every sub-component. Tuned
// to outlast the slowest expected in-flight HTTP handler / gRPC stream
// but stay well under any orchestrator's default SIGTERM-to-SIGKILL gap
// (k8s defaults to 30s) so we never trip an external preemption.
const shutdownTimeout = 5 * time.Second

// subsystem is a runnable component of silod with a uniform lifecycle.
// Run treats every subsystem the same so adding gossip, replication, or
// a UI server later does not change the lifecycle code.
type subsystem interface {
	Name() string
	Start() error
	Shutdown(ctx context.Context) error
}

// Factories for each subsystem are package-level so tests can swap in
// fakes without spinning up real listeners.
var (
	newHTTPSubsystem = func(cfg *config.Config, version string, logger *slog.Logger, metrics http.Handler) subsystem {
		return &httpSub{srv: observability.NewServer(cfg.HTTPAddr, cfg.NodeID, version, logger, observability.WithMetricsHandler(metrics))}
	}
	newGRPCSubsystem = func(cfg *config.Config, tlsCfg *tls.Config, tokenAuth *transport.TokenAuthenticator, store chunkstore.Store, coord transport.Coordinator, ns transport.NamespaceOps, members transport.StatusMembers, drainer transport.Drainer, version string, logger *slog.Logger) subsystem {
		opts := []transport.GRPCOption{
			transport.WithStatusService(transport.NewStatusService(members, store, cfg.DataDir, cfg.NodeID, version, logger)),
		}
		if drainer != nil {
			opts = append(opts, transport.WithNodeAdminService(transport.NewNodeAdminService(drainer, cfg.NodeID, logger)))
		}
		if tokenAuth != nil {
			opts = append(opts, transport.WithTokenAuth(tokenAuth))
		}
		return &grpcSub{srv: transport.NewGRPCServer(cfg.GRPCAddr, tlsCfg, store, coord, ns, logger, opts...)}
	}
	newScrubberSubsystem = func(cfg *config.Config, place replication.Placement, catalog replication.ChunkCatalog, probe replication.ReplicaProbe, logger *slog.Logger) subsystem {
		return replication.NewScrubber(place, catalog, probe, cfg.Replication, cfg.ScrubInterval, logger)
	}
	newRebalancerSubsystem = func(cfg *config.Config, members *membership.Membership, logger *slog.Logger) subsystem {
		return replication.NewRebalancer(members, cfg.DataDir, cfg.ScrubInterval, logger)
	}
	// newNBDSubsystem builds the NBD block-device listener. Only constructed
	// when SILO_NBD_ADDR is set, since NBD is unauthenticated block I/O.
	newNBDSubsystem = func(cfg *config.Config, ns nsVolumes, coord transport.Coordinator, logger *slog.Logger) subsystem {
		backend := newVolumeBackend(ns, coord, cfg.NodeID, logger)
		return newNBDSub(cfg.NBDAddr, nbd.NewServer(backend, logger), logger)
	}
	newBootstrapSubsystem = func(cfg *config.Config, tlsCfg *tls.Config, tokens transport.TokenRedeemer, minter transport.ClientCertMinter, logger *slog.Logger) subsystem {
		svc := transport.NewBootstrapService(tokens, minter, cfg.GRPCAdvertise, logger)
		return &bootstrapSub{srv: transport.NewBootstrapServer(cfg.BootstrapAddr, tlsCfg, svc, logger)}
	}
	// newGossipSubsystem constructs the gossip subsystem. Returns
	// (nil, error) when the configuration is rejected at construction
	// time — typically a self-seed misconfiguration that the operator
	// must fix before silod can start.
	newGossipSubsystem = func(cfg *config.Config, serverTLS, clientTLS *tls.Config, members *membership.Membership, ext gossip.SyncExtension, logger *slog.Logger) (subsystem, error) {
		s, err := gossip.New(members, gossip.Options{
			Addr:      cfg.GossipAddr,
			Advertise: cfg.GossipAdvertise,
			Seeds:     cfg.Seeds,
			ServerTLS: serverTLS,
			ClientTLS: clientTLS,
			Extension: ext,
		}, logger)
		if err != nil {
			return nil, err
		}
		return &gossipSub{srv: s}, nil
	}
	newChunkStore = func(cfg *config.Config, cipher *crypto.Cipher) (chunkstore.Store, error) {
		return chunkstore.NewFileStore(cfg.DataDir, cipher)
	}
	// newMembership is the seam Run goes through to construct the
	// shared membership table. Defaults to membership.New; tests
	// substitute it to exercise the "could not initialise" branch
	// without smuggling an invalid NodeID past the config validator.
	newMembership = membership.New
	// newNamespace is the seam Run goes through to open the persistent
	// namespace. Defaults to namespace.Open; tests substitute it to exercise
	// the "could not open" branch without an unreadable data dir.
	newNamespace = namespace.Open
	// loadClusterTLS loads (and bootstraps if needed) the cluster CA
	// and this node's TLS material. Swappable so tests can inject a
	// minimal in-memory pair without writing files.
	loadClusterTLS = defaultLoadClusterTLS
	// openTokenStore is the seam through which silod opens the bootstrap
	// token list. Tests substitute a constructor that hands back a
	// no-op redeemer so they don't have to touch the filesystem.
	openTokenStore = defaultOpenTokenStore
)

// defaultOpenTokenStore opens the bootstrap-token list at the
// conventional path under DataDir. Pulled out into a seam so unit tests
// for Run can inject a fake redeemer without writing JSON files; the
// production path is one ReadFile-plus-JSON-Unmarshal away.
func defaultOpenTokenStore(cfg *config.Config) (*bootstraptoken.Store, error) {
	return bootstraptoken.Open(filepath.Join(cfg.DataDir, bootstraptoken.DefaultStoreName()))
}

// defaultClusterCALifetime is the validity window silod requests when it
// mints its own cluster CA at first boot. Long enough to outlive
// expected hardware refresh cycles, short enough that operators upgrading
// from a self-bootstrapped dev cluster to a production deployment have a
// natural moment to swap in a real CA.
const defaultClusterCALifetime = 10 * 365 * 24 * time.Hour

// clusterCAJoinTimeout caps how long a non-seed silod waits for the
// shared CA cert + key to appear at the configured paths. 30s matches
// the worst-case docker-compose cold-start time on a busy laptop;
// kubernetes deployments with slow secret provisioning may want to
// tune this upward once that becomes a real constraint. Declared as a
// var so unit tests can shorten the wait without spawning real
// 30-second delays.
var clusterCAJoinTimeout = 30 * time.Second

// clusterCAJoinPoll is how often waitForCA re-stats the files while
// waiting. Short enough that a fast docker-compose boot completes
// without measurable extra latency.
var clusterCAJoinPoll = 250 * time.Millisecond

// waitForCA blocks until the cert+key at the operator-supplied paths
// both exist, or until clusterCAJoinTimeout elapses. The function
// returns immediately when the cert file is already on disk — even if
// the key isn't — so a verifier-only node (operator distributed the
// cert but not the key) is not stuck waiting for a key that will never
// arrive. The timeout error is actionable: it names the paths and
// points operators at the seed node's logs.
func waitForCA(certPath, keyPath string) error {
	deadline := time.Now().Add(clusterCAJoinTimeout)
	for time.Now().Before(deadline) {
		// Cert alone is enough to leave the wait; LoadCA will fail
		// loudly later if the cert is malformed, and the verifier-only
		// case is handled by the caller.
		if fileExists(certPath) {
			return nil
		}
		// Key without cert is the inverse race: the seed has flushed
		// the key but not the cert yet. Wait until the cert lands too.
		if fileExists(keyPath) && !fileExists(certPath) {
			time.Sleep(clusterCAJoinPoll)
			continue
		}
		time.Sleep(clusterCAJoinPoll)
	}
	return fmt.Errorf("silod: waited %s for the shared cluster CA to appear at %s and %s; check that the seed node has written them (in docker-compose, silo-a is the implicit seed — view its logs with 'docker compose logs silo-a')", clusterCAJoinTimeout, certPath, keyPath)
}

// defaultLoadClusterTLS reads (or mints) the cluster CA and this node's
// TLS material. The behavior depends on what is already on disk and on
// whether the operator explicitly set SILO_TLS_CA_CERT:
//
//   - CA cert + CA key both present at the configured paths: load them.
//   - Operator set SILO_TLS_CA_CERT but the files are not (yet) on disk:
//     wait up to clusterCAJoinTimeout for the seed node to write them.
//     This is the docker-compose / Kubernetes pattern where node-a
//     mints into a shared volume and node-b/c just load.
//   - SILO_TLS_CA_CERT not set and nothing on disk: mint a fresh
//     self-signed CA into the default DataDir/ca.{crt,key} location.
//     This is the zero-configuration single-node path.
//   - Cert present without key: load cert-only (verifier-only mode).
//     The node refuses to mint a new node cert in this mode.
//
// Operators who want to share a CA across nodes point SILO_TLS_CA_CERT
// and SILO_TLS_CA_KEY at a shared location (a Kubernetes secret, an NFS
// mount, the docker-compose named volume) so every node loads the same
// material instead of minting its own.
func defaultLoadClusterTLS(cfg *config.Config) (*clustertls.CA, *clustertls.NodeCert, error) {
	switch {
	case cfg.CAExternal && cfg.CASeed:
		// Seed node in a shared-volume deployment: mint into the
		// external path on first boot, load on every subsequent boot.
		if !fileExists(cfg.CACertPath) && !fileExists(cfg.CAKeyPath) {
			if err := mintClusterCA(cfg.CACertPath, cfg.CAKeyPath, cfg.NodeID); err != nil {
				return nil, nil, err
			}
		}
	case cfg.CAExternal:
		// Non-seed node: wait for the seed to publish the CA. This
		// path is what makes the docker-compose three-node cluster
		// work without an external PKI.
		if err := waitForCA(cfg.CACertPath, cfg.CAKeyPath); err != nil {
			return nil, nil, err
		}
	default:
		// Single-node dev path: mint into the default DataDir/ca.* if
		// nothing is there yet.
		if !fileExists(cfg.CACertPath) && !fileExists(cfg.CAKeyPath) {
			if err := mintClusterCA(cfg.CACertPath, cfg.CAKeyPath, cfg.NodeID); err != nil {
				return nil, nil, err
			}
		}
	}
	caKeyPresent := fileExists(cfg.CAKeyPath)

	certPEM, err := os.ReadFile(cfg.CACertPath)
	if err != nil {
		return nil, nil, fmt.Errorf("could not read the cluster CA certificate at %s (%w); delete the file so silod can mint a fresh one, or point SILO_TLS_CA_CERT at a CA cert generated elsewhere", cfg.CACertPath, err)
	}
	var keyPEM []byte
	if caKeyPresent {
		keyPEM, err = os.ReadFile(cfg.CAKeyPath)
		if err != nil {
			return nil, nil, fmt.Errorf("could not read the cluster CA key at %s (%w); either remove SILO_TLS_CA_KEY (this silod will not be able to mint its own node cert) or point it at a readable key file", cfg.CAKeyPath, err)
		}
	}
	ca, err := clustertls.LoadCA(certPEM, keyPEM)
	if err != nil {
		return nil, nil, err
	}
	// IPs in the node cert SAN: the loopback plus any host IPs we can
	// discover. This keeps gRPC verification happy when the operator
	// dials by IP from siloctl or another container on the same bridge.
	ips := []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	nodeCert, err := clustertls.LoadOrMintNode(cfg.DataDir, ca, cfg.NodeID, []string{"localhost"}, ips)
	if err != nil {
		return nil, nil, err
	}
	return ca, nodeCert, nil
}

// mintClusterCA writes a fresh self-signed CA at (certPath, keyPath).
// Used only when neither file is on disk yet. nodeID is folded into the
// CA common name so an operator inspecting two clusters can tell them
// apart without having to read the certificate serials.
func mintClusterCA(certPath, keyPath, nodeID string) error {
	if err := os.MkdirAll(filepath.Dir(certPath), 0o700); err != nil {
		return fmt.Errorf("could not create the directory for the cluster CA at %s (%w); check the parent path is on a writable filesystem and silod has permission", certPath, err)
	}
	certPEM, keyPEM, err := generateCA("silo-"+nodeID, defaultClusterCALifetime)
	if err != nil {
		return err
	}
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		return fmt.Errorf("could not write the newly-minted cluster CA cert to %s (%w); check the data directory is writable", certPath, err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		_ = os.Remove(certPath)
		return fmt.Errorf("could not write the newly-minted cluster CA key to %s (%w); the partial cert at %s was removed", keyPath, err, certPath)
	}
	return nil
}

// fileExists distinguishes "file is on disk" from "file is missing"
// without surfacing every Stat error to callers. The self-mint path
// treats a missing file as a trigger to mint, not an error to propagate.
// Package-level var so tests can substitute it for the unusual "Stat
// returns a non-IsNotExist error" branch.
var fileExists = func(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

// generateCA is the seam mintClusterCA calls through so tests can
// exercise the "could not generate" branch without depleting host
// entropy. Production points at clustertls.GenerateCA.
var generateCA = clustertls.GenerateCA

// Run blocks until ctx is cancelled or any subsystem fails. It returns
// nil on graceful shutdown. Future sub-components plug into this single
// select loop so silod has exactly one place where the lifecycle decision
// is made.
//
// announce is where silod prints the inaugural bootstrap token + server
// fingerprint on first boot. Production wires it to os.Stdout; tests
// can pass a bytes.Buffer to assert on the operator-facing handshake
// string without grepping process output.
func Run(ctx context.Context, cfg *config.Config, logger *slog.Logger, announce io.Writer, version string) error {
	if cfg == nil {
		return fmt.Errorf("silod.Run: cfg is nil; pass a *config.Config loaded from config.LoadFromEnv or constructed in tests")
	}
	if logger == nil {
		return fmt.Errorf("silod.Run: logger is nil; pass an *slog.Logger built with observability.NewLogger")
	}
	if announce == nil {
		announce = io.Discard
	}

	key, err := keyProvider(cfg).ClusterKey()
	if err != nil {
		return fmt.Errorf("silod.Run: could not load the cluster encryption key (%w)", err)
	}
	cipher, err := crypto.NewCipher(key)
	if err != nil {
		return fmt.Errorf("silod.Run: could not initialise the cluster encryption key (%w)", err)
	}

	store, err := newChunkStore(cfg, cipher)
	if err != nil {
		return fmt.Errorf("silod.Run: could not open the chunk store (%w); check SILO_DATA_DIR is on a writable filesystem and silod has permission", err)
	}

	ca, nodeCert, err := loadClusterTLS(cfg)
	if err != nil {
		return fmt.Errorf("silod.Run: could not load the cluster TLS material (%w)", err)
	}
	crl, err := loadRevocation(cfg, ca, logger)
	if err != nil {
		return fmt.Errorf("silod.Run: %w", err)
	}
	tokenAuth, err := newTokenAuthenticator(cfg, ca, logger)
	if err != nil {
		return fmt.Errorf("silod.Run: %w", err)
	}
	serverTLS, err := clustertls.ServerConfig(ca, nodeCert, clustertls.WithRevocation(crl))
	if err != nil {
		return fmt.Errorf("silod.Run: could not build the gRPC TLS server config (%w)", err)
	}
	// PeerConfig cannot fail here for the same reason ServerOnlyConfig
	// can't: both helpers re-parse the NodeCert that ServerConfig just
	// accepted, so any malformed pair would have surfaced two lines up.
	// Defensive guard kept against a future refactor that swaps the
	// shared parse path for two divergent ones.
	peerTLS, err := clustertls.PeerConfig(ca, nodeCert, clustertls.WithRevocation(crl))
	if err != nil {
		return fmt.Errorf("silod.Run: unexpected peer TLS config failure after ServerConfig succeeded (%w); please file a bug at https://github.com/hyperized/silo/issues", err)
	}
	// ServerOnlyConfig cannot fail here: ServerConfig already parsed the
	// same NodeCert via tls.X509KeyPair above, so any error in the cert
	// pair would have surfaced on the previous line. Asserting the
	// invariant here keeps a stray nil-NodeCert refactor from silently
	// landing.
	bootstrapTLS, err := clustertls.ServerOnlyConfig(nodeCert)
	if err != nil {
		return fmt.Errorf("silod.Run: unexpected bootstrap TLS server config failure after ServerConfig succeeded (%w); please file a bug at https://github.com/hyperized/silo/issues", err)
	}

	tokenStorePath := filepath.Join(cfg.DataDir, bootstraptoken.DefaultStoreName())
	firstBoot := !fileExists(tokenStorePath)
	tokens, err := openTokenStore(cfg)
	if err != nil {
		return fmt.Errorf("silod.Run: could not open the bootstrap-token store (%w)", err)
	}
	if firstBoot || cfg.PrintBootstrapToken {
		if err := announceBootstrap(announce, tokens, nodeCert, cfg); err != nil {
			return fmt.Errorf("silod.Run: could not mint the inaugural bootstrap token (%w)", err)
		}
	}

	logger.Info("silo starting",
		"version", version,
		"node_id", cfg.NodeID,
		"grpc_addr", cfg.GRPCAddr,
		"bootstrap_addr", cfg.BootstrapAddr,
		"gossip_addr", cfg.GossipAddr,
		"http_addr", cfg.HTTPAddr,
		"seeds", cfg.Seeds,
		"domain", cfg.Domain,
		"data_dir", cfg.DataDir,
		"chunk_size", cfg.ChunkSize,
		"replication", cfg.Replication,
		"encryption_key_source", cfg.KeySource,
	)

	members, err := newMembership(cfg.NodeID, cfg.GossipAddr, cfg.GRPCPeerAdvertise)
	if err != nil {
		return fmt.Errorf("silod.Run: could not initialise the membership table (%w)", err)
	}
	// The namespace is this node's replica of the cluster filesystem tree.
	// It persists local mutations under DataDir and reloads them on restart,
	// rides the gossip anti-entropy exchange to converge with peers, and a
	// background sweep reclaims tombstones older than the retention window.
	// The sweep stops when ctx is cancelled on shutdown.
	// The skew monitor compares peer-issued HLC timestamps (seen as peer
	// state arrives over anti-entropy) against this node's clock, warning and
	// counting an alert when a peer runs ahead beyond the threshold — the
	// early signal of broken time sync, which silently corrupts write order.
	skew := clockskew.New(cfg.MaxClockSkew, logger)
	ns, err := newNamespace(hlc.New(cfg.NodeID), filepath.Join(cfg.DataDir, "namespace.json"), logger, namespace.WithPeerClockObserver(skew.Observe))
	if err != nil {
		return fmt.Errorf("silod.Run: could not open the namespace state (%w)", err)
	}
	go ns.RunGC(ctx, cfg.TombstoneRetention, 0, logger)
	gossipSubsys, err := newGossipSubsystem(cfg, serverTLS, peerTLS, members, namespaceSyncExt{ns: ns}, logger)
	if err != nil {
		return fmt.Errorf("silod.Run: could not initialise the gossip subsystem (%w)", err)
	}

	// The replication coordinator turns each client Put/Get into a fan-out
	// across the chunk's ring replicas. It dials peers over the same mTLS
	// material gossip uses; peers.Close releases those connections on exit.
	// The scrubber shares the same ring view and peer client to re-form
	// replicas that a write missed or a node loss dropped.
	router := replication.NewRouter(members)
	peers := replication.NewGRPCPeers(credentials.NewTLS(peerTLS), logger)
	defer func() { _ = peers.Close() }()
	coord := newMeteredCoord(replication.New(router, store, peers, cfg.Replication, logger), cfg.NodeID)

	// The exporter renders silod's Prometheus exposition at /metrics, which the
	// observability server hosts on its shared listener. Instrumented
	// components register their instances; each owns its metric names and
	// namespace, and the exporter pulls current values on every scrape.
	exp := exporter.New()
	exp.Register(metrics.Static("silo", metrics.Metric{
		Name:   "build_info",
		Help:   "Build information for the running silod.",
		Kind:   metrics.Gauge,
		Value:  1,
		Labels: [][2]string{{"node", cfg.NodeID}, {"version", version}},
	}))
	exp.Register(skew)
	exp.Register(newStorageMetrics(store, cfg.DataDir, cfg.NodeID))
	exp.Register(coord)
	exp.Register(ns)

	scrubberSubsys := newScrubberSubsystem(cfg, router, store, peers, logger)
	rebalancerSubsys := newRebalancerSubsystem(cfg, members, logger)
	// The scrubber and rebalancer expose replication/capacity metrics; surface
	// them to Prometheus when the concrete subsystem implements metrics.Source
	// (the production ones do; a test fake may not).
	for _, sub := range []subsystem{scrubberSubsys, rebalancerSubsys, gossipSubsys} {
		if src, ok := sub.(metrics.Source); ok {
			exp.Register(src)
		}
	}

	subs := []subsystem{
		newHTTPSubsystem(cfg, version, logger, exp.Handler()),
		newGRPCSubsystem(cfg, serverTLS, tokenAuth, store, coord, ns, members, gossipDrainer(gossipSubsys), version, logger),
		newBootstrapSubsystem(cfg, bootstrapTLS, tokens, transport.NewClientCertMinter(ca), logger),
		gossipSubsys,
		scrubberSubsys,
		rebalancerSubsys,
	}
	if cfg.NBDAddr != "" {
		subs = append(subs, newNBDSubsystem(cfg, ns, coord, logger))
	}
	if cfg.BackupTarget != "" {
		backupSub, err := newBackupSubsystem(cfg, store, ns, logger)
		if err != nil {
			return err
		}
		exp.Register(backupSub)
		subs = append(subs, backupSub)
	}

	type subResult struct {
		name string
		err  error
	}
	results := make(chan subResult, len(subs))
	for _, sub := range subs {
		go func(s subsystem) {
			results <- subResult{name: s.Name(), err: s.Start()}
		}(sub)
	}

	var startupErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received; stopping silod")
	case r := <-results:
		if r.err != nil {
			startupErr = fmt.Errorf("subsystem %q failed before silod was fully running: %w", r.name, r.err)
		} else {
			startupErr = fmt.Errorf("subsystem %q exited cleanly without a shutdown signal; this is unexpected — please file a bug at https://github.com/hyperized/silo/issues", r.name)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	for _, sub := range subs {
		if err := sub.Shutdown(shutdownCtx); err != nil {
			logger.Error("subsystem did not shut down cleanly",
				"subsystem", sub.Name(),
				"err", err,
			)
			if startupErr == nil {
				startupErr = err
			}
		}
	}

	// Drain remaining Start results so the goroutines don't leak.
	remaining := len(subs)
	if startupErr != nil {
		remaining--
	}
	for i := 0; i < remaining; i++ {
		<-results
	}

	if startupErr != nil {
		return startupErr
	}
	logger.Info("silo stopped cleanly")
	return nil
}

type httpSub struct {
	srv *observability.Server
}

func (h *httpSub) Name() string                       { return "http" }
func (h *httpSub) Start() error                       { return h.srv.Start() }
func (h *httpSub) Shutdown(ctx context.Context) error { return h.srv.Shutdown(ctx) }

type grpcSub struct {
	srv *transport.GRPCServer
}

func (g *grpcSub) Name() string                       { return "grpc" }
func (g *grpcSub) Start() error                       { return g.srv.Start() }
func (g *grpcSub) Shutdown(ctx context.Context) error { return g.srv.Shutdown(ctx) }

type bootstrapSub struct {
	srv *transport.BootstrapServer
}

func (b *bootstrapSub) Name() string                       { return "bootstrap" }
func (b *bootstrapSub) Start() error                       { return b.srv.Start() }
func (b *bootstrapSub) Shutdown(ctx context.Context) error { return b.srv.Shutdown(ctx) }

type gossipSub struct {
	srv *gossip.Subsystem
}

func (g *gossipSub) Name() string                       { return "gossip" }
func (g *gossipSub) Start() error                       { return g.srv.Start() }
func (g *gossipSub) Shutdown(ctx context.Context) error { return g.srv.Shutdown(ctx) }

// Drain lets the gossip subsystem satisfy transport.Drainer, so the NodeAdmin
// service can drain this node on operator request.
func (g *gossipSub) Drain() bool { return g.srv.Drain() }

// MetricPrefix and CollectMetrics let the gossip subsystem satisfy
// metrics.Source (member counts and anti-entropy lag) through the wrapper.
func (g *gossipSub) MetricPrefix() string             { return g.srv.MetricPrefix() }
func (g *gossipSub) CollectMetrics() []metrics.Metric { return g.srv.CollectMetrics() }

// gossipDrainer returns the drainer behind the gossip subsystem, or nil when
// the subsystem does not expose draining (a test fake may not), in which case
// the NodeAdmin service is simply not registered.
func gossipDrainer(s subsystem) transport.Drainer {
	if d, ok := s.(transport.Drainer); ok {
		return d
	}
	return nil
}

// announceBootstrap mints a fresh single-use join token and writes the
// operator-facing handshake string to w. The token is generated, hashed,
// persisted, and surfaced exactly once — silod never prints it again,
// even on the next boot. The handshake names the variable, gives the
// fingerprint to pin, and shows the exact siloctl invocation the
// operator should run next, matching the project's "errors are
// instructions" doctrine.
func announceBootstrap(w io.Writer, tokens *bootstraptoken.Store, nodeCert *clustertls.NodeCert, cfg *config.Config) error {
	plaintext, err := tokens.Mint(bootstraptoken.DefaultTTL, bootstraptoken.DefaultSingleUse)
	if err != nil {
		return err
	}
	fp, err := nodeCert.LeafFingerprint()
	if err != nil {
		return err
	}
	fmt.Fprintf(w, `
========================================================================
silo cluster bootstrap token (valid for 24h, single-use)

  token:               %s
  server fingerprint:  %s

Run this on the operator host to claim a client certificate:

  siloctl auth init \
    --token %s \
    --server %s \
    --server-fingerprint %s

To mint another token later, restart silod with SILO_PRINT_BOOTSTRAP_TOKEN=1.
========================================================================

`, plaintext, fp, plaintext, cfg.BootstrapAdvertise, fp)
	return nil
}
