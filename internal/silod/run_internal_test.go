package silod

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/bootstraptoken"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/clustertls"
	"github.com/hyperized/silo/internal/config"
	"github.com/hyperized/silo/internal/crypto"
	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/transport"
)

// testConfig returns a Config with the minimal fields Run needs.
func testConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		NodeID:        "test-node",
		GRPCAddr:      "127.0.0.1:0",
		BootstrapAddr: "127.0.0.1:0",
		GossipAddr:    "127.0.0.1:0",
		HTTPAddr:      "127.0.0.1:0",
		DataDir:       t.TempDir(),
		ChunkSize:     4 * 1024 * 1024,
		Replication:   1,
		KeySource:     config.KeySourceStatic,
		EncryptionKey: make([]byte, crypto.ClusterKeyBytes),
		LogLevel:      "info",
		LogFormat:     "text",
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeSubsystem doubles a subsystem for lifecycle-edge-case tests. It is
// instrumented with hooks so tests can choose whether Start blocks,
// whether it returns an error, and whether Shutdown fails.
type fakeSubsystem struct {
	name        string
	startErr    error
	shutdownErr error
	blockStart  bool

	mu       sync.Mutex
	startCh  chan struct{}
	shutdown chan struct{}
}

func newFakeSubsystem(name string, startErr, shutdownErr error, blocking bool) *fakeSubsystem {
	return &fakeSubsystem{
		name:        name,
		startErr:    startErr,
		shutdownErr: shutdownErr,
		blockStart:  blocking,
		startCh:     make(chan struct{}),
		shutdown:    make(chan struct{}),
	}
}

func (f *fakeSubsystem) Name() string { return f.name }
func (f *fakeSubsystem) Start() error {
	close(f.startCh)
	if !f.blockStart {
		return f.startErr
	}
	<-f.shutdown
	return f.startErr
}

func (f *fakeSubsystem) Shutdown(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.shutdown:
	default:
		close(f.shutdown)
	}
	return f.shutdownErr
}

// installFakes swaps the subsystem factories, the TLS loader, and the
// token-store opener for the duration of a test, restoring the
// originals on cleanup. All four legs are stubbed at once so Run never
// touches the filesystem or real entropy from a unit test.
func installFakes(t *testing.T, httpSub, grpcSub, bootSub, gossipSubFake *fakeSubsystem) {
	t.Helper()
	prevHTTP := newHTTPSubsystem
	prevGRPC := newGRPCSubsystem
	prevBoot := newBootstrapSubsystem
	prevGossip := newGossipSubsystem
	prevTLS := loadClusterTLS
	prevTokens := openTokenStore
	t.Cleanup(func() {
		newHTTPSubsystem = prevHTTP
		newGRPCSubsystem = prevGRPC
		newBootstrapSubsystem = prevBoot
		newGossipSubsystem = prevGossip
		loadClusterTLS = prevTLS
		openTokenStore = prevTokens
	})
	newHTTPSubsystem = func(_ *config.Config, _ string, _ *slog.Logger) subsystem { return httpSub }
	newGRPCSubsystem = func(_ *config.Config, _ *tls.Config, _ chunkstore.Store, _ transport.Coordinator, _ *slog.Logger) subsystem {
		return grpcSub
	}
	newBootstrapSubsystem = func(_ *config.Config, _ *tls.Config, _ transport.TokenRedeemer, _ transport.ClientCertMinter, _ *slog.Logger) subsystem {
		return bootSub
	}
	newGossipSubsystem = func(_ *config.Config, _ *tls.Config, _ *tls.Config, _ *membership.Membership, _ *slog.Logger) (subsystem, error) {
		return gossipSubFake, nil
	}
	loadClusterTLS = stubLoadClusterTLS
	openTokenStore = stubOpenTokenStore
}

// stubOpenTokenStore returns a store rooted under a per-test temp dir so
// the bootstrap-token mint path runs without writing into the host's
// real DataDir. The returned store is empty, so announceBootstrap mints
// (and persists) a token exactly as in production.
func stubOpenTokenStore(_ *config.Config) (*bootstraptoken.Store, error) {
	dir, err := os.MkdirTemp("", "silod-test-tokens-")
	if err != nil {
		return nil, err
	}
	return bootstraptoken.Open(filepath.Join(dir, bootstraptoken.DefaultStoreName()))
}

// stubLoadClusterTLS hands Run a freshly-minted CA + node cert so the
// production code path (which reads files) is bypassed for unit tests.
// Tests that want to exercise loader-failure paths swap loadClusterTLS
// directly instead of calling installFakes.
func stubLoadClusterTLS(cfg *config.Config) (*clustertls.CA, *clustertls.NodeCert, error) {
	caPEM, keyPEM, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		return nil, nil, err
	}
	ca, err := clustertls.LoadCA(caPEM, keyPEM)
	if err != nil {
		return nil, nil, err
	}
	nc, err := clustertls.MintNodeCert(ca, cfg.NodeID, nil, nil, time.Hour)
	if err != nil {
		return nil, nil, err
	}
	return ca, nc, nil
}

func TestRun_NilConfig(t *testing.T) {
	err := Run(context.Background(), nil, discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "cfg is nil") {
		t.Errorf("expected nil-cfg error, got %v", err)
	}
}

func TestRun_NilLogger(t *testing.T) {
	err := Run(context.Background(), testConfig(t), nil, io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "logger is nil") {
		t.Errorf("expected nil-logger error, got %v", err)
	}
}

func TestRun_BadEncryptionKeyLength(t *testing.T) {
	cfg := testConfig(t)
	cfg.EncryptionKey = []byte("too-short")
	err := Run(context.Background(), cfg, discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "encryption key") {
		t.Errorf("expected encryption-key error, got %v", err)
	}
}

func TestRun_TLSLoadFailure(t *testing.T) {
	prevTLS := loadClusterTLS
	t.Cleanup(func() { loadClusterTLS = prevTLS })
	loadClusterTLS = func(*config.Config) (*clustertls.CA, *clustertls.NodeCert, error) {
		return nil, nil, errors.New("simulated CA load failure")
	}
	err := Run(context.Background(), testConfig(t), discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "simulated CA load failure") {
		t.Errorf("expected TLS load failure, got %v", err)
	}
}

func TestRun_NilAnnounceFallsBackToDiscard(t *testing.T) {
	// Passing announce=nil must not panic — Run substitutes io.Discard.
	httpSub := newFakeSubsystem("http", nil, nil, true)
	grpcSub := newFakeSubsystem("grpc", nil, nil, true)
	bootSub := newFakeSubsystem("bootstrap", nil, nil, true)
	gossipSub := newFakeSubsystem("gossip", nil, nil, true)
	installFakes(t, httpSub, grpcSub, bootSub, gossipSub)

	cfg := testConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, discardLogger(), nil, "v0") }()
	<-httpSub.startCh
	<-grpcSub.startCh
	<-bootSub.startCh
	<-gossipSub.startCh
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s")
	}
}

func TestRun_TokenStoreOpenFails(t *testing.T) {
	prev := openTokenStore
	t.Cleanup(func() { openTokenStore = prev })
	prevTLS := loadClusterTLS
	t.Cleanup(func() { loadClusterTLS = prevTLS })
	loadClusterTLS = stubLoadClusterTLS
	openTokenStore = func(*config.Config) (*bootstraptoken.Store, error) {
		return nil, errors.New("simulated token-store open failure")
	}
	err := Run(context.Background(), testConfig(t), discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "bootstrap-token store") {
		t.Errorf("got %v, want token-store open error", err)
	}
}

func TestRun_AnnounceFailureSurfacesActionable(t *testing.T) {
	// Force announceBootstrap to fail by handing back a token store that
	// cannot persist (a directory at the temp-rename target). The error
	// must be wrapped with the actionable prefix so operators know which
	// step failed.
	prevTLS := loadClusterTLS
	t.Cleanup(func() { loadClusterTLS = prevTLS })
	loadClusterTLS = stubLoadClusterTLS

	prevTokens := openTokenStore
	t.Cleanup(func() { openTokenStore = prevTokens })
	openTokenStore = func(_ *config.Config) (*bootstraptoken.Store, error) {
		dir := t.TempDir()
		// Pre-place a directory at the would-be .tmp path so Mint's
		// persist step trips. The store itself opens cleanly because the
		// real file doesn't exist yet.
		path := filepath.Join(dir, bootstraptoken.DefaultStoreName())
		if err := os.MkdirAll(path+".tmp", 0o700); err != nil {
			t.Fatalf("seed wedge: %v", err)
		}
		return bootstraptoken.Open(path)
	}

	err := Run(context.Background(), testConfig(t), discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "inaugural bootstrap token") {
		t.Errorf("got %v, want inaugural-token wrapper", err)
	}
}

func TestRun_MembershipInitFailure(t *testing.T) {
	prevTLS := loadClusterTLS
	t.Cleanup(func() { loadClusterTLS = prevTLS })
	loadClusterTLS = stubLoadClusterTLS
	prev := newMembership
	t.Cleanup(func() { newMembership = prev })
	newMembership = func(string, string, string) (*membership.Membership, error) {
		return nil, errors.New("simulated membership init failure")
	}
	err := Run(context.Background(), testConfig(t), discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "membership table") {
		t.Errorf("got %v, want membership-init failure", err)
	}
}

func TestRun_GossipSubsystemRejectsSelfSeed(t *testing.T) {
	// Production-shaped test: when SILO_SEEDS contains the node's own
	// gossip address, gossip.New refuses to start. Run must surface
	// that as an instruction-shaped error before any subsystem boots.
	prevTLS := loadClusterTLS
	t.Cleanup(func() { loadClusterTLS = prevTLS })
	loadClusterTLS = stubLoadClusterTLS

	cfg := testConfig(t)
	cfg.GossipAddr = "127.0.0.1:7100"
	cfg.Seeds = []string{"127.0.0.1:7100"}
	err := Run(context.Background(), cfg, discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "own identity") {
		t.Errorf("got %v, want self-seed error", err)
	}
}

func TestRun_GossipSubsystemInitFailure(t *testing.T) {
	prevTLS := loadClusterTLS
	t.Cleanup(func() { loadClusterTLS = prevTLS })
	loadClusterTLS = stubLoadClusterTLS

	prevGossip := newGossipSubsystem
	t.Cleanup(func() { newGossipSubsystem = prevGossip })
	newGossipSubsystem = func(_ *config.Config, _ *tls.Config, _ *tls.Config, _ *membership.Membership, _ *slog.Logger) (subsystem, error) {
		return nil, errors.New("simulated gossip init failure")
	}
	err := Run(context.Background(), testConfig(t), discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "gossip subsystem") {
		t.Errorf("got %v, want gossip-init failure", err)
	}
}

func TestRun_TLSServerConfigFailure(t *testing.T) {
	prevTLS := loadClusterTLS
	t.Cleanup(func() { loadClusterTLS = prevTLS })
	// Return a CA struct with no Cert so ServerConfig rejects it. This
	// exercises the "could not build the gRPC TLS server config" branch.
	loadClusterTLS = func(*config.Config) (*clustertls.CA, *clustertls.NodeCert, error) {
		return &clustertls.CA{}, nil, nil
	}
	err := Run(context.Background(), testConfig(t), discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "gRPC TLS server config") {
		t.Errorf("expected TLS server-config failure, got %v", err)
	}
}

func TestRun_ChunkStoreInitFailure(t *testing.T) {
	prev := newChunkStore
	t.Cleanup(func() { newChunkStore = prev })
	newChunkStore = func(*config.Config, *crypto.Cipher) (chunkstore.Store, error) {
		return nil, errors.New("simulated mount failure")
	}
	err := Run(context.Background(), testConfig(t), discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "chunk store") {
		t.Errorf("expected chunk-store error, got %v", err)
	}
}

func TestRun_GracefulShutdownOnContextCancel(t *testing.T) {
	httpSub := newFakeSubsystem("http", nil, nil, true)
	grpcSub := newFakeSubsystem("grpc", nil, nil, true)
	bootSub := newFakeSubsystem("bootstrap", nil, nil, true)
	gossipSub := newFakeSubsystem("gossip", nil, nil, true)
	installFakes(t, httpSub, grpcSub, bootSub, gossipSub)

	cfg := testConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, discardLogger(), io.Discard, "v0") }()

	// Wait until every subsystem is actually started.
	<-httpSub.startCh
	<-grpcSub.startCh
	<-bootSub.startCh
	<-gossipSub.startCh

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil after cancel", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s of ctx cancel")
	}
}

func TestRun_HTTPSubsystemStartFails(t *testing.T) {
	httpSub := newFakeSubsystem("http", errors.New("simulated bind failure"), nil, false)
	grpcSub := newFakeSubsystem("grpc", nil, nil, true)
	bootSub := newFakeSubsystem("bootstrap", nil, nil, true)
	gossipSub := newFakeSubsystem("gossip", nil, nil, true)
	installFakes(t, httpSub, grpcSub, bootSub, gossipSub)

	err := Run(context.Background(), testConfig(t), discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "http") || !strings.Contains(err.Error(), "before silod was fully running") {
		t.Errorf("expected subsystem-failure error naming http, got %v", err)
	}
}

func TestRun_SubsystemExitsCleanlyWithoutSignal(t *testing.T) {
	// blockStart=false + startErr=nil simulates Start returning early
	// without a Shutdown ever being called. Run should treat this as
	// unexpected and surface a "please file a bug" error.
	httpSub := newFakeSubsystem("http", nil, nil, false)
	grpcSub := newFakeSubsystem("grpc", nil, nil, true)
	bootSub := newFakeSubsystem("bootstrap", nil, nil, true)
	gossipSub := newFakeSubsystem("gossip", nil, nil, true)
	installFakes(t, httpSub, grpcSub, bootSub, gossipSub)

	err := Run(context.Background(), testConfig(t), discardLogger(), io.Discard, "v0")
	if err == nil || !strings.Contains(err.Error(), "without a shutdown signal") {
		t.Errorf("expected unexpected-exit error, got %v", err)
	}
}

func TestRun_ShutdownErrorIsLoggedAndReturned(t *testing.T) {
	httpSub := newFakeSubsystem("http", nil, errors.New("simulated http shutdown failure"), true)
	grpcSub := newFakeSubsystem("grpc", nil, nil, true)
	bootSub := newFakeSubsystem("bootstrap", nil, nil, true)
	gossipSub := newFakeSubsystem("gossip", nil, nil, true)
	installFakes(t, httpSub, grpcSub, bootSub, gossipSub)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, testConfig(t), discardLogger(), io.Discard, "v0") }()

	<-httpSub.startCh
	<-grpcSub.startCh
	<-bootSub.startCh
	<-gossipSub.startCh
	cancel()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "simulated http shutdown failure") {
			t.Errorf("expected shutdown error, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s")
	}
}

func TestRun_ServesHealthAndMetricsWhileRunning(t *testing.T) {
	// End-to-end smoke through Run: real HTTP listener on an ephemeral
	// port, real (in-memory chunk store underneath), then cancel.
	// Swap the TLS loader so we don't need a CA file on disk for the
	// happy-path smoke test.
	prevTLS := loadClusterTLS
	t.Cleanup(func() { loadClusterTLS = prevTLS })
	loadClusterTLS = stubLoadClusterTLS

	cfg := testConfig(t)
	port := freeTCPPort(t)
	cfg.HTTPAddr = "127.0.0.1:" + port
	cfg.GRPCAddr = "127.0.0.1:" + freeTCPPort(t)
	cfg.BootstrapAddr = "127.0.0.1:" + freeTCPPort(t)
	cfg.GossipAddr = "127.0.0.1:" + freeTCPPort(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, discardLogger(), io.Discard, "v0-int") }()

	url := "http://" + cfg.HTTPAddr
	if !waitForServer(url+"/healthz", 2*time.Second) {
		cancel()
		<-done
		t.Fatal("server did not become reachable within 2s")
	}

	resp, err := http.Get(url + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), `"node":"test-node"`) {
		t.Errorf("/healthz response: missing node id, got %q", body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v after cancel, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not shut down within 3s")
	}
}

func TestWaitForCA_ReturnsImmediatelyWhenCertPresent(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(certPath, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	start := time.Now()
	if err := waitForCA(certPath, filepath.Join(dir, "ca.key")); err != nil {
		t.Fatalf("waitForCA: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("waitForCA took %s, should return immediately when cert present", elapsed)
	}
}

func TestWaitForCA_TimeoutErrorIsActionable(t *testing.T) {
	// Crank the timeout and poll down so the test runs in milliseconds.
	prevTimeout := clusterCAJoinTimeout
	prevPoll := clusterCAJoinPoll
	t.Cleanup(func() {
		clusterCAJoinTimeout = prevTimeout
		clusterCAJoinPoll = prevPoll
	})
	clusterCAJoinTimeoutSet(20 * time.Millisecond)
	clusterCAJoinPoll = 5 * time.Millisecond

	dir := t.TempDir()
	err := waitForCA(filepath.Join(dir, "ca.crt"), filepath.Join(dir, "ca.key"))
	if err == nil || !strings.Contains(err.Error(), "shared cluster CA to appear") {
		t.Errorf("got %v, want timeout error", err)
	}
	if !strings.Contains(err.Error(), "seed node") {
		t.Errorf("error should mention the seed node, got %q", err)
	}
}

func TestWaitForCA_KeyArrivesBeforeCert(t *testing.T) {
	// Race direction: key flushed first, cert delayed. waitForCA should
	// keep polling until the cert lands rather than returning early.
	prevTimeout := clusterCAJoinTimeout
	prevPoll := clusterCAJoinPoll
	t.Cleanup(func() {
		clusterCAJoinTimeout = prevTimeout
		clusterCAJoinPoll = prevPoll
	})
	clusterCAJoinTimeoutSet(500 * time.Millisecond)
	clusterCAJoinPoll = 5 * time.Millisecond

	dir := t.TempDir()
	certPath := filepath.Join(dir, "ca.crt")
	keyPath := filepath.Join(dir, "ca.key")
	if err := os.WriteFile(keyPath, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed key: %v", err)
	}
	// Schedule the cert to land after a brief delay.
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = os.WriteFile(certPath, []byte("x"), 0o600)
	}()
	if err := waitForCA(certPath, keyPath); err != nil {
		t.Errorf("waitForCA: %v", err)
	}
}

// clusterCAJoinTimeoutSet exists to keep the call sites readable —
// reassigning a package var inline mixes test-only mutation with the
// test logic. Each call is paired with a t.Cleanup that restores the
// prior value.
func clusterCAJoinTimeoutSet(d time.Duration) {
	clusterCAJoinTimeout = d
}

func TestDefaultLoadClusterTLS_ExternalCAWaitsThenLoads(t *testing.T) {
	// When SILO_TLS_CA_CERT is set explicitly, silod must NOT self-mint
	// on first boot — it should wait for the seed node to publish the
	// CA into the shared volume. We simulate that race by writing the
	// CA into place asynchronously while defaultLoadClusterTLS is running.
	prevTimeout := clusterCAJoinTimeout
	prevPoll := clusterCAJoinPoll
	t.Cleanup(func() {
		clusterCAJoinTimeout = prevTimeout
		clusterCAJoinPoll = prevPoll
	})
	clusterCAJoinTimeoutSet(2 * time.Second)
	clusterCAJoinPoll = 5 * time.Millisecond

	dir := t.TempDir()
	caPath := filepath.Join(dir, "shared-ca.crt")
	keyPath := filepath.Join(dir, "shared-ca.key")
	caPEM, keyPEM, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = os.WriteFile(caPath, caPEM, 0o600)
		_ = os.WriteFile(keyPath, keyPEM, 0o600)
	}()

	cfg := testConfig(t)
	cfg.CACertPath = caPath
	cfg.CAKeyPath = keyPath
	cfg.CAExternal = true
	cfg.DataDir = filepath.Join(dir, "node-state")
	ca, nc, err := defaultLoadClusterTLS(cfg)
	if err != nil {
		t.Fatalf("defaultLoadClusterTLS: %v", err)
	}
	if ca == nil || nc == nil {
		t.Fatal("expected CA + node cert to be returned")
	}
}

func TestDefaultLoadClusterTLS_ExternalCASeedMintsImmediately(t *testing.T) {
	// CAExternal + CASeed means "I am the seed; mint into the shared
	// path on first boot". No waiting, no error, even when the shared
	// path is empty.
	dir := t.TempDir()
	cfg := testConfig(t)
	cfg.CACertPath = filepath.Join(dir, "shared-ca.crt")
	cfg.CAKeyPath = filepath.Join(dir, "shared-ca.key")
	cfg.CAExternal = true
	cfg.CASeed = true
	cfg.DataDir = filepath.Join(dir, "node-state")

	ca, nc, err := defaultLoadClusterTLS(cfg)
	if err != nil {
		t.Fatalf("defaultLoadClusterTLS: %v", err)
	}
	if ca == nil || ca.Key == nil {
		t.Fatal("seed mint should produce a CA with both cert and key")
	}
	if nc == nil {
		t.Fatal("seed mint should produce a node cert")
	}
	if _, statErr := os.Stat(cfg.CACertPath); statErr != nil {
		t.Errorf("shared CA cert not on disk: %v", statErr)
	}
}

func TestDefaultLoadClusterTLS_ExternalCATimeout(t *testing.T) {
	prevTimeout := clusterCAJoinTimeout
	prevPoll := clusterCAJoinPoll
	t.Cleanup(func() {
		clusterCAJoinTimeout = prevTimeout
		clusterCAJoinPoll = prevPoll
	})
	clusterCAJoinTimeoutSet(20 * time.Millisecond)
	clusterCAJoinPoll = 5 * time.Millisecond

	dir := t.TempDir()
	cfg := testConfig(t)
	cfg.CACertPath = filepath.Join(dir, "never.crt")
	cfg.CAKeyPath = filepath.Join(dir, "never.key")
	cfg.CAExternal = true

	_, _, err := defaultLoadClusterTLS(cfg)
	if err == nil || !strings.Contains(err.Error(), "shared cluster CA") {
		t.Errorf("got %v, want timeout error", err)
	}
}

func TestDefaultLoadClusterTLS_SelfMintsOnFirstBoot(t *testing.T) {
	// Neither CA file present: silod is expected to mint its own pair
	// into DataDir and return a fully-loaded CA + node cert.
	dir := t.TempDir()
	cfg := testConfig(t)
	cfg.DataDir = dir
	cfg.CACertPath = filepath.Join(dir, "ca.crt")
	cfg.CAKeyPath = filepath.Join(dir, "ca.key")

	ca, nc, err := defaultLoadClusterTLS(cfg)
	if err != nil {
		t.Fatalf("defaultLoadClusterTLS: %v", err)
	}
	if ca == nil || ca.Cert == nil || ca.Key == nil {
		t.Fatal("expected a fully-minted CA after first boot")
	}
	if nc == nil || len(nc.CertPEM) == 0 || len(nc.KeyPEM) == 0 {
		t.Fatal("expected a node cert after first boot")
	}
	// Files should be on disk for the next boot to reuse.
	for _, name := range []string{"ca.crt", "ca.key", "node.crt", "node.key"} {
		if _, statErr := os.Stat(filepath.Join(dir, name)); statErr != nil {
			t.Errorf("%s not persisted: %v", name, statErr)
		}
	}
}

func TestDefaultLoadClusterTLS_SelfMintCertReadFailure(t *testing.T) {
	// Pre-create the parent dir with the CA cert as a directory so the
	// self-mint short-circuits (fileExists returns true for the dir),
	// then the subsequent ReadFile fails — exercising the read-error
	// branch separately from the write-error path.
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "ca.crt"), 0o700); err != nil {
		t.Fatalf("seed ca.crt as dir: %v", err)
	}
	cfg := testConfig(t)
	cfg.DataDir = dir
	cfg.CACertPath = filepath.Join(dir, "ca.crt")
	cfg.CAKeyPath = filepath.Join(dir, "ca.key")
	_, _, err := defaultLoadClusterTLS(cfg)
	if err == nil || !strings.Contains(err.Error(), "cluster CA certificate") {
		t.Errorf("got %v, want CA-read error", err)
	}
}

func TestDefaultLoadClusterTLS_MissingCAKey(t *testing.T) {
	// Real CA cert on disk + ca.key as a directory: the load path runs
	// (because fileExists for both is true) and the ReadFile of ca.key
	// fails, surfacing the actionable error.
	dir := t.TempDir()
	caPEM, _, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write ca.crt: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "ca.key"), 0o700); err != nil {
		t.Fatalf("seed ca.key as dir: %v", err)
	}
	cfg := testConfig(t)
	cfg.DataDir = dir
	cfg.CACertPath = caPath
	cfg.CAKeyPath = filepath.Join(dir, "ca.key")
	_, _, err = defaultLoadClusterTLS(cfg)
	if err == nil || !strings.Contains(err.Error(), "cluster CA key") {
		t.Errorf("got %v, want missing-CA-key error", err)
	}
}

func TestDefaultLoadClusterTLS_MintMkdirFailure(t *testing.T) {
	// Place a regular file where the data directory should be so
	// MkdirAll inside mintClusterCA can't create the parent. Exercises
	// the actionable error for that branch.
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte{0}, 0o600); err != nil {
		t.Fatalf("seed blocker: %v", err)
	}
	cfg := testConfig(t)
	cfg.DataDir = filepath.Join(blocker, "subdir")
	cfg.CACertPath = filepath.Join(cfg.DataDir, "ca.crt")
	cfg.CAKeyPath = filepath.Join(cfg.DataDir, "ca.key")
	_, _, err := defaultLoadClusterTLS(cfg)
	if err == nil || !strings.Contains(err.Error(), "directory for the cluster CA") {
		t.Errorf("got %v, want mkdir-failure error mentioning the CA directory", err)
	}
}

func TestDefaultLoadClusterTLS_MintGenerateCAFailure(t *testing.T) {
	// Swap the GenerateCA seam so the mint path surfaces an error
	// without depending on the host's entropy state.
	prev := generateCA
	t.Cleanup(func() { generateCA = prev })
	generateCA = func(string, time.Duration) (cert, key []byte, err error) {
		return nil, nil, errors.New("simulated generation failure")
	}

	prevExists := fileExists
	t.Cleanup(func() { fileExists = prevExists })
	fileExists = func(string) bool { return false }

	dir := t.TempDir()
	cfg := testConfig(t)
	cfg.DataDir = dir
	cfg.CACertPath = filepath.Join(dir, "ca.crt")
	cfg.CAKeyPath = filepath.Join(dir, "ca.key")
	_, _, err := defaultLoadClusterTLS(cfg)
	if err == nil || !strings.Contains(err.Error(), "simulated generation failure") {
		t.Errorf("got %v, want GenerateCA failure", err)
	}
}

func TestDefaultLoadClusterTLS_MintCertWriteFailure(t *testing.T) {
	// Place a directory at the CA cert path before minting starts; the
	// mint path's WriteFile(certPath, ...) fails with EISDIR. Stub
	// fileExists so the mint branch runs instead of the load branch.
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "ca.crt"), 0o700); err != nil {
		t.Fatalf("seed ca.crt as dir: %v", err)
	}
	prev := fileExists
	t.Cleanup(func() { fileExists = prev })
	fileExists = func(string) bool { return false }

	cfg := testConfig(t)
	cfg.DataDir = dir
	cfg.CACertPath = filepath.Join(dir, "ca.crt")
	cfg.CAKeyPath = filepath.Join(dir, "ca.key")
	_, _, err := defaultLoadClusterTLS(cfg)
	if err == nil || !strings.Contains(err.Error(), "cluster CA cert") {
		t.Errorf("got %v, want cert-write-failure error", err)
	}
}

func TestDefaultLoadClusterTLS_MintKeyWriteFailure(t *testing.T) {
	// Place a directory at the CA key path before minting starts. The
	// mint path's WriteFile(keyPath, ...) then fails with EISDIR. We
	// stub fileExists to claim neither file is on disk so the mint
	// branch actually runs (without the stub, fileExists(ca.key) would
	// be true and we'd skip past it into the load branch instead).
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "ca.key"), 0o700); err != nil {
		t.Fatalf("seed ca.key as dir: %v", err)
	}
	prev := fileExists
	t.Cleanup(func() { fileExists = prev })
	fileExists = func(string) bool { return false }

	cfg := testConfig(t)
	cfg.DataDir = dir
	cfg.CACertPath = filepath.Join(dir, "ca.crt")
	cfg.CAKeyPath = filepath.Join(dir, "ca.key")
	_, _, err := defaultLoadClusterTLS(cfg)
	if err == nil || !strings.Contains(err.Error(), "cluster CA key") {
		t.Errorf("got %v, want key-write-failure error", err)
	}
}

func TestDefaultOpenTokenStore_UsesDataDir(t *testing.T) {
	// defaultOpenTokenStore is the seam silod.Run hits in production;
	// confirm it returns a usable store at the canonical path.
	cfg := testConfig(t)
	store, err := defaultOpenTokenStore(cfg)
	if err != nil {
		t.Fatalf("defaultOpenTokenStore: %v", err)
	}
	if _, err := store.Mint(0, true); err != nil {
		t.Fatalf("Mint: %v", err)
	}
	want := filepath.Join(cfg.DataDir, "bootstrap-tokens.json")
	if _, statErr := os.Stat(want); statErr != nil {
		t.Errorf("token store not at %s: %v", want, statErr)
	}
}

func TestAnnounceBootstrap_PrintsActionableHandshake(t *testing.T) {
	// announceBootstrap is what the operator sees on first boot. Verify
	// the output names the variable, gives the fingerprint to pin, and
	// shows the exact siloctl command the operator should run next.
	cfg := testConfig(t)
	cfg.BootstrapAddr = "127.0.0.1:7001"
	cfg.BootstrapAdvertise = "127.0.0.1:7001"
	tokens, err := bootstraptoken.Open(filepath.Join(t.TempDir(), bootstraptoken.DefaultStoreName()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, _, nodeCert := newBootstrapTestPair(t, cfg.NodeID)

	var buf strings.Builder
	if err := announceBootstrap(&buf, tokens, nodeCert, cfg); err != nil {
		t.Fatalf("announceBootstrap: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"siloctl auth init",
		"--token",
		"--server 127.0.0.1:7001",
		"--server-fingerprint sha256:",
		"SILO_PRINT_BOOTSTRAP_TOKEN",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("announce output missing %q; got:\n%s", want, out)
		}
	}
	if got := tokens.Tokens(); len(got) != 1 {
		t.Errorf("Mint should leave one token in the store, got %d", len(got))
	}
}

func TestAnnounceBootstrap_MintFailure(t *testing.T) {
	// Force the underlying Store.Mint to fail by pointing the store at
	// a path whose .tmp sibling is a directory. announceBootstrap must
	// surface the persist failure unwrapped — the caller adds the
	// "inaugural bootstrap token" prefix.
	dir := t.TempDir()
	path := filepath.Join(dir, bootstraptoken.DefaultStoreName())
	if err := os.MkdirAll(path+".tmp", 0o700); err != nil {
		t.Fatalf("seed wedge: %v", err)
	}
	tokens, err := bootstraptoken.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, _, nodeCert := newBootstrapTestPair(t, "n1")
	cfg := testConfig(t)
	if err := announceBootstrap(io.Discard, tokens, nodeCert, cfg); err == nil {
		t.Error("announceBootstrap should fail when Mint cannot persist")
	}
}

func TestAnnounceBootstrap_FingerprintFailure(t *testing.T) {
	// Corrupt the node cert PEM so LeafFingerprint fails after Mint
	// succeeds — proves the second error path is wired.
	tokens, err := bootstraptoken.Open(filepath.Join(t.TempDir(), bootstraptoken.DefaultStoreName()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, _, nodeCert := newBootstrapTestPair(t, "n1")
	nodeCert.CertPEM = []byte("garbage")
	cfg := testConfig(t)
	if err := announceBootstrap(io.Discard, tokens, nodeCert, cfg); err == nil || !strings.Contains(err.Error(), "no certificate block") {
		t.Errorf("got %v, want fingerprint failure", err)
	}
}

// newBootstrapTestPair mints a fresh CA + node cert without any of the
// production seams the rest of the suite touches. Used by the announce
// tests so they don't depend on stubLoadClusterTLS's call ordering.
func newBootstrapTestPair(t *testing.T, nodeID string) (*clustertls.CA, []byte, *clustertls.NodeCert) {
	t.Helper()
	caPEM, keyPEM, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, err := clustertls.LoadCA(caPEM, keyPEM)
	if err != nil {
		t.Fatalf("LoadCA: %v", err)
	}
	nc, err := clustertls.MintNodeCert(ca, nodeID, nil, nil, time.Hour)
	if err != nil {
		t.Fatalf("MintNodeCert: %v", err)
	}
	return ca, caPEM, nc
}

func TestFileExists(t *testing.T) {
	// Hits both branches of the default fileExists helper: empty path
	// is always false; a real path returns true.
	if fileExists("") {
		t.Error("fileExists(\"\"): got true, want false")
	}
	f := filepath.Join(t.TempDir(), "real")
	if err := os.WriteFile(f, []byte{0}, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !fileExists(f) {
		t.Errorf("fileExists(%q): got false, want true", f)
	}
	if fileExists(filepath.Join(t.TempDir(), "no-such-file")) {
		t.Error("fileExists on missing path: got true, want false")
	}
}

func TestDefaultLoadClusterTLS_GarbageCA(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caPath, []byte("this is not PEM"), 0o600); err != nil {
		t.Fatalf("write ca.crt: %v", err)
	}
	cfg := testConfig(t)
	cfg.CACertPath = caPath
	_, _, err := defaultLoadClusterTLS(cfg)
	if err == nil || !strings.Contains(err.Error(), "cluster CA") {
		t.Errorf("got %v, want LoadCA failure, got %v", err, err)
	}
}

func TestDefaultLoadClusterTLS_NodeMintFails(t *testing.T) {
	// Write a valid cert-only CA. LoadOrMintNode will see no key, no
	// pre-existing node files, and refuse to mint — exercising the
	// "no CA key, no node cert" branch inside LoadOrMintNode that
	// defaultLoadClusterTLS propagates.
	dir := t.TempDir()
	caPEM, _, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write ca.crt: %v", err)
	}
	cfg := testConfig(t)
	cfg.CACertPath = caPath
	cfg.DataDir = filepath.Join(dir, "node-state")
	_, _, err = defaultLoadClusterTLS(cfg)
	if err == nil || !strings.Contains(err.Error(), "SILO_TLS_CA_KEY") {
		t.Errorf("got %v, want node-mint failure", err)
	}
}

func TestDefaultLoadClusterTLS_HappyPath(t *testing.T) {
	dir := t.TempDir()
	caPEM, keyPEM, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	caPath := filepath.Join(dir, "ca.crt")
	caKeyPath := filepath.Join(dir, "ca.key")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write ca.crt: %v", err)
	}
	if err := os.WriteFile(caKeyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write ca.key: %v", err)
	}

	cfg := testConfig(t)
	cfg.CACertPath = caPath
	cfg.CAKeyPath = caKeyPath
	cfg.DataDir = filepath.Join(dir, "node-state")
	ca, nc, err := defaultLoadClusterTLS(cfg)
	if err != nil {
		t.Fatalf("defaultLoadClusterTLS: %v", err)
	}
	if ca == nil || ca.Cert == nil || ca.Key == nil {
		t.Error("CA missing material after load")
	}
	if nc == nil || len(nc.CertPEM) == 0 || len(nc.KeyPEM) == 0 {
		t.Error("node cert missing material")
	}
	// Second call should reuse the on-disk pair.
	_, nc2, err := defaultLoadClusterTLS(cfg)
	if err != nil {
		t.Fatalf("defaultLoadClusterTLS second call: %v", err)
	}
	if string(nc.CertPEM) != string(nc2.CertPEM) {
		t.Error("second call should reuse the on-disk node cert")
	}
}

// freeTCPPort returns a port that was free at call time. Inherently racy
// but adequate for a single-process integration test.
func freeTCPPort(t *testing.T) string {
	t.Helper()
	ln, err := listenTCP()
	if err != nil {
		t.Fatalf("could not find a free TCP port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	i := strings.LastIndex(addr, ":")
	return addr[i+1:]
}

func waitForServer(url string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 300 {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
