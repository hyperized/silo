package gossip_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/gossip"
	"github.com/hyperized/silo/internal/membership"
)

// sharedTLS holds one self-signed cert that every node in a test uses
// for both server and client sides. It models the simplest version of
// silod's symmetric mTLS without pulling clustertls into a test
// dependency.
type sharedTLS struct {
	cert tls.Certificate
	pool *x509.CertPool
}

func newSharedTLS(t *testing.T) *sharedTLS {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "gossip-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	pool := x509.NewCertPool()
	parsed, _ := x509.ParseCertificate(der)
	pool.AddCert(parsed)
	return &sharedTLS{
		cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv},
		pool: pool,
	}
}

func (s *sharedTLS) server() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{s.cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    s.pool,
		MinVersion:   tls.VersionTLS13,
	}
}

func (s *sharedTLS) client() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{s.cert},
		RootCAs:      s.pool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS13,
	}
}

// startNode boots a gossip Subsystem on 127.0.0.1:0 with the supplied
// seeds. The returned cleanup function tears the subsystem down. The
// id and address are set up so test peers can recognise each other.
func startNode(t *testing.T, shared *sharedTLS, id string, seeds []string) (*gossip.Subsystem, string, func()) {
	return startNodeExt(t, shared, id, seeds, nil)
}

func startNodeExt(t *testing.T, shared *sharedTLS, id string, seeds []string, ext gossip.SyncExtension) (*gossip.Subsystem, string, func()) {
	t.Helper()
	// Advertise a distinct, recognisable data-plane address per node so
	// tests can assert it propagates over gossip. It is never dialed here
	// (no data plane runs), only gossiped.
	m, err := membership.New(id, "127.0.0.1:0", id+":7000")
	if err != nil {
		t.Fatalf("membership.New: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := gossip.New(m, gossip.Options{
		Addr:                "127.0.0.1:0",
		Seeds:               seeds,
		ServerTLS:           shared.server(),
		ClientTLS:           shared.client(),
		ProbeInterval:       30 * time.Millisecond,
		ProbeTimeout:        100 * time.Millisecond,
		SuspectTimeout:      200 * time.Millisecond,
		DeadRetention:       500 * time.Millisecond,
		AntiEntropyInterval: 50 * time.Millisecond,
		Timeout:             200 * time.Millisecond,
		PiggybackCap:        16,
		IndirectK:           2,
		Extension:           ext,
	}, logger)
	if err != nil {
		t.Fatalf("gossip.New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Start() }()
	for s.Addr() == "" {
		time.Sleep(2 * time.Millisecond)
	}
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
		<-done
	}
	return s, s.Addr(), cleanup
}

// TestGossip_TwoNodesConverge runs two real gossip subsystems on
// ephemeral ports; one is seeded with the other. Within a few probe
// intervals both tables should hold an Alive entry for each peer.
func TestGossip_TwoNodesConverge(t *testing.T) {
	shared := newSharedTLS(t)
	a, aAddr, aClose := startNode(t, shared, "a", nil)
	defer aClose()

	b, _, bClose := startNode(t, shared, "b", []string{aAddr})
	defer bClose()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		members := a.Members().Members()
		for _, n := range members {
			if n.ID == "b" && n.State == membership.StateAlive {
				// b's data-plane address must arrive with its membership
				// entry — placement resolves node ids to this value.
				if n.DataAddress != "b:7000" {
					t.Fatalf("a learned b but not its data address: got %q, want b:7000", n.DataAddress)
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("a did not see b as Alive within 3s; a=%+v b=%+v", a.Members().Members(), b.Members().Members())
}

// setExt is a tiny mergeable SyncExtension — a grow-only set of strings —
// used to prove the gossip anti-entropy exchange ferries and converges an
// extension's state across nodes.
type setExt struct {
	mu    sync.Mutex
	items map[string]bool
}

func newSetExt(seed string) *setExt { return &setExt{items: map[string]bool{seed: true}} }

func (e *setExt) LocalState() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	keys := make([]string, 0, len(e.items))
	for k := range e.items {
		keys = append(keys, k)
	}
	return json.Marshal(keys)
}

func (e *setExt) MergeRemote(b []byte) error {
	var keys []string
	if err := json.Unmarshal(b, &keys); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, k := range keys {
		e.items[k] = true
	}
	return nil
}

func (e *setExt) has(k string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.items[k]
}

// TestGossip_SyncExtensionConverges proves the anti-entropy exchange
// carries an extension's state in both directions: two nodes seeded with
// different data end up holding the union once they have synced.
func TestGossip_SyncExtensionConverges(t *testing.T) {
	shared := newSharedTLS(t)
	extA := newSetExt("a-data")
	extB := newSetExt("b-data")

	_, aAddr, aClose := startNodeExt(t, shared, "a", nil, extA)
	defer aClose()
	_, _, bClose := startNodeExt(t, shared, "b", []string{aAddr}, extB)
	defer bClose()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if extA.has("b-data") && extB.has("a-data") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("sync extensions did not converge within 3s")
}

// TestGossip_DeadNodeDetected starts two nodes, shuts the second one
// down, and confirms the first transitions it to Dead within
// SuspectTimeout + ProbeInterval + slack.
func TestGossip_DeadNodeDetected(t *testing.T) {
	shared := newSharedTLS(t)
	a, aAddr, aClose := startNode(t, shared, "alpha", nil)
	defer aClose()

	_, _, bClose := startNode(t, shared, "beta", []string{aAddr})

	// Wait for convergence.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n, ok := a.Members().Lookup("beta"); ok && n.State == membership.StateAlive {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n, ok := a.Members().Lookup("beta"); !ok || n.State != membership.StateAlive {
		t.Fatalf("convergence: got %+v, ok=%v", n, ok)
	}

	// Kill beta.
	bClose()

	// Within probe + suspect window (200 + 30ms + slack) beta should
	// be Dead from alpha's perspective.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n, ok := a.Members().Lookup("beta"); ok && n.State == membership.StateDead {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	n, _ := a.Members().Lookup("beta")
	t.Fatalf("beta did not transition to Dead; state=%s", n.State)
}

// TestGossip_RefusesSelfSeed proves the constructor's instruction-shaped
// rejection of misconfigured seed lists is reachable from the public API.
func TestGossip_RefusesSelfSeed(t *testing.T) {
	shared := newSharedTLS(t)
	m, err := membership.New("alpha", "alpha:7100", "alpha:7000")
	if err != nil {
		t.Fatalf("membership.New: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err = gossip.New(m, gossip.Options{
		Addr:      "0.0.0.0:7100",
		Seeds:     []string{"alpha"},
		ServerTLS: shared.server(),
		ClientTLS: shared.client(),
	}, logger)
	if err == nil || !strings.Contains(err.Error(), "own identity") {
		t.Errorf("expected self-seed rejection, got %v", err)
	}
}

func TestGossip_DefaultsCover(t *testing.T) {
	// Sanity check that the exported defaults are positive durations.
	for name, d := range map[string]time.Duration{
		"ProbeInterval":       gossip.DefaultProbeInterval,
		"ProbeTimeout":        gossip.DefaultProbeTimeout,
		"SuspectTimeout":      gossip.DefaultSuspectTimeout,
		"DeadRetention":       gossip.DefaultDeadRetention,
		"AntiEntropyInterval": gossip.DefaultAntiEntropyInterval,
		"Timeout":             gossip.DefaultTimeout,
	} {
		if d <= 0 {
			t.Errorf("%s default <= 0: %s", name, d)
		}
	}
	if gossip.DefaultIndirectK <= 0 {
		t.Error("DefaultIndirectK should be > 0")
	}
	if gossip.DefaultPiggybackCap <= 0 {
		t.Error("DefaultPiggybackCap should be > 0")
	}
	if gossip.MaxMessageBytes <= 0 {
		t.Error("MaxMessageBytes should be > 0")
	}
}
