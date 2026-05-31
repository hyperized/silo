//go:build integration

// Integration tests for the gossip subsystem: spawn three real silod
// processes pointed at each other and observe SWIM converging, then
// kill a node and watch it transition Suspect → Dead, then restart it
// and watch it rejoin. Run with: make test-integration.
package integration_test

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// gossipNode tracks one silod process for the convergence tests. The
// HTTP /healthz endpoint is the integration surface — silod surfaces
// the membership table via a follow-up endpoint in a later milestone,
// so for now we infer "node is up" from /healthz becoming reachable
// and "node sees the cluster" by scraping the gossip logs.
type gossipNode struct {
	id            string
	httpAddr      string
	bootstrapAddr string
	gossipAddr    string
	grpcAddr      string
	dataDir       string
	env           []string
	logFile       string

	mu      sync.Mutex
	cmd     *exec.Cmd
	stopped bool
}

// startGossipNode launches one silod with a private DataDir and the
// supplied seeds. Each silod self-mints its own cluster CA on first
// boot because we are not sharing a CA volume across the test nodes;
// that means seeds need explicit fingerprint pinning for production
// but for an isolated three-process gossip test we can hand-roll a
// shared CA via SILO_TLS_CA_CERT/_KEY.
func startGossipNode(t testing.TB, bin string, id string, seeds []string, sharedCACert, sharedCAKey, encryptionKey string, isCASeed bool) *gossipNode {
	t.Helper()
	dataDir := t.TempDir()
	httpAddr := freePort(t)
	bootstrapAddr := freePort(t)
	gossipAddr := freePort(t)
	grpcAddr := freePort(t)

	env := []string{
		"SILO_NODE_ID=" + id,
		"SILO_HTTP_ADDR=" + httpAddr,
		"SILO_GRPC_ADDR=" + grpcAddr,
		"SILO_BOOTSTRAP_ADDR=" + bootstrapAddr,
		"SILO_GOSSIP_ADDR=" + gossipAddr,
		"SILO_DATA_DIR=" + dataDir,
		"SILO_TLS_CA_CERT=" + sharedCACert,
		"SILO_TLS_CA_KEY=" + sharedCAKey,
		"SILO_ENCRYPTION_KEY=" + encryptionKey,
		"SILO_LOG_LEVEL=info",
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
	}
	if isCASeed {
		env = append(env, "SILO_TLS_CA_SEED=1")
	}
	if len(seeds) > 0 {
		env = append(env, "SILO_SEEDS="+strings.Join(seeds, ","))
	}

	node := &gossipNode{
		id:            id,
		httpAddr:      httpAddr,
		bootstrapAddr: bootstrapAddr,
		gossipAddr:    gossipAddr,
		grpcAddr:      grpcAddr,
		dataDir:       dataDir,
		env:           env,
		logFile:       filepath.Join(dataDir, "silod.log"),
	}
	node.spawn(t, bin)
	return node
}

func (n *gossipNode) spawn(t testing.TB, bin string) {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Env = n.env

	logF, err := os.Create(n.logFile)
	if err != nil {
		t.Fatalf("create log %s: %v", n.logFile, err)
	}
	cmd.Stdout = logF
	cmd.Stderr = logF

	if err := cmd.Start(); err != nil {
		_ = logF.Close()
		t.Fatalf("start silod %s: %v", n.id, err)
	}
	n.mu.Lock()
	n.cmd = cmd
	n.stopped = false
	n.mu.Unlock()

	if err := waitForTCP(n.gossipAddr, 5*time.Second); err != nil {
		n.kill()
		_ = logF.Close()
		t.Fatalf("silod %s gossip listener not reachable: %v", n.id, err)
	}
	if err := waitForTCP(n.httpAddr, 5*time.Second); err != nil {
		n.kill()
		_ = logF.Close()
		t.Fatalf("silod %s http listener not reachable: %v", n.id, err)
	}
}

func (n *gossipNode) kill() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.cmd == nil {
		return
	}
	n.stopped = true
	_ = n.cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() {
		_ = n.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = n.cmd.Process.Kill()
		<-done
	}
}

// readLog returns the current contents of the node's log file for
// post-mortem assertion in tests.
func (n *gossipNode) readLog() string {
	data, err := os.ReadFile(n.logFile)
	if err != nil {
		return ""
	}
	return string(data)
}

// countStateChanges counts how many "gossip state change" log lines
// mention the given node id transitioning to the supplied state.
// silod's slog text handler renders these lines deterministically.
func (n *gossipNode) countStateChanges(targetID, state string) int {
	log := n.readLog()
	want := fmt.Sprintf(`node=%s state=%s`, targetID, state)
	count := 0
	scanner := bufio.NewScanner(strings.NewReader(log))
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), want) {
			count++
		}
	}
	return count
}

// buildSilod compiles cmd/silod and returns the binary path. Tests
// re-use the binary across cases via a per-test sync.Once.
func buildSilod(t testing.TB) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "silod")
	cmd := exec.Command("go", "build", "-o", bin, "../../cmd/silod")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build silod: %v", err)
	}
	return bin
}

// mintSharedCA runs silod once with SILO_TLS_CA_SEED so it writes a
// reusable CA cert and key under DataDir. Returns the paths so the
// three-node test can point every silod at the same material — the
// gossip mTLS handshake fails otherwise (each silod would mint a
// different self-signed CA).
func mintSharedCA(t testing.TB, bin string) (caCertPath, caKeyPath, encryptionKey string) {
	t.Helper()
	dir := t.TempDir()
	caCertPath = filepath.Join(dir, "ca.crt")
	caKeyPath = filepath.Join(dir, "ca.key")
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	encryptionKey = base64.StdEncoding.EncodeToString(key)

	httpAddr := freePort(t)
	grpcAddr := freePort(t)
	bootstrapAddr := freePort(t)
	gossipAddr := freePort(t)
	dataDir := filepath.Join(dir, "node")
	cmd := exec.Command(bin)
	cmd.Env = []string{
		"SILO_NODE_ID=ca-seed",
		"SILO_HTTP_ADDR=" + httpAddr,
		"SILO_GRPC_ADDR=" + grpcAddr,
		"SILO_BOOTSTRAP_ADDR=" + bootstrapAddr,
		"SILO_GOSSIP_ADDR=" + gossipAddr,
		"SILO_DATA_DIR=" + dataDir,
		"SILO_TLS_CA_CERT=" + caCertPath,
		"SILO_TLS_CA_KEY=" + caKeyPath,
		"SILO_TLS_CA_SEED=1",
		"SILO_ENCRYPTION_KEY=" + encryptionKey,
		"SILO_LOG_LEVEL=warn",
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
	}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start CA seed silod: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	})
	if err := waitForTCP(httpAddr, 5*time.Second); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("CA seed silod not ready: %v", err)
	}
	// Now stop the seed; we only needed it to write the CA.
	_ = cmd.Process.Signal(os.Interrupt)
	_ = cmd.Wait()
	// Sanity check.
	for _, p := range []string{caCertPath, caKeyPath} {
		if st, err := os.Stat(p); err != nil || st.Size() == 0 {
			t.Fatalf("CA file %s missing or empty: %v", p, err)
		}
	}
	return caCertPath, caKeyPath, encryptionKey
}

// TestGossip_ThreeNodeConvergence brings up three silod processes
// configured with shared CA + key material so their mTLS handshakes
// succeed. Within a few seconds every node should see every other
// node as Alive.
func TestGossip_ThreeNodeConvergence(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}
	bin := buildSilod(t)
	caCert, caKey, encKey := mintSharedCA(t, bin)

	a := startGossipNode(t, bin, "alpha", nil, caCert, caKey, encKey, false)
	t.Cleanup(a.kill)

	b := startGossipNode(t, bin, "beta", []string{a.gossipAddr}, caCert, caKey, encKey, false)
	t.Cleanup(b.kill)

	c := startGossipNode(t, bin, "gamma", []string{a.gossipAddr, b.gossipAddr}, caCert, caKey, encKey, false)
	t.Cleanup(c.kill)

	// Each node must see the other two as Alive. We watch for the
	// "alive" state-change log line that applyIncoming emits the first
	// time it learns about a peer.
	if err := waitForConvergence(a, []string{"beta", "gamma"}, 8*time.Second); err != nil {
		t.Errorf("alpha: %v\n---alpha log:\n%s", err, a.readLog())
	}
	if err := waitForConvergence(b, []string{"alpha", "gamma"}, 8*time.Second); err != nil {
		t.Errorf("beta: %v\n---beta log:\n%s", err, b.readLog())
	}
	if err := waitForConvergence(c, []string{"alpha", "beta"}, 8*time.Second); err != nil {
		t.Errorf("gamma: %v\n---gamma log:\n%s", err, c.readLog())
	}
}

// TestGossip_FailureDetectionAndRejoin starts three nodes, waits for
// convergence, kills the third, watches the others transition it to
// Suspect and then Dead, then restarts it and watches it rejoin.
func TestGossip_FailureDetectionAndRejoin(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}
	bin := buildSilod(t)
	caCert, caKey, encKey := mintSharedCA(t, bin)

	a := startGossipNode(t, bin, "alpha", nil, caCert, caKey, encKey, false)
	t.Cleanup(a.kill)

	b := startGossipNode(t, bin, "beta", []string{a.gossipAddr}, caCert, caKey, encKey, false)
	t.Cleanup(b.kill)

	cSeeds := []string{a.gossipAddr, b.gossipAddr}
	c := startGossipNode(t, bin, "gamma", cSeeds, caCert, caKey, encKey, false)
	t.Cleanup(c.kill)

	if err := waitForConvergence(a, []string{"beta", "gamma"}, 8*time.Second); err != nil {
		t.Fatalf("initial convergence: %v", err)
	}

	// Kill gamma. The other two should mark it Suspect, then Dead
	// within SuspectTimeout (5s default) + probe slack.
	c.kill()
	if err := waitForState(a, "gamma", "suspect", 10*time.Second); err != nil {
		t.Fatalf("alpha did not mark gamma suspect: %v\n%s", err, a.readLog())
	}
	if err := waitForState(a, "gamma", "dead", 15*time.Second); err != nil {
		t.Fatalf("alpha did not mark gamma dead: %v", err)
	}

	// Restart gamma with the same node id; it should rejoin via the
	// same seeds and the table should flip it back to Alive on alpha.
	c2 := startGossipNode(t, bin, "gamma", cSeeds, caCert, caKey, encKey, false)
	t.Cleanup(c2.kill)
	if err := waitForRejoin(a, "gamma", 10*time.Second); err != nil {
		t.Fatalf("alpha did not see gamma rejoin: %v\n%s", err, a.readLog())
	}
}

// waitForConvergence blocks until log lines from `n` show every entry
// in wantIDs has transitioned to "alive" at least once, or timeout
// elapses. waitForConvergence relies on the structured "gossip state
// change" log line emitted by applyIncoming the first time a peer
// becomes known.
func waitForConvergence(n *gossipNode, wantIDs []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range wantIDs {
			if n.countStateChanges(id, "alive") == 0 {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("did not see all of %v transition to alive within %s", wantIDs, timeout)
}

func waitForState(n *gossipNode, target, state string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.countStateChanges(target, state) > 0 {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("did not see %s transition to %s within %s", target, state, timeout)
}

// waitForRejoin counts "alive" transitions for target on n's log. A
// rejoin produces a NEW alive line because the table had marked the
// peer Dead in between — applyIncoming logs every state change, so the
// second alive entry signals "the peer returned".
func waitForRejoin(n *gossipNode, target string, timeout time.Duration) error {
	before := n.countStateChanges(target, "alive")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.countStateChanges(target, "alive") > before {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("did not see %s rejoin (second alive transition) within %s", target, timeout)
}
