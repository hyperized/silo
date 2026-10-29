//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"testing"
	"time"
)

// statusView is the slice of /api/v1/status this test reads. 64-bit numbers
// arrive as JSON strings (protojson).
type statusView struct {
	Nodes []struct {
		ID            string `json:"id"`
		CapacityBytes string `json:"capacityBytes"`
	} `json:"nodes"`
}

// peerCapacities returns, for each member n sees, the capacity it reports.
func peerCapacities(n *gossipNode) (map[string]string, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	res, err := client.Get("http://" + n.httpAddr + "/api/v1/status")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", res.StatusCode)
	}
	var v statusView
	if err := json.NewDecoder(res.Body).Decode(&v); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(v.Nodes))
	for _, node := range v.Nodes {
		out[node.ID] = node.CapacityBytes
	}
	return out, nil
}

// waitForPeerCapacity blocks until n reports a non-zero capacity for every
// id in peers. The status endpoint caches for two seconds, which the poll
// interval absorbs.
func waitForPeerCapacity(n *gossipNode, peers []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last map[string]string
	for time.Now().Before(deadline) {
		caps, err := peerCapacities(n)
		if err == nil {
			last = caps
			ok := true
			for _, id := range peers {
				if c := caps[id]; c == "" || c == "0" {
					ok = false
				}
			}
			if ok {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("%s did not see a capacity for every one of %v within %s; last view %v", n.id, peers, timeout, last)
}

// TestGossip_PeersSeeEachOthersCapacity is the end-to-end check for peers'
// capacity adverts: every node must report every other node's capacity, and a
// restarted node must learn its peers' figures again.
func TestGossip_PeersSeeEachOthersCapacity(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}
	if runtime.GOOS != "linux" {
		t.Skip("silod measures filesystem capacity only on Linux, so there is no advert to propagate here")
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

	for _, tc := range []struct {
		node  *gossipNode
		peers []string
	}{
		{a, []string{"beta", "gamma"}},
		{b, []string{"alpha", "gamma"}},
		{c, []string{"alpha", "beta"}},
	} {
		if err := waitForPeerCapacity(tc.node, tc.peers, 20*time.Second); err != nil {
			t.Errorf("%v\n---log:\n%s", err, tc.node.readLog())
		}
	}

	// A quick restart, well inside the suspicion window, is the rolling-upgrade
	// case: the peers never mark gamma dead, and gamma must still pick up their
	// figures from scratch.
	c.kill()
	c2 := startGossipNode(t, bin, "gamma", cSeeds, caCert, caKey, encKey, false)
	t.Cleanup(c2.kill)
	if err := waitForPeerCapacity(c2, []string{"alpha", "beta"}, 20*time.Second); err != nil {
		t.Errorf("restarted gamma: %v\n---log:\n%s", err, c2.readLog())
	}
	if err := waitForPeerCapacity(a, []string{"gamma"}, 20*time.Second); err != nil {
		t.Errorf("alpha after gamma's restart: %v", err)
	}
}
