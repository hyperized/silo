package clustertls

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testCALifetime = time.Hour

// helpers ---------------------------------------------------------------------

func newCA(t *testing.T) (caPEM, keyPEM []byte, ca *CA) {
	t.Helper()
	caPEM, keyPEM, err := GenerateCA("silo-test", testCALifetime)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, err = LoadCA(caPEM, keyPEM)
	if err != nil {
		t.Fatalf("LoadCA: %v", err)
	}
	return caPEM, keyPEM, ca
}

func mustMint(t *testing.T, ca *CA, id string) *NodeCert {
	t.Helper()
	nc, err := MintNodeCert(ca, id, nil, []net.IP{net.ParseIP("127.0.0.1")}, time.Hour)
	if err != nil {
		t.Fatalf("MintNodeCert: %v", err)
	}
	return nc
}

// CA -------------------------------------------------------------------------

func TestGenerateCA_RoundTrips(t *testing.T) {
	caPEM, keyPEM, ca := newCA(t)
	if ca.Cert == nil || ca.Key == nil {
		t.Fatal("CA should carry both cert and key after Load")
	}
	if !ca.Cert.IsCA {
		t.Error("generated cert is not marked as CA")
	}
	if ca.Cert.Subject.CommonName != "silo-test" {
		t.Errorf("CN: got %q, want silo-test", ca.Cert.Subject.CommonName)
	}
	if got := len(ca.Key); got != ed25519.PrivateKeySize {
		t.Errorf("key length: got %d, want %d", got, ed25519.PrivateKeySize)
	}

	// CA cert PEM should parse to the same cert we loaded.
	other, err := LoadCA(caPEM, keyPEM)
	if err != nil {
		t.Fatalf("second LoadCA: %v", err)
	}
	if !other.Cert.Equal(ca.Cert) {
		t.Error("re-loaded CA cert does not equal first load")
	}
}

func TestLoadCA_RejectsBadInputs(t *testing.T) {
	caPEM, keyPEM, _ := newCA(t)

	cases := []struct {
		name    string
		cert    []byte
		key     []byte
		wantSub string
	}{
		{"empty cert", nil, nil, "PEM block missing"},
		{"non-cert PEM block", pem.EncodeToMemory(&pem.Block{Type: "X", Bytes: []byte{0x00}}), nil, "PEM block is"},
		{"garbage cert DER", pem.EncodeToMemory(&pem.Block{Type: pemTypeCertificate, Bytes: []byte{0x00, 0x01}}), nil, "DER parse"},
		{"non-CA cert", makeNonCACertPEM(t), nil, "IsCA=false"},
		{"bad key PEM", caPEM, []byte("not pem"), "PEM block missing"},
		{"wrong key PEM type", caPEM, pem.EncodeToMemory(&pem.Block{Type: "X", Bytes: []byte{0}}), "PEM block is"},
		{"garbage key DER", caPEM, pem.EncodeToMemory(&pem.Block{Type: pemTypePrivateKey, Bytes: []byte{0x00, 0x01}}), "PKCS#8 parse"},
		{"key/cert mismatch", caPEM, makeMismatchedKeyPEM(t, keyPEM), "do not match"},
		{"non-ed25519 key", caPEM, makeRSAKeyPEM(t), "ed25519.PrivateKey"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadCA(tc.cert, tc.key)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("got %v, want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestLoadCA_CertOnlyOmitsKey(t *testing.T) {
	caPEM, _, _ := newCA(t)
	ca, err := LoadCA(caPEM, nil)
	if err != nil {
		t.Fatalf("LoadCA cert-only: %v", err)
	}
	if ca.Key != nil {
		t.Error("Key should be nil when only the cert is provided")
	}
}

// MintNodeCert ---------------------------------------------------------------

func TestMintNodeCert_Validates(t *testing.T) {
	_, _, ca := newCA(t)

	cases := []struct {
		name    string
		ca      *CA
		id      string
		wantSub string
	}{
		{"nil CA", nil, "n1", "without a CA"},
		{"CA cert only", &CA{Cert: ca.Cert}, "n1", "private key"},
		{"empty id", ca, "", "NodeID"},
		{"id with control chars", ca, "node\x7f", "SPIFFE URI"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := MintNodeCert(tc.ca, tc.id, nil, nil, time.Hour)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("got %v, want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestMintNodeCert_SignedByCA(t *testing.T) {
	_, _, ca := newCA(t)
	nc := mustMint(t, ca, "node-a")

	cert := parsePEMToCert(t, nc.CertPEM)
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	chains, err := cert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil || len(chains) == 0 {
		t.Errorf("verify: %v (chains=%d)", err, len(chains))
	}
	if cert.Subject.CommonName != "node-a" {
		t.Errorf("CN: got %q, want node-a", cert.Subject.CommonName)
	}

	wantDNS := []string{"node-a"}
	if got, want := cert.DNSNames, wantDNS; !equalStrings(got, want) {
		t.Errorf("DNSNames: got %v, want %v", got, want)
	}
	if len(cert.URIs) != 1 || cert.URIs[0].String() != "spiffe://silo/node/node-a" {
		t.Errorf("URIs: got %v, want spiffe://silo/node/node-a", cert.URIs)
	}
}

// TLS configs ---------------------------------------------------------------

func TestTLSConfigs_ValidateInputs(t *testing.T) {
	_, _, ca := newCA(t)
	nc := mustMint(t, ca, "n1")

	if _, err := ServerConfig(nil, nc); err == nil || !strings.Contains(err.Error(), "loaded cluster CA") {
		t.Errorf("ServerConfig nil ca: %v", err)
	}
	if _, err := ServerConfig(ca, nil); err == nil || !strings.Contains(err.Error(), "node certificate") {
		t.Errorf("ServerConfig nil cert: %v", err)
	}
	if _, err := ClientConfig(nil, nc, "n1"); err == nil {
		t.Errorf("ClientConfig nil ca: %v", err)
	}
	if _, err := ClientConfig(ca, nil, "n1"); err == nil {
		t.Errorf("ClientConfig nil cert: %v", err)
	}
	if _, err := ClientConfig(ca, nc, ""); err == nil || !strings.Contains(err.Error(), "ServerName") {
		t.Errorf("ClientConfig empty serverName: %v", err)
	}

	// Mismatched cert/key should be rejected when AsTLSCertificate is invoked.
	bad := &NodeCert{CertPEM: nc.CertPEM, KeyPEM: pem.EncodeToMemory(&pem.Block{Type: pemTypePrivateKey, Bytes: []byte{0x00}})}
	if _, err := ServerConfig(ca, bad); err == nil || !strings.Contains(err.Error(), "do not pair") {
		t.Errorf("ServerConfig bad pair: %v", err)
	}
	if _, err := ClientConfig(ca, bad, "n1"); err == nil || !strings.Contains(err.Error(), "do not pair") {
		t.Errorf("ClientConfig bad pair: %v", err)
	}
}

func TestPeerConfig_ValidateInputs(t *testing.T) {
	_, _, ca := newCA(t)
	nc := mustMint(t, ca, "n1")

	if _, err := PeerConfig(nil, nc); err == nil || !strings.Contains(err.Error(), "loaded cluster CA") {
		t.Errorf("PeerConfig nil ca: %v", err)
	}
	if _, err := PeerConfig(ca, nil); err == nil || !strings.Contains(err.Error(), "node certificate") {
		t.Errorf("PeerConfig nil cert: %v", err)
	}
	bad := &NodeCert{CertPEM: nc.CertPEM, KeyPEM: pem.EncodeToMemory(&pem.Block{Type: pemTypePrivateKey, Bytes: []byte{0x00}})}
	if _, err := PeerConfig(ca, bad); err == nil || !strings.Contains(err.Error(), "do not pair") {
		t.Errorf("PeerConfig bad pair: %v", err)
	}
}

func TestPeerConfig_VerifyPeerCertificate(t *testing.T) {
	// Build a PeerConfig and exercise its custom VerifyPeerCertificate
	// callback. The callback should accept certs signed by the cluster
	// CA and reject everything else, regardless of ServerName.
	_, _, ca := newCA(t)
	nc := mustMint(t, ca, "n1")
	cfg, err := PeerConfig(ca, nc)
	if err != nil {
		t.Fatalf("PeerConfig: %v", err)
	}
	if cfg.VerifyPeerCertificate == nil {
		t.Fatal("PeerConfig must set VerifyPeerCertificate")
	}

	// Empty rawCerts: reject.
	if err := cfg.VerifyPeerCertificate(nil, nil); err == nil {
		t.Error("empty rawCerts: expected reject")
	}

	// Garbage rawCerts: reject.
	if err := cfg.VerifyPeerCertificate([][]byte{[]byte("not a cert")}, nil); err == nil {
		t.Error("garbage rawCerts: expected reject")
	}

	// Real peer cert signed by the same CA: accept.
	peer := mustMint(t, ca, "peer")
	block, _ := pem.Decode(peer.CertPEM)
	if err := cfg.VerifyPeerCertificate([][]byte{block.Bytes}, nil); err != nil {
		t.Errorf("legit peer cert: %v", err)
	}

	// Cert signed by a different CA: reject.
	_, _, otherCA := newCA(t)
	rogue := mustMint(t, otherCA, "rogue")
	rblock, _ := pem.Decode(rogue.CertPEM)
	if err := cfg.VerifyPeerCertificate([][]byte{rblock.Bytes}, nil); err == nil {
		t.Error("rogue cert: expected reject")
	}
}

func TestPeerConfig_VerifyHandlesIntermediates(t *testing.T) {
	// VerifyPeerCertificate iterates rawCerts[1:] adding parseable
	// intermediates. We feed it a real leaf followed by an unparseable
	// blob (the intermediate branch) and one real intermediate so both
	// halves of the loop body run.
	_, _, ca := newCA(t)
	nc := mustMint(t, ca, "n1")
	cfg, _ := PeerConfig(ca, nc)
	peer := mustMint(t, ca, "peer")
	leaf, _ := pem.Decode(peer.CertPEM)
	garbage := []byte("not a der cert")
	if err := cfg.VerifyPeerCertificate([][]byte{leaf.Bytes, garbage, leaf.Bytes}, nil); err != nil {
		t.Errorf("real leaf with garbage intermediate: %v", err)
	}
}

func TestPeerConfig_EndToEndHandshakeAcrossPeers(t *testing.T) {
	// Two peers minted by the same CA should mTLS-handshake successfully
	// using PeerConfig on both sides even though neither sets ServerName.
	_, _, ca := newCA(t)
	a := mustMint(t, ca, "a")
	b := mustMint(t, ca, "b")
	srv, err := PeerConfig(ca, a)
	if err != nil {
		t.Fatalf("PeerConfig server: %v", err)
	}
	srv.ClientAuth = tls.RequireAndVerifyClientCert
	srv.ClientCAs = srv.RootCAs
	cli, err := PeerConfig(ca, b)
	if err != nil {
		t.Fatalf("PeerConfig client: %v", err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srv)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer ln.Close()
	done := make(chan error, 1)
	go func() {
		c, acceptErr := ln.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		if tc, ok := c.(*tls.Conn); ok {
			done <- tc.HandshakeContext(testContext(t))
		}
		_ = c.Close()
	}()
	conn, err := tls.Dial("tcp", ln.Addr().String(), cli)
	if err != nil {
		t.Fatalf("tls.Dial: %v", err)
	}
	_ = conn.Close()
	if err := <-done; err != nil {
		t.Errorf("server handshake: %v", err)
	}
}

func TestTLSConfigs_EndToEndHandshake(t *testing.T) {
	_, _, ca := newCA(t)
	server := mustMint(t, ca, "server-id")
	client := mustMint(t, ca, "client-id")

	srvCfg, err := ServerConfig(ca, server)
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	cliCfg, err := ClientConfig(ca, client, "server-id")
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", srvCfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer ln.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		if tc, ok := conn.(*tls.Conn); ok {
			if err := tc.HandshakeContext(testContext(t)); err != nil {
				done <- err
				return
			}
			// Send a byte so the client knows we accepted.
			_, _ = conn.Write([]byte("ok"))
		}
		done <- nil
	}()

	conn, err := tls.Dial("tcp", ln.Addr().String(), cliCfg)
	if err != nil {
		t.Fatalf("tls.Dial: %v", err)
	}
	defer conn.Close()
	buf := make([]byte, 2)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("server: %v", err)
	}
	if string(buf) != "ok" {
		t.Errorf("got %q, want ok", buf)
	}
}

func TestTLSConfigs_RejectUnsignedClient(t *testing.T) {
	_, _, clusterCA := newCA(t)
	_, _, otherCA := newCA(t)

	server := mustMint(t, clusterCA, "server-id")
	rogue := mustMint(t, otherCA, "rogue-id")

	srvCfg, _ := ServerConfig(clusterCA, server)
	cliCfg, _ := ClientConfig(otherCA, rogue, "server-id")

	ln, _ := tls.Listen("tcp", "127.0.0.1:0", srvCfg)
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()

	_, err := tls.Dial("tcp", ln.Addr().String(), cliCfg)
	if err == nil {
		t.Fatal("Dial should fail when the client cert is from a different CA")
	}
}

// LoadOrMintNode -------------------------------------------------------------

func TestLoadOrMintNode_FirstCallMintsSubsequentCallsLoad(t *testing.T) {
	dir := t.TempDir()
	_, _, ca := newCA(t)

	first, err := LoadOrMintNode(dir, ca, "n1", nil, nil)
	if err != nil {
		t.Fatalf("LoadOrMintNode mint: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "node.crt")); err != nil {
		t.Errorf("node.crt not written: %v", err)
	}

	second, err := LoadOrMintNode(dir, ca, "n1", nil, nil)
	if err != nil {
		t.Fatalf("LoadOrMintNode load: %v", err)
	}
	if string(first.CertPEM) != string(second.CertPEM) {
		t.Error("second call should reuse the on-disk cert, not regenerate")
	}
}

func TestLoadOrMintNode_RequiresDir(t *testing.T) {
	_, _, ca := newCA(t)
	if _, err := LoadOrMintNode("", ca, "n1", nil, nil); err == nil || !strings.Contains(err.Error(), "SILO_DATA_DIR") {
		t.Errorf("expected SILO_DATA_DIR error, got %v", err)
	}
}

func TestLoadOrMintNode_NoCAKeyAndNoStoredCert(t *testing.T) {
	dir := t.TempDir()
	_, _, ca := newCA(t)
	ca.Key = nil // simulate "only the cert was distributed"

	_, err := LoadOrMintNode(dir, ca, "n1", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "SILO_TLS_CA_KEY") {
		t.Errorf("expected actionable no-key error, got %v", err)
	}
}

// Rand / x509 failure injection ---------------------------------------------

// withED25519Failure overrides the key-generation seam for the duration
// of a test. The default implementation is restored on cleanup.
func withED25519Failure(t *testing.T, err error) {
	t.Helper()
	prev := ed25519GenerateKey
	t.Cleanup(func() { ed25519GenerateKey = prev })
	ed25519GenerateKey = func(io.Reader) (ed25519.PublicKey, ed25519.PrivateKey, error) {
		return nil, nil, err
	}
}

func withRandIntFailure(t *testing.T, err error) {
	t.Helper()
	prev := randInt
	t.Cleanup(func() { randInt = prev })
	randInt = func(*big.Int) (*big.Int, error) { return nil, err }
}

func withCreateCertFailure(t *testing.T, err error) {
	t.Helper()
	prev := x509CreateCertificate
	t.Cleanup(func() { x509CreateCertificate = prev })
	x509CreateCertificate = func(io.Reader, *x509.Certificate, *x509.Certificate, any, any) ([]byte, error) {
		return nil, err
	}
}

func withMarshalPKCS8Failure(t *testing.T, err error) {
	t.Helper()
	prev := x509MarshalPKCS8PrivateKey
	t.Cleanup(func() { x509MarshalPKCS8PrivateKey = prev })
	x509MarshalPKCS8PrivateKey = func(any) ([]byte, error) { return nil, err }
}

func TestGenerateCA_RandFailures(t *testing.T) {
	cases := []struct {
		name string
		swap func(*testing.T)
		want string
	}{
		{
			name: "key generation fails",
			swap: func(t *testing.T) { withED25519Failure(t, errors.New("no entropy")) },
			want: "CA key",
		},
		{
			name: "serial number fails",
			swap: func(t *testing.T) { withRandIntFailure(t, errors.New("no entropy")) },
			want: "serial number",
		},
		{
			name: "certificate signing fails",
			swap: func(t *testing.T) { withCreateCertFailure(t, errors.New("x509 boom")) },
			want: "CA certificate",
		},
		{
			name: "PKCS8 marshal fails",
			swap: func(t *testing.T) { withMarshalPKCS8Failure(t, errors.New("marshal boom")) },
			want: "PKCS#8",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.swap(t)
			_, _, err := GenerateCA("silo-test", testCALifetime)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestMintNodeCert_RandFailures(t *testing.T) {
	_, _, ca := newCA(t)
	cases := []struct {
		name string
		swap func(*testing.T)
		want string
	}{
		{
			name: "key generation fails",
			swap: func(t *testing.T) { withED25519Failure(t, errors.New("no entropy")) },
			want: "node TLS key",
		},
		{
			name: "serial number fails",
			swap: func(t *testing.T) { withRandIntFailure(t, errors.New("no entropy")) },
			want: "serial number",
		},
		{
			name: "certificate signing fails",
			swap: func(t *testing.T) { withCreateCertFailure(t, errors.New("x509 boom")) },
			want: "sign the node certificate",
		},
		{
			name: "PKCS8 marshal fails",
			swap: func(t *testing.T) { withMarshalPKCS8Failure(t, errors.New("marshal boom")) },
			want: "PKCS#8",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.swap(t)
			_, err := MintNodeCert(ca, "n1", nil, nil, time.Hour)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want substring %q", err, tc.want)
			}
		})
	}
}

// writeAtomic failure injection --------------------------------------------

// fakeFile fails Write/Sync/Close on demand so the post-open branches of
// writeAtomic become testable without simulating real disk faults.
type fakeFile struct {
	writeErr error
	syncErr  error
	closeErr error
}

func (f *fakeFile) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return len(p), nil
}

func (f *fakeFile) Sync() error  { return f.syncErr }
func (f *fakeFile) Close() error { return f.closeErr }

func withFakeFile(t *testing.T, f *fakeFile) {
	t.Helper()
	prev := openExclusiveFile
	t.Cleanup(func() { openExclusiveFile = prev })
	openExclusiveFile = func(string, os.FileMode) (syncCloser, error) { return f, nil }
}

func withOpenFailure(t *testing.T, err error) {
	t.Helper()
	prev := openExclusiveFile
	t.Cleanup(func() { openExclusiveFile = prev })
	openExclusiveFile = func(string, os.FileMode) (syncCloser, error) { return nil, err }
}

func withRenameFailure(t *testing.T, err error) {
	t.Helper()
	prev := osRename
	t.Cleanup(func() { osRename = prev })
	osRename = func(string, string) error { return err }
}

func TestLoadOrMintNode_WriteFailures(t *testing.T) {
	// Each subtest forces a different failure inside writeAtomic so the
	// "could not write the new node certificate/key" error paths in
	// LoadOrMintNode are exercised end-to-end.
	cases := []struct {
		name string
		swap func(*testing.T)
		want string
	}{
		{
			name: "open fails",
			swap: func(t *testing.T) { withOpenFailure(t, errors.New("openboom")) },
			want: "could not write the new node certificate",
		},
		{
			name: "write fails",
			swap: func(t *testing.T) { withFakeFile(t, &fakeFile{writeErr: errors.New("wboom")}) },
			want: "could not write the new node certificate",
		},
		{
			name: "sync fails",
			swap: func(t *testing.T) { withFakeFile(t, &fakeFile{syncErr: errors.New("sboom")}) },
			want: "could not write the new node certificate",
		},
		{
			name: "close fails",
			swap: func(t *testing.T) { withFakeFile(t, &fakeFile{closeErr: errors.New("cboom")}) },
			want: "could not write the new node certificate",
		},
		{
			name: "rename fails",
			swap: func(t *testing.T) { withRenameFailure(t, errors.New("rboom")) },
			want: "could not write the new node certificate",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			_, _, ca := newCA(t)
			tc.swap(t)
			_, err := LoadOrMintNode(dir, ca, "n1", nil, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want substring %q", err, tc.want)
			}
		})
	}
}

// TestLoadOrMintNode_KeyWriteFailureRollsBack covers the rare case where
// the cert file is written but the key file write fails. The partial
// cert must be removed so a subsequent boot doesn't load a half-pair.
func TestLoadOrMintNode_KeyWriteFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	_, _, ca := newCA(t)

	// Make the cert write succeed but the key write fail. We do this by
	// counting calls: first openExclusiveFile call is the cert (real),
	// second is the key (fake).
	prevOpen := openExclusiveFile
	t.Cleanup(func() { openExclusiveFile = prevOpen })
	var calls int
	openExclusiveFile = func(path string, mode os.FileMode) (syncCloser, error) {
		calls++
		if calls == 1 {
			return prevOpen(path, mode)
		}
		return nil, errors.New("simulated key write failure")
	}

	_, err := LoadOrMintNode(dir, ca, "n1", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "node key") {
		t.Fatalf("got %v, want node-key write error", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "node.crt")); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("orphan cert should be removed when key write fails, stat=%v", statErr)
	}
}

// TestLoadOrMintNode_MkdirFailure exercises the MkdirAll error path by
// pointing dir at a path that exists as a regular file.
func TestLoadOrMintNode_MkdirFailure(t *testing.T) {
	parent := t.TempDir()
	blocker := filepath.Join(parent, "blocker")
	if err := os.WriteFile(blocker, []byte{0}, 0o600); err != nil {
		t.Fatalf("seed blocker: %v", err)
	}
	dir := filepath.Join(blocker, "subdir") // can't mkdir under a regular file
	_, _, ca := newCA(t)
	if _, err := LoadOrMintNode(dir, ca, "n1", nil, nil); err == nil || !strings.Contains(err.Error(), "TLS material directory") {
		t.Errorf("got %v, want TLS-material-directory error", err)
	}
}

// TestLoadOrMintNode_MintFailurePropagates wires a mint failure through
// LoadOrMintNode so the package-level err propagation branch is covered.
func TestLoadOrMintNode_MintFailurePropagates(t *testing.T) {
	dir := t.TempDir()
	_, _, ca := newCA(t)
	withED25519Failure(t, errors.New("no entropy"))
	_, err := LoadOrMintNode(dir, ca, "n1", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "node TLS key") {
		t.Errorf("got %v, want propagated mint error", err)
	}
}

// TestLoadCA_NonEd25519CertPublicKey covers the rare path where the cert
// PEM parses but the embedded public key is not Ed25519. We pair an RSA
// cert with a syntactically valid Ed25519 key so LoadCA gets past
// parseEd25519KeyPEM and reaches the type assertion on cert.PublicKey.
func TestLoadCA_NonEd25519CertPublicKey(t *testing.T) {
	rsaPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "rsa-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &rsaPriv.PublicKey, rsaPriv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: pemTypeCertificate, Bytes: der})

	// Synthesise an unrelated Ed25519 key; parseEd25519KeyPEM is happy
	// with it, and the next check (type assertion on cert.PublicKey)
	// fails because the cert carries an RSA public key.
	_, edPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519: %v", err)
	}
	edDER, err := x509.MarshalPKCS8PrivateKey(edPriv)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: pemTypePrivateKey, Bytes: edDER})

	if _, err := LoadCA(certPEM, keyPEM); err == nil || !strings.Contains(err.Error(), "non-Ed25519") {
		t.Errorf("got %v, want non-Ed25519 error", err)
	}
}

// TestLoadIfPresent_KeyMissingAfterCert exercises the "cert present, key
// missing" half of loadIfPresent so both ReadFile errors are covered.
func TestLoadIfPresent_KeyMissingAfterCert(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "node.crt"), []byte("placeholder"), 0o600); err != nil {
		t.Fatalf("seed cert: %v", err)
	}
	if _, ok := loadIfPresent(filepath.Join(dir, "node.crt"), filepath.Join(dir, "node.key")); ok {
		t.Error("loadIfPresent should return false when the key file is absent")
	}
}

// Helpers --------------------------------------------------------------------

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func parsePEMToCert(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("could not decode PEM cert")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func makeNonCACertPEM(t *testing.T) []byte {
	t.Helper()
	_, _, ca := newCA(t)
	nc := mustMint(t, ca, "leaf")
	return nc.CertPEM
}

func makeMismatchedKeyPEM(t *testing.T, _ []byte) []byte {
	t.Helper()
	_, mismatchedKeyPEM, err := GenerateCA("other", testCALifetime)
	if err != nil {
		t.Fatal(err)
	}
	return mismatchedKeyPEM
}

// makeRSAKeyPEM produces a PKCS#8 RSA key to exercise the
// "not Ed25519" branch of LoadCA. RSA is the lightest non-Ed25519
// key type stdlib offers.
func makeRSAKeyPEM(t *testing.T) []byte {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: pemTypePrivateKey, Bytes: der})
}

// MintClientCert ------------------------------------------------------------

func TestMintClientCert_Validates(t *testing.T) {
	_, _, ca := newCA(t)
	cases := []struct {
		name      string
		ca        *CA
		principal string
		wantSub   string
	}{
		{"nil CA", nil, "user@host", "without a CA"},
		{"CA cert only", &CA{Cert: ca.Cert}, "user@host", "private key"},
		{"empty principal", ca, "", "empty principal"},
		{"principal with bad URI chars", ca, "user\x7f", "SPIFFE URI"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := MintClientCert(tc.ca, tc.principal, time.Hour)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("got %v, want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestMintClientCert_SignedByCA(t *testing.T) {
	_, _, ca := newCA(t)
	nc, err := MintClientCert(ca, "user@host", time.Hour)
	if err != nil {
		t.Fatalf("MintClientCert: %v", err)
	}
	cert := parsePEMToCert(t, nc.CertPEM)
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		t.Errorf("verify: %v", err)
	}
	if cert.Subject.CommonName != "user@host" {
		t.Errorf("CN: got %q, want user@host", cert.Subject.CommonName)
	}
	if len(cert.URIs) != 1 || cert.URIs[0].String() != "spiffe://silo/client/user@host" {
		t.Errorf("URIs: got %v, want spiffe://silo/client/user@host", cert.URIs)
	}
	// Client certs deliberately carry no DNS-SAN — silod is the server,
	// not the client, in mTLS handshakes.
	if len(cert.DNSNames) != 0 {
		t.Errorf("DNSNames: got %v, want empty", cert.DNSNames)
	}
}

func TestMintClientCert_RandFailures(t *testing.T) {
	_, _, ca := newCA(t)
	cases := []struct {
		name string
		swap func(*testing.T)
		want string
	}{
		{
			name: "key generation fails",
			swap: func(t *testing.T) { withED25519Failure(t, errors.New("no entropy")) },
			want: "client TLS key",
		},
		{
			name: "serial number fails",
			swap: func(t *testing.T) { withRandIntFailure(t, errors.New("no entropy")) },
			want: "serial number",
		},
		{
			name: "cert signing fails",
			swap: func(t *testing.T) { withCreateCertFailure(t, errors.New("x509 boom")) },
			want: "sign the client certificate",
		},
		{
			name: "PKCS8 marshal fails",
			swap: func(t *testing.T) { withMarshalPKCS8Failure(t, errors.New("marshal boom")) },
			want: "PKCS#8",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.swap(t)
			_, err := MintClientCert(ca, "user@host", time.Hour)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want substring %q", err, tc.want)
			}
		})
	}
}

// EncodeCertPEM / LeafFingerprint -------------------------------------------

func TestEncodeCertPEM(t *testing.T) {
	_, _, ca := newCA(t)
	got := EncodeCertPEM(ca.Cert.Raw)
	if !bytesContains(got, []byte("-----BEGIN CERTIFICATE-----")) {
		t.Errorf("EncodeCertPEM: missing PEM header, got %q", got)
	}
	// Round-trip: decoding the PEM should recover the original DER.
	block, _ := pem.Decode(got)
	if block == nil || block.Type != pemTypeCertificate {
		t.Fatalf("decode: block=%v", block)
	}
	if string(block.Bytes) != string(ca.Cert.Raw) {
		t.Error("round-tripped DER does not match the original")
	}
}

func TestLeafFingerprint(t *testing.T) {
	_, _, ca := newCA(t)
	nc := mustMint(t, ca, "n1")
	fp, err := nc.LeafFingerprint()
	if err != nil {
		t.Fatalf("LeafFingerprint: %v", err)
	}
	if !strings.HasPrefix(fp, "sha256:") {
		t.Errorf("fingerprint should be sha256-prefixed, got %q", fp)
	}
	if len(fp) != len("sha256:")+64 {
		t.Errorf("fingerprint length: got %d, want %d", len(fp), len("sha256:")+64)
	}
}

func TestLeafFingerprint_CorruptPEM(t *testing.T) {
	nc := &NodeCert{CertPEM: []byte("not pem"), KeyPEM: []byte("not pem")}
	if _, err := nc.LeafFingerprint(); err == nil || !strings.Contains(err.Error(), "no certificate block") {
		t.Errorf("got %v, want corrupt-PEM error", err)
	}
}

// ServerOnlyConfig ----------------------------------------------------------

func TestServerOnlyConfig_NilCert(t *testing.T) {
	if _, err := ServerOnlyConfig(nil); err == nil || !strings.Contains(err.Error(), "node certificate") {
		t.Errorf("got %v, want nil-cert error", err)
	}
}

func TestServerOnlyConfig_RejectsBadPair(t *testing.T) {
	_, _, ca := newCA(t)
	good := mustMint(t, ca, "n1")
	bad := &NodeCert{CertPEM: good.CertPEM, KeyPEM: []byte("not a key")}
	if _, err := ServerOnlyConfig(bad); err == nil || !strings.Contains(err.Error(), "bootstrap TLS certificate") {
		t.Errorf("got %v, want bad-pair error", err)
	}
}

func TestServerOnlyConfig_AcceptsAnyClient(t *testing.T) {
	// The bootstrap surface intentionally does NOT request a client
	// cert. Verify by handshaking with a plain TLS client (no certs)
	// and the server-only config; the connection must succeed.
	_, _, ca := newCA(t)
	server := mustMint(t, ca, "n1")
	srvCfg, err := ServerOnlyConfig(server)
	if err != nil {
		t.Fatalf("ServerOnlyConfig: %v", err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srvCfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer ln.Close()
	srvDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			srvDone <- err
			return
		}
		defer conn.Close()
		// Drive the handshake to completion before tearing down so the
		// client's tls.Dial sees a clean exchange, not a half-open TCP
		// reset.
		if tc, ok := conn.(*tls.Conn); ok {
			if err := tc.HandshakeContext(testContext(t)); err != nil {
				srvDone <- err
				return
			}
			_, _ = conn.Write([]byte("ok"))
		}
		srvDone <- nil
	}()
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
		RootCAs:    pool,
		ServerName: "n1",
		MinVersion: tls.VersionTLS13,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	buf := make([]byte, 2)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-srvDone; err != nil {
		t.Fatalf("server: %v", err)
	}
}

func bytesContains(haystack, needle []byte) bool {
	return strings.Contains(string(haystack), string(needle))
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
