package clustertls

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"strings"
	"testing"
	"time"
)

// serialOfCert pulls the serial number out of a minted NodeCert so tests can
// revoke a real, CA-signed identity.
func serialOfCert(t *testing.T, nc *NodeCert) *big.Int {
	t.Helper()
	serial, err := SerialOf(nc.CertPEM)
	if err != nil {
		t.Fatalf("SerialOf: %v", err)
	}
	return serial
}

// derOf returns the raw DER of a NodeCert's leaf, the form a
// VerifyPeerCertificate callback receives in rawCerts[0].
func derOf(t *testing.T, nc *NodeCert) []byte {
	t.Helper()
	block, _ := pem.Decode(nc.CertPEM)
	if block == nil {
		t.Fatal("derOf: no PEM block in cert")
	}
	return block.Bytes
}

// certOnlyCA returns the same CA loaded with its certificate but no private
// key — the state of a node that can verify but not sign.
func certOnlyCA(t *testing.T, ca *CA) ([]byte, *CA) {
	t.Helper()
	certPEM := pem.EncodeToMemory(&pem.Block{Type: pemTypeCertificate, Bytes: ca.Cert.Raw})
	loaded, err := LoadCA(certPEM, nil)
	if err != nil {
		t.Fatalf("LoadCA cert-only: %v", err)
	}
	return certPEM, loaded
}

// crlPEMWithGarbageDER wraps non-CRL bytes in a correctly-labelled CRL PEM
// block so LoadCRL gets past the block-type check and fails in the DER parse.
func crlPEMWithGarbageDER() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: pemTypeCRL, Bytes: []byte("not der")})
}

// withNow swaps the package clock so freshness checks are deterministic.
func withNow(t *testing.T, at time.Time) {
	t.Helper()
	prev := nowFunc
	nowFunc = func() time.Time { return at }
	t.Cleanup(func() { nowFunc = prev })
}

func withCreateCRLFailure(t *testing.T, err error) {
	t.Helper()
	prev := x509CreateRevocationList
	x509CreateRevocationList = func(io.Reader, *x509.RevocationList, *x509.Certificate, crypto.Signer) ([]byte, error) {
		return nil, err
	}
	t.Cleanup(func() { x509CreateRevocationList = prev })
}

func TestCRL_GenerateLoadRoundTrip(t *testing.T) {
	_, _, ca := newCA(t)
	revoked := mustMint(t, ca, "revoked-node")
	live := mustMint(t, ca, "live-node")
	revokedSerial := serialOfCert(t, revoked)

	crlPEM, err := GenerateCRL(ca, []*big.Int{revokedSerial}, big.NewInt(7), DefaultCRLLifetime)
	if err != nil {
		t.Fatalf("GenerateCRL: %v", err)
	}

	crl, err := LoadCRL(crlPEM, ca)
	if err != nil {
		t.Fatalf("LoadCRL: %v", err)
	}
	if !crl.IsRevoked(revokedSerial) {
		t.Error("the revoked serial should be reported as revoked")
	}
	if crl.IsRevoked(serialOfCert(t, live)) {
		t.Error("a live cert's serial should not be revoked")
	}
	if crl.Count() != 1 {
		t.Errorf("Count = %d, want 1", crl.Count())
	}
	if crl.Number.Cmp(big.NewInt(7)) != 0 {
		t.Errorf("Number = %s, want 7", crl.Number)
	}
	if got := crl.Serials(); len(got) != 1 || got[0].Cmp(revokedSerial) != 0 {
		t.Errorf("Serials = %v, want [%s]", got, revokedSerial)
	}
}

func TestCRL_SerialsSortedAndDeduped(t *testing.T) {
	_, _, ca := newCA(t)
	// Pass the same serial twice plus two others out of order; the loaded list
	// must be deduped and ascending.
	s := []*big.Int{big.NewInt(30), big.NewInt(10), big.NewInt(10), big.NewInt(20)}
	crlPEM, err := GenerateCRL(ca, s, big.NewInt(1), time.Hour)
	if err != nil {
		t.Fatalf("GenerateCRL: %v", err)
	}
	crl, err := LoadCRL(crlPEM, ca)
	if err != nil {
		t.Fatalf("LoadCRL: %v", err)
	}
	got := crl.Serials()
	if len(got) != 3 {
		t.Fatalf("Serials len = %d, want 3 (deduped)", len(got))
	}
	if got[0].Cmp(big.NewInt(10)) != 0 || got[1].Cmp(big.NewInt(20)) != 0 || got[2].Cmp(big.NewInt(30)) != 0 {
		t.Errorf("Serials = %v, want [10 20 30]", got)
	}
}

func TestCRL_GenerateErrors(t *testing.T) {
	_, _, ca := newCA(t)
	_, certOnly := certOnlyCA(t, ca)

	cases := []struct {
		name    string
		ca      *CA
		serials []*big.Int
		number  *big.Int
		want    string
	}{
		{"nil ca", nil, []*big.Int{big.NewInt(1)}, big.NewInt(1), "CA certificate"},
		{"no key", certOnly, []*big.Int{big.NewInt(1)}, big.NewInt(1), "CA private key"},
		{"nil number", ca, []*big.Int{big.NewInt(1)}, nil, "sequence number"},
		{"nil serial", ca, []*big.Int{nil}, big.NewInt(1), "nil serial"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GenerateCRL(tc.ca, tc.serials, tc.number, time.Hour)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestCRL_GenerateSigningFailure(t *testing.T) {
	_, _, ca := newCA(t)
	withCreateCRLFailure(t, errors.New("x509 boom"))
	_, err := GenerateCRL(ca, []*big.Int{big.NewInt(1)}, big.NewInt(1), time.Hour)
	if err == nil || !strings.Contains(err.Error(), "could not sign the CRL") {
		t.Errorf("got %v, want a signing failure", err)
	}
}

func TestCRL_LoadErrors(t *testing.T) {
	_, _, ca := newCA(t)
	_, _, otherCA := newCA(t)
	good, err := GenerateCRL(ca, []*big.Int{big.NewInt(1)}, big.NewInt(1), time.Hour)
	if err != nil {
		t.Fatalf("GenerateCRL: %v", err)
	}

	cases := []struct {
		name string
		pem  []byte
		ca   *CA
		want string
	}{
		{"nil ca", good, nil, "without the cluster CA"},
		{"empty pem", []byte("not pem"), ca, "PEM block missing"},
		{"wrong block type", []byte("-----BEGIN CERTIFICATE-----\nMA==\n-----END CERTIFICATE-----\n"), ca, "want \"X509 CRL\""},
		{"unparseable der", crlPEMWithGarbageDER(), ca, "could not parse the CRL"},
		{"wrong signer", good, otherCA, "does not verify"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadCRL(tc.pem, tc.ca)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestCRL_NilReceiverIsInert(t *testing.T) {
	var crl *RevocationList
	if crl.IsRevoked(big.NewInt(1)) {
		t.Error("nil CRL revokes nothing")
	}
	if crl.Count() != 0 {
		t.Error("nil CRL Count should be 0")
	}
	if crl.Serials() != nil {
		t.Error("nil CRL Serials should be nil")
	}
	if crl.Stale() {
		t.Error("nil CRL is never stale")
	}
}

func TestCRL_IsRevokedNilSerial(t *testing.T) {
	_, _, ca := newCA(t)
	crlPEM, _ := GenerateCRL(ca, []*big.Int{big.NewInt(1)}, big.NewInt(1), time.Hour)
	crl, _ := LoadCRL(crlPEM, ca)
	if crl.IsRevoked(nil) {
		t.Error("a nil serial is never revoked")
	}
}

func TestCRL_Stale(t *testing.T) {
	_, _, ca := newCA(t)
	crlPEM, _ := GenerateCRL(ca, nil, big.NewInt(1), time.Hour)
	crl, _ := LoadCRL(crlPEM, ca)
	if crl.Stale() {
		t.Error("a CRL valid for an hour should not be stale now")
	}
	// Advance the clock past NextUpdate.
	withNow(t, crl.NextUpdate.Add(time.Minute))
	if !crl.Stale() {
		t.Error("a CRL past NextUpdate should be stale")
	}
}

func TestSerialOf_BadPEM(t *testing.T) {
	if _, err := SerialOf([]byte("not a cert")); err == nil {
		t.Error("SerialOf should reject non-cert PEM")
	}
}

func TestRevocation_VerifyLeafNotRevoked(t *testing.T) {
	_, _, ca := newCA(t)
	revoked := mustMint(t, ca, "revoked")
	live := mustMint(t, ca, "live")
	crlPEM, _ := GenerateCRL(ca, []*big.Int{serialOfCert(t, revoked)}, big.NewInt(1), time.Hour)
	crl, _ := LoadCRL(crlPEM, ca)

	revokedDER := derOf(t, revoked)
	liveDER := derOf(t, live)

	if err := crl.verifyLeafNotRevoked([][]byte{revokedDER}, nil); err == nil {
		t.Error("a revoked leaf should be rejected")
	}
	if err := crl.verifyLeafNotRevoked([][]byte{liveDER}, nil); err != nil {
		t.Errorf("a live leaf should pass: %v", err)
	}
	if err := crl.verifyLeafNotRevoked(nil, nil); err == nil {
		t.Error("no certificate should be rejected")
	}
	if err := crl.verifyLeafNotRevoked([][]byte{[]byte("garbage")}, nil); err == nil {
		t.Error("an unparseable leaf should be rejected")
	}
}

func TestWithRevocation_OptionPlumbing(t *testing.T) {
	// nil option (no CRL) leaves VerifyPeerCertificate unset on Server/Client.
	if got := applyConfigOptions(nil).crl; got != nil {
		t.Errorf("no options should yield a nil CRL, got %v", got)
	}
	crl := &RevocationList{}
	if got := applyConfigOptions([]ConfigOption{WithRevocation(crl)}).crl; got != crl {
		t.Error("WithRevocation should record the CRL")
	}

	_, _, ca := newCA(t)
	nc := mustMint(t, ca, "n")
	srv, err := ServerConfig(ca, nc, WithRevocation(crl))
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	if srv.VerifyPeerCertificate == nil {
		t.Error("ServerConfig with a CRL should install a VerifyPeerCertificate hook")
	}
	cli, err := ClientConfig(ca, nc, "n", WithRevocation(crl))
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	if cli.VerifyPeerCertificate == nil {
		t.Error("ClientConfig with a CRL should install a VerifyPeerCertificate hook")
	}
	// No CRL → no hook.
	plain, _ := ServerConfig(ca, nc)
	if plain.VerifyPeerCertificate != nil {
		t.Error("ServerConfig without a CRL should not set VerifyPeerCertificate")
	}
}

// TestRevocation_RejectsRevokedClient is the end-to-end proof: a client whose
// cert the operator has revoked cannot complete the mTLS handshake even though
// the cert still chains to the cluster CA.
func TestRevocation_RejectsRevokedClient(t *testing.T) {
	_, _, ca := newCA(t)
	server := mustMint(t, ca, "server-id")
	client := mustMint(t, ca, "client-id")

	crlPEM, _ := GenerateCRL(ca, []*big.Int{serialOfCert(t, client)}, big.NewInt(1), time.Hour)
	crl, _ := LoadCRL(crlPEM, ca)

	srvCfg, _ := ServerConfig(ca, server, WithRevocation(crl))
	cliCfg, _ := ClientConfig(ca, client, "server-id")

	ln, err := tls.Listen("tcp", "127.0.0.1:0", srvCfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer ln.Close()
	// The server enforces the CRL on the client's cert. In TLS 1.3 the client's
	// Dial can return before the server's rejection alert arrives, so the
	// authoritative signal is the server-side handshake error.
	srvErr := make(chan error, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			srvErr <- acceptErr
			return
		}
		defer conn.Close()
		if tc, ok := conn.(*tls.Conn); ok {
			srvErr <- tc.HandshakeContext(testContext(t))
			return
		}
		srvErr <- nil
	}()

	conn, err := tls.Dial("tcp", ln.Addr().String(), cliCfg)
	if err == nil {
		// Drive a read so the server's alert can propagate, then close.
		_, _ = conn.Read(make([]byte, 1))
		_ = conn.Close()
	}
	if err := <-srvErr; err == nil {
		t.Fatal("the server should reject a revoked client cert")
	}
}

// TestRevocation_PeerConfigRejectsRevoked covers the gossip/replication dial
// path, where PeerConfig folds the revocation check into its custom callback.
func TestRevocation_PeerConfigRejectsRevoked(t *testing.T) {
	_, _, ca := newCA(t)
	server := mustMint(t, ca, "a")
	client := mustMint(t, ca, "b")

	crlPEM, _ := GenerateCRL(ca, []*big.Int{serialOfCert(t, server)}, big.NewInt(1), time.Hour)
	crl, _ := LoadCRL(crlPEM, ca)

	srv, _ := PeerConfig(ca, server)
	srv.ClientAuth = tls.RequireAndVerifyClientCert
	srv.ClientCAs = srv.RootCAs
	// The client enforces the CRL; the server's cert is revoked, so the dial
	// must fail when the client verifies the peer.
	cli, _ := PeerConfig(ca, client, WithRevocation(crl))

	ln, err := tls.Listen("tcp", "127.0.0.1:0", srv)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer ln.Close()
	go func() {
		if conn, acceptErr := ln.Accept(); acceptErr == nil {
			if tc, ok := conn.(*tls.Conn); ok {
				_ = tc.HandshakeContext(testContext(t))
			}
			_ = conn.Close()
		}
	}()

	conn, err := tls.Dial("tcp", ln.Addr().String(), cli)
	if err == nil {
		_ = conn.Close()
		t.Fatal("dialing a revoked peer should fail")
	}
}
