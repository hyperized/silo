package clustertls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
)

// configOptions collects the optional knobs the tls.Config builders accept.
// Today the only knob is a revocation list; keeping it in a struct leaves room
// to grow (cipher-suite overrides, key-log writers) without churning every
// builder signature again.
type configOptions struct {
	crl *RevocationList
}

// ConfigOption configures a tls.Config built by this package. Follows the
// functional-options pattern so existing two-arg callers keep compiling while
// new behaviour is opt-in.
type ConfigOption func(*configOptions)

// WithRevocation makes the resulting tls.Config reject any handshake whose peer
// leaf certificate appears in the CRL — in addition to the standard cluster-CA
// chain check. Passing a nil list is a no-op, so silod can thread an optional
// CRL straight through without branching.
func WithRevocation(crl *RevocationList) ConfigOption {
	return func(o *configOptions) { o.crl = crl }
}

func applyConfigOptions(opts []ConfigOption) configOptions {
	var o configOptions
	for _, opt := range opts {
		opt(o.ptr())
	}
	return o
}

// ptr lets applyConfigOptions pass &o to each option without taking the address
// of a range variable; it simply returns the receiver's address.
func (o *configOptions) ptr() *configOptions { return o }

// verifyLeafNotRevoked is a VerifyPeerCertificate callback that rejects a
// handshake whose leaf serial is in the CRL. It runs after Go's standard chain
// verification (RequireAndVerifyClientCert / RootCAs already established trust),
// so it only needs to look at the already-trusted leaf in rawCerts[0].
func (r *RevocationList) verifyLeafNotRevoked(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) == 0 {
		return errors.New("silo: peer presented no certificate for the revocation check")
	}
	leaf, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("silo: peer leaf cert did not parse for the revocation check (%w)", err)
	}
	if r.IsRevoked(leaf.SerialNumber) {
		return fmt.Errorf("silo: peer certificate (serial %s) has been revoked", leaf.SerialNumber)
	}
	return nil
}

// ServerConfig returns a tls.Config that requires the peer to present a
// certificate signed by the cluster CA (mTLS). Chain verification against the
// CA pool establishes membership; pass WithRevocation to additionally reject
// certs that appear in a CA-signed CRL.
func ServerConfig(ca *CA, cert *NodeCert, opts ...ConfigOption) (*tls.Config, error) {
	if ca == nil || ca.Cert == nil {
		return nil, errors.New("silo: ServerConfig requires a loaded cluster CA; pass the result of LoadCA(...)")
	}
	if cert == nil {
		return nil, errors.New("silo: ServerConfig requires a node certificate; pass the result of MintNodeCert or LoadOrMintNode")
	}
	tlsCert, err := cert.AsTLSCertificate()
	if err != nil {
		return nil, fmt.Errorf("silo: could not assemble the server TLS certificate (%w)", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	cfg := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}
	if crl := applyConfigOptions(opts).crl; crl != nil {
		enforceRevocation(cfg, crl)
	}
	return cfg, nil
}

// enforceRevocation installs the leaf-revocation check and disables session
// resumption. Without disabling resumption a peer could complete a fresh
// handshake, get its cert revoked, then resume the cached session and skip the
// VerifyPeerCertificate callback entirely — so the revocation check must come
// with SessionTicketsDisabled to mean anything.
func enforceRevocation(cfg *tls.Config, crl *RevocationList) {
	cfg.VerifyPeerCertificate = crl.verifyLeafNotRevoked
	cfg.SessionTicketsDisabled = true
	cfg.ClientSessionCache = nil
}

// ServerOnlyConfig returns a tls.Config that serves silod's identity but
// does not request a client certificate. Used by the join endpoint
// during initial bootstrap, where the client has not yet been issued a
// cluster-signed cert (they are calling this RPC precisely to get one).
// Pinning is the operator's responsibility — silod prints the server
// cert's fingerprint on first boot so siloctl can verify it on the
// inaugural connection.
func ServerOnlyConfig(cert *NodeCert) (*tls.Config, error) {
	if cert == nil {
		return nil, errors.New("silo: ServerOnlyConfig requires a node certificate; pass the result of MintNodeCert or LoadOrMintNode")
	}
	tlsCert, err := cert.AsTLSCertificate()
	if err != nil {
		return nil, fmt.Errorf("silo: could not assemble the bootstrap TLS certificate (%w)", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// PeerConfig returns a tls.Config suitable for any peer-to-peer mTLS
// dial inside the cluster. Unlike ClientConfig it does not pin a
// ServerName — gossip dials a moving target set of peer addresses and
// would otherwise need a custom config per peer. Identity is checked
// via the cluster CA chain instead: any cert signed by the CA is
// trusted as a cluster peer, which is the same trust model as the
// gRPC chunk service. Use ClientConfig when you do know the peer's
// node id and want strict SAN matching (e.g. siloctl talking to one
// specific silod).
func PeerConfig(ca *CA, cert *NodeCert, opts ...ConfigOption) (*tls.Config, error) {
	if ca == nil || ca.Cert == nil {
		return nil, errors.New("silo: PeerConfig requires a loaded cluster CA; pass the result of LoadCA(...)")
	}
	if cert == nil {
		return nil, errors.New("silo: PeerConfig requires a node certificate; pass the result of MintNodeCert or LoadOrMintNode")
	}
	tlsCert, err := cert.AsTLSCertificate()
	if err != nil {
		return nil, fmt.Errorf("silo: could not assemble the peer TLS certificate (%w)", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	crl := applyConfigOptions(opts).crl
	// InsecureSkipVerify=true plus VerifyPeerCertificate gives us
	// CA-chain verification without ServerName matching. The
	// VerifyPeerCertificate callback runs after the standard handshake
	// and only succeeds when the peer's leaf cert chains to the
	// cluster CA — exactly the contract we want for inter-peer mTLS.
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
		// Session resumption is disabled so a resumed handshake cannot
		// bypass the VerifyPeerCertificate callback below. The custom
		// callback is what enforces the cluster-CA chain in lieu of
		// ServerName matching, so leaving resumption on would defeat
		// the whole verification model.
		SessionTicketsDisabled: true,
		ClientSessionCache:     nil,
		InsecureSkipVerify:     true, //nolint:gosec // VerifyPeerCertificate below enforces the cluster CA chain
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("silo: peer presented no certificate")
			}
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("silo: peer leaf cert did not parse (%w)", err)
			}
			intermediates := x509.NewCertPool()
			for _, raw := range rawCerts[1:] {
				if c, err := x509.ParseCertificate(raw); err == nil {
					intermediates.AddCert(c)
				}
			}
			_, err = leaf.Verify(x509.VerifyOptions{
				Roots:         pool,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
			})
			if err != nil {
				return fmt.Errorf("silo: peer cert does not chain to the cluster CA (%w)", err)
			}
			// Revocation is checked after the chain so a revoked-but-otherwise-
			// valid peer is still rejected. nil CRL → no-op (IsRevoked guards it).
			if crl.IsRevoked(leaf.SerialNumber) {
				return fmt.Errorf("silo: peer certificate (serial %s) has been revoked", leaf.SerialNumber)
			}
			return nil
		},
	}, nil
}

// ClientConfig returns a tls.Config that presents the given client
// certificate and validates the server's cert against the cluster CA.
// ServerName is required because Go's TLS verification needs a DNS-like
// expected name to anchor the SAN check — set it to the peer's node id
// for mTLS, since node certs include the id as a DNS SAN.
func ClientConfig(ca *CA, cert *NodeCert, serverName string, opts ...ConfigOption) (*tls.Config, error) {
	if ca == nil || ca.Cert == nil {
		return nil, errors.New("silo: ClientConfig requires a loaded cluster CA; pass the result of LoadCA(...)")
	}
	if cert == nil {
		return nil, errors.New("silo: ClientConfig requires a client certificate; ship siloctl with one minted from the cluster CA or run 'siloctl cert mint'")
	}
	if serverName == "" {
		return nil, errors.New("silo: ClientConfig requires a ServerName matching the peer's node id; pass it explicitly so TLS can verify the peer's identity")
	}
	tlsCert, err := cert.AsTLSCertificate()
	if err != nil {
		return nil, fmt.Errorf("silo: could not assemble the client TLS certificate (%w)", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	cfg := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		RootCAs:      pool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
	}
	if crl := applyConfigOptions(opts).crl; crl != nil {
		enforceRevocation(cfg, crl)
	}
	return cfg, nil
}
