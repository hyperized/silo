package clustertls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
)

// ServerConfig returns a tls.Config that requires the peer to present a
// certificate signed by the cluster CA (mTLS). VerifyPeerCertificate is
// not customised here — chain verification against the CA pool is
// sufficient for the current authorization model where any cluster
// member is trusted.
func ServerConfig(ca *CA, cert *NodeCert) (*tls.Config, error) {
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
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
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

// ClientConfig returns a tls.Config that presents the given client
// certificate and validates the server's cert against the cluster CA.
// ServerName is required because Go's TLS verification needs a DNS-like
// expected name to anchor the SAN check — set it to the peer's node id
// for mTLS, since node certs include the id as a DNS SAN.
func ClientConfig(ca *CA, cert *NodeCert, serverName string) (*tls.Config, error) {
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
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		RootCAs:      pool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
	}, nil
}
