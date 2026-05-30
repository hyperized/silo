package clustertls

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"time"
)

// NodeCert is a per-node TLS identity: the Ed25519 private key plus a
// certificate chain that climbs to the cluster CA. It is held in PEM
// form so callers can serialise it to disk or load it from disk without
// re-encoding.
type NodeCert struct {
	CertPEM []byte
	KeyPEM  []byte
}

// MintNodeCert produces a fresh per-node cert signed by the CA. The
// nodeID is encoded both as a SPIFFE-style URI SAN and as a DNS SAN so
// either x509 chain validation or SPIFFE-aware authz can identify the
// node. Extra DNS names and IPs are added verbatim to handle container
// hostnames and load-balancer endpoints.
func MintNodeCert(ca *CA, nodeID string, dnsNames []string, ipAddrs []net.IP, validFor time.Duration) (*NodeCert, error) {
	if ca == nil || ca.Cert == nil {
		return nil, errors.New("silo: cannot mint a node cert without a CA; load the cluster CA from SILO_TLS_CA_CERT first")
	}
	if ca.Key == nil {
		return nil, errors.New("silo: cannot mint a node cert without the CA private key; only the node that performs the bootstrap needs SILO_TLS_CA_KEY, but this code path required it")
	}
	if nodeID == "" {
		return nil, errors.New("silo: cannot mint a node cert with an empty NodeID; set SILO_NODE_ID to a stable identifier")
	}

	pub, priv, err := ed25519GenerateKey(randReader)
	if err != nil {
		return nil, fmt.Errorf("silo: could not generate the node TLS key (%w); the system entropy pool may be exhausted", err)
	}

	serial, err := randInt(new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("silo: could not generate the node certificate serial number (%w); the system entropy pool may be exhausted", err)
	}

	spiffeURI, err := url.Parse("spiffe://silo/node/" + nodeID)
	if err != nil {
		return nil, fmt.Errorf("silo: could not encode the node identity as a SPIFFE URI (%w); the node id %q may contain characters that are invalid in a URI", err, nodeID)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   nodeID,
			Organization: []string{"silo"},
		},
		NotBefore:   time.Now().Add(-time.Minute),
		NotAfter:    time.Now().Add(validFor),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    append([]string{nodeID}, dnsNames...),
		IPAddresses: ipAddrs,
		URIs:        []*url.URL{spiffeURI},
	}

	der, err := x509CreateCertificate(randReader, template, ca.Cert, pub, ca.Key)
	if err != nil {
		return nil, fmt.Errorf("silo: could not sign the node certificate (%w); please file a bug at https://github.com/hyperized/silo/issues", err)
	}

	keyDER, err := x509MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("silo: could not encode the node TLS key in PKCS#8 (%w); please file a bug at https://github.com/hyperized/silo/issues", err)
	}

	return &NodeCert{
		CertPEM: encodePEM(pemTypeCertificate, der),
		KeyPEM:  encodePEM(pemTypePrivateKey, keyDER),
	}, nil
}

// AsTLSCertificate parses the PEM material into the form tls.Config wants.
// Doing it here keeps callers free of crypto/tls plumbing.
func (n *NodeCert) AsTLSCertificate() (tls.Certificate, error) {
	cert, err := tls.X509KeyPair(n.CertPEM, n.KeyPEM)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("silo: node cert and key do not pair (%w); regenerate the node identity by removing node.crt and node.key from the data directory and restarting silod", err)
	}
	return cert, nil
}

// MintClientCert produces a fresh client-only certificate signed by the
// CA. Used by the bootstrap join flow to hand operators a short-lived
// identity they can present on every subsequent mTLS RPC. The principal
// shows up in the Subject CommonName so audit logs can attribute actions
// back to a human; ExtKeyUsage is ClientAuth-only because client certs
// are never presented as server identities.
func MintClientCert(ca *CA, principal string, validFor time.Duration) (*NodeCert, error) {
	if ca == nil || ca.Cert == nil {
		return nil, errors.New("silo: cannot mint a client cert without a CA; load the cluster CA first")
	}
	if ca.Key == nil {
		return nil, errors.New("silo: cannot mint a client cert without the CA private key; this silod was started without the CA key, so it cannot sign new identities")
	}
	if principal == "" {
		return nil, errors.New("silo: cannot mint a client cert with an empty principal; pass <user>@<hostname> so the issued certificate is attributable")
	}

	pub, priv, err := ed25519GenerateKey(randReader)
	if err != nil {
		return nil, fmt.Errorf("silo: could not generate the client TLS key (%w); the system entropy pool may be exhausted", err)
	}

	serial, err := randInt(new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("silo: could not generate the client certificate serial number (%w); the system entropy pool may be exhausted", err)
	}

	spiffeURI, err := url.Parse("spiffe://silo/client/" + principal)
	if err != nil {
		return nil, fmt.Errorf("silo: could not encode the principal %q as a SPIFFE URI (%w); use a value that is valid in a URI path", principal, err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   principal,
			Organization: []string{"silo"},
		},
		NotBefore:   time.Now().Add(-time.Minute),
		NotAfter:    time.Now().Add(validFor),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        []*url.URL{spiffeURI},
	}

	der, err := x509CreateCertificate(randReader, template, ca.Cert, pub, ca.Key)
	if err != nil {
		return nil, fmt.Errorf("silo: could not sign the client certificate (%w); please file a bug at https://github.com/hyperized/silo/issues", err)
	}

	keyDER, err := x509MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("silo: could not encode the client TLS key in PKCS#8 (%w); please file a bug at https://github.com/hyperized/silo/issues", err)
	}

	return &NodeCert{
		CertPEM: encodePEM(pemTypeCertificate, der),
		KeyPEM:  encodePEM(pemTypePrivateKey, keyDER),
	}, nil
}

// EncodeCertPEM wraps the DER bytes of an x509 certificate in a PEM
// envelope. Exposed so callers outside the package (the bootstrap join
// handler) can hand the cluster CA cert back to a client without
// reimplementing the PEM dance.
func EncodeCertPEM(der []byte) []byte {
	return encodePEM(pemTypeCertificate, der)
}

// LeafFingerprint returns the SHA-256 fingerprint of the leaf
// certificate in n.CertPEM, formatted as "sha256:<hex>". Printed by
// silod on first boot so an operator running `siloctl auth init` can
// pin the server's identity by hand on the inaugural connection — the
// same trust-on-first-use ritual SSH uses for host keys.
func (n *NodeCert) LeafFingerprint() (string, error) {
	block, _ := pem.Decode(n.CertPEM)
	if block == nil || block.Type != pemTypeCertificate {
		return "", errors.New("silo: node cert PEM has no certificate block; the on-disk node.crt may be corrupted")
	}
	sum := sha256.Sum256(block.Bytes)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// NotAfter parses the leaf certificate and returns its expiry time, used by the
// rotation check to decide whether the cert is close enough to expiry to re-mint.
func (n *NodeCert) NotAfter() (time.Time, error) {
	block, _ := pem.Decode(n.CertPEM)
	if block == nil || block.Type != pemTypeCertificate {
		return time.Time{}, errors.New("silo: node cert PEM has no certificate block; the on-disk node.crt may be corrupted")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return time.Time{}, fmt.Errorf("silo: could not parse the node certificate (%w); the on-disk node.crt may be corrupted", err)
	}
	return cert.NotAfter, nil
}

func encodePEM(blockType string, der []byte) []byte {
	return pemEncode(blockType, der)
}
