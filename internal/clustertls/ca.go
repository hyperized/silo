// Package clustertls provides silo's cluster PKI: an Ed25519 CA whose
// private key signs short-lived per-node certificates, plus the
// tls.Config builders that turn the resulting material into mTLS for
// both the gRPC surface and (later) the gossip transport.
//
// Ed25519 was chosen over RSA/ECDSA because it gives the smallest keys
// and signatures of any TLS-eligible algorithm; small certs matter when
// every gossip handshake exchanges them.
package clustertls

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"time"
)

// pemTypeCertificate and pemTypeKey are the standard PEM block labels.
// Using x509.MarshalPKCS8PrivateKey lets us encode Ed25519 keys with the
// same envelope as any other modern key type.
const (
	pemTypeCertificate = "CERTIFICATE"
	pemTypePrivateKey  = "PRIVATE KEY"
)

// Crypto seams. Production wires them to crypto/rand and x509;
// tests swap them to exercise the error paths that real entropy
// pools never trip on a healthy host.
var (
	randReader                 = rand.Reader
	ed25519GenerateKey         = ed25519.GenerateKey
	x509CreateCertificate      = x509.CreateCertificate
	x509MarshalPKCS8PrivateKey = x509.MarshalPKCS8PrivateKey
	randInt                    = defaultRandInt
)

func defaultRandInt(upper *big.Int) (*big.Int, error) {
	return rand.Int(rand.Reader, upper)
}

// CA represents a parsed cluster certificate authority. The Key is
// optional: nodes that only need to verify chains (or operator hosts
// that hand-distribute certs) can load just the certificate.
type CA struct {
	Cert *x509.Certificate
	Key  ed25519.PrivateKey
}

// GenerateCA produces a self-signed Ed25519 CA valid for the given
// lifetime. Suitable for new clusters and test fixtures; production
// deployments would typically pin a long-lived CA generated once by an
// operator and distributed to every node.
func GenerateCA(commonName string, validFor time.Duration) (certPEM, keyPEM []byte, err error) {
	pub, priv, err := ed25519GenerateKey(randReader)
	if err != nil {
		return nil, nil, fmt.Errorf("silo: could not generate the cluster CA key (%w); the system entropy pool may be exhausted", err)
	}

	serial, err := randInt(new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("silo: could not generate the CA serial number (%w); the system entropy pool may be exhausted", err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"silo"},
		},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(validFor),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true, // disallow intermediate CAs
	}

	der, err := x509CreateCertificate(randReader, template, template, pub, priv)
	if err != nil {
		return nil, nil, fmt.Errorf("silo: could not create the CA certificate (%w); please file a bug at https://github.com/hyperized/silo/issues", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: pemTypeCertificate, Bytes: der})

	keyDER, err := x509MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, fmt.Errorf("silo: could not encode the CA key in PKCS#8 (%w); this is a programming error, please file a bug at https://github.com/hyperized/silo/issues", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: pemTypePrivateKey, Bytes: keyDER})

	return certPEM, keyPEM, nil
}

// LoadCA parses the PEM material into a CA. The key argument is
// optional: if empty, the returned CA can only verify chains, not
// mint new node certificates.
func LoadCA(certPEM, keyPEM []byte) (*CA, error) {
	cert, err := parseCertPEM(certPEM)
	if err != nil {
		return nil, fmt.Errorf("silo: cluster CA certificate (%w); regenerate with 'siloctl ca init' or point SILO_TLS_CA_CERT at the correct file", err)
	}
	if !cert.IsCA {
		return nil, errors.New("silo: cluster CA certificate has IsCA=false; the file at SILO_TLS_CA_CERT is not a CA — regenerate one with 'siloctl ca init'")
	}

	ca := &CA{Cert: cert}
	if len(keyPEM) == 0 {
		return ca, nil
	}

	key, err := parseEd25519KeyPEM(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("silo: cluster CA key (%w); the file at SILO_TLS_CA_KEY may be corrupted, the wrong format, or the wrong algorithm — regenerate with 'siloctl ca init'", err)
	}

	pub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("silo: cluster CA certificate carries a non-Ed25519 public key; silo expects Ed25519, regenerate with 'siloctl ca init'")
	}
	if !pub.Equal(key.Public()) {
		return nil, errors.New("silo: the cluster CA certificate and key do not match each other; ensure SILO_TLS_CA_CERT and SILO_TLS_CA_KEY come from the same 'siloctl ca init' invocation")
	}

	ca.Key = key
	return ca, nil
}

func parseCertPEM(data []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("PEM block missing or unrecognised")
	}
	if block.Type != pemTypeCertificate {
		return nil, fmt.Errorf("PEM block is %q, want %q", block.Type, pemTypeCertificate)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("DER parse failed: %w", err)
	}
	return cert, nil
}

func parseEd25519KeyPEM(data []byte) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("PEM block missing or unrecognised")
	}
	if block.Type != pemTypePrivateKey {
		return nil, fmt.Errorf("PEM block is %q, want %q", block.Type, pemTypePrivateKey)
	}
	keyAny, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("PKCS#8 parse failed: %w", err)
	}
	key, ok := keyAny.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is %T, want ed25519.PrivateKey", keyAny)
	}
	return key, nil
}
