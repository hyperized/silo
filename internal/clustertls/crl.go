package clustertls

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"
)

// pemTypeCRL is the standard PEM block label for an X.509 certificate
// revocation list, as produced by openssl and Go's encoding/pem.
const pemTypeCRL = "X509 CRL"

// DefaultCRLLifetime is how long a freshly generated CRL stays valid. A week
// is short enough that a forgotten re-issue is noticed (NextUpdate goes stale
// and silod logs it) but long enough that routine revocations don't demand
// daily tooling. Operators who revoke rarely can pass a longer validFor.
const DefaultCRLLifetime = 7 * 24 * time.Hour

// x509CreateRevocationList is a seam over x509.CreateRevocationList so tests can
// exercise the signing-error path that real Ed25519 keys never trip.
var x509CreateRevocationList = x509.CreateRevocationList

// RevocationList is a parsed, CA-verified set of revoked certificate serials.
// silod consults it on every mTLS handshake (via WithRevocation), so a revoked
// node or client cert is rejected even though it still chains to the CA and has
// not yet expired — closing the window the threat model called out as "stop the
// node and let its cert expire".
//
// The zero value is not usable; build one with LoadCRL. A nil *RevocationList
// is treated as "nothing revoked" by every method, so callers can pass nil
// when no CRL is configured.
type RevocationList struct {
	revoked map[string]struct{} // serial.String() (base 10) -> {}
	serials []*big.Int          // same set, retained for re-issue ordering

	// Number is the monotonic CRL sequence number; ThisUpdate/NextUpdate bound
	// its freshness. Exposed so silod can log a stale CRL and 'siloctl ca
	// revoke' can increment Number when re-signing.
	Number     *big.Int
	ThisUpdate time.Time
	NextUpdate time.Time
}

// IsRevoked reports whether the given certificate serial appears in the list.
// A nil receiver or nil serial reports false, so the handshake-time check is
// safe to call unconditionally.
func (r *RevocationList) IsRevoked(serial *big.Int) bool {
	if r == nil || serial == nil {
		return false
	}
	_, ok := r.revoked[serial.String()]
	return ok
}

// Count returns how many serials are revoked. Drives operator output and the
// silo_tls_revoked_certs gauge.
func (r *RevocationList) Count() int {
	if r == nil {
		return 0
	}
	return len(r.revoked)
}

// Serials returns the revoked serial numbers in ascending order, so 'siloctl ca
// revoke' can re-sign a CRL that extends the existing set without losing prior
// revocations.
func (r *RevocationList) Serials() []*big.Int {
	if r == nil {
		return nil
	}
	out := make([]*big.Int, len(r.serials))
	copy(out, r.serials)
	sort.Slice(out, func(i, j int) bool { return out[i].Cmp(out[j]) < 0 })
	return out
}

// Stale reports whether the CRL's NextUpdate has passed. silod still enforces a
// stale CRL (failing closed on all handshakes would be worse than an
// out-of-date revocation set) but logs it so the operator re-issues.
func (r *RevocationList) Stale() bool {
	if r == nil {
		return false
	}
	return nowFunc().After(r.NextUpdate)
}

// GenerateCRL signs a CRL listing the given serials, valid for validFor from
// now. The CA must hold its private key (the CA cert carries KeyUsageCRLSign
// from GenerateCA). number is the monotonic CRL sequence number — pass the
// previous CRL's Number+1 on re-issue so relying parties can detect a rollback
// to an older, less-complete list.
func GenerateCRL(ca *CA, serials []*big.Int, number *big.Int, validFor time.Duration) ([]byte, error) {
	if ca == nil || ca.Cert == nil {
		return nil, errors.New("silo: cannot sign a CRL without the cluster CA certificate; load it from SILO_TLS_CA_CERT first")
	}
	if ca.Key == nil {
		return nil, errors.New("silo: cannot sign a CRL without the CA private key; run 'siloctl ca revoke' on a host that holds SILO_TLS_CA_KEY")
	}
	if number == nil {
		return nil, errors.New("silo: a CRL needs a sequence number; pass big.NewInt(1) for the first issue and increment on each re-issue")
	}

	now := nowFunc()
	entries := make([]x509.RevocationListEntry, 0, len(serials))
	for _, s := range serials {
		if s == nil {
			return nil, errors.New("silo: cannot revoke a nil serial number; check the cert you pointed at parsed correctly")
		}
		entries = append(entries, x509.RevocationListEntry{
			SerialNumber:   s,
			RevocationTime: now,
		})
	}

	template := &x509.RevocationList{
		Number:                    number,
		ThisUpdate:                now.Add(-time.Minute),
		NextUpdate:                now.Add(validFor),
		RevokedCertificateEntries: entries,
	}
	der, err := x509CreateRevocationList(randReader, template, ca.Cert, ca.Key)
	if err != nil {
		return nil, fmt.Errorf("silo: could not sign the CRL (%w); please file a bug at https://github.com/hyperized/silo/issues", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: pemTypeCRL, Bytes: der}), nil
}

// LoadCRL parses a PEM-encoded CRL, verifies its signature against the cluster
// CA, and returns the revoked-serial set. A CRL that does not verify is
// rejected outright — silod would rather refuse to start than enforce an
// attacker-supplied revocation list.
func LoadCRL(crlPEM []byte, ca *CA) (*RevocationList, error) {
	if ca == nil || ca.Cert == nil {
		return nil, errors.New("silo: cannot verify a CRL without the cluster CA certificate")
	}
	block, _ := pem.Decode(crlPEM)
	if block == nil {
		return nil, errors.New("silo: CRL PEM block missing or unrecognised; point SILO_TLS_CRL at a file produced by 'siloctl ca revoke'")
	}
	if block.Type != pemTypeCRL {
		return nil, fmt.Errorf("silo: CRL PEM block is %q, want %q; the file at SILO_TLS_CRL is not a CRL", block.Type, pemTypeCRL)
	}
	crl, err := x509.ParseRevocationList(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("silo: could not parse the CRL (%w); regenerate it with 'siloctl ca revoke'", err)
	}
	if err := crl.CheckSignatureFrom(ca.Cert); err != nil {
		return nil, fmt.Errorf("silo: CRL signature does not verify against the cluster CA (%w); it was signed by a different CA or has been tampered with", err)
	}

	revoked := make(map[string]struct{}, len(crl.RevokedCertificateEntries))
	serials := make([]*big.Int, 0, len(crl.RevokedCertificateEntries))
	for _, e := range crl.RevokedCertificateEntries {
		key := e.SerialNumber.String()
		if _, dup := revoked[key]; dup {
			continue
		}
		revoked[key] = struct{}{}
		serials = append(serials, e.SerialNumber)
	}
	return &RevocationList{
		revoked:    revoked,
		serials:    serials,
		Number:     crl.Number,
		ThisUpdate: crl.ThisUpdate,
		NextUpdate: crl.NextUpdate,
	}, nil
}

// SerialOf parses a certificate PEM and returns its serial number, so an
// operator can revoke by pointing 'siloctl ca revoke' at the cert file instead
// of copying a hex serial by hand.
func SerialOf(certPEM []byte) (*big.Int, error) {
	cert, err := parseCertPEM(certPEM)
	if err != nil {
		return nil, fmt.Errorf("silo: could not read the certificate to revoke (%w)", err)
	}
	return cert.SerialNumber, nil
}
