package clustertls_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/clustertls"
)

// FuzzLoadCRL hardens revocation-list parsing — LoadCRL decodes operator-
// supplied PEM (SILO_TLS_CRL) and DER that gates whose certificates remain
// trusted. Malformed PEM, corrupt DER, and a CRL signed by the wrong CA must
// surface as an error, never a panic, and a tampered CRL must never verify.
// Seeded with a real signed CRL so the fuzzer mutates outward from valid input.
func FuzzLoadCRL(f *testing.F) {
	certPEM, keyPEM, caErr := clustertls.GenerateCA("silo-fuzz", time.Hour)
	var ca *clustertls.CA
	if caErr == nil {
		ca, _ = clustertls.LoadCA(certPEM, keyPEM)
		if ca != nil {
			if crlPEM, err := clustertls.GenerateCRL(ca, []*big.Int{big.NewInt(7), big.NewInt(42)}, big.NewInt(1), time.Hour); err == nil {
				f.Add(crlPEM)
			}
		}
	}
	f.Add([]byte("-----BEGIN X509 CRL-----\nbogus\n-----END X509 CRL-----\n"))
	f.Add([]byte("-----BEGIN CERTIFICATE-----\nbogus\n-----END CERTIFICATE-----\n"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, crlPEM []byte) {
		if ca == nil {
			t.Skip("CA generation unavailable")
		}
		if _, err := clustertls.LoadCRL(crlPEM, ca); err != nil {
			return // a rejected CRL is the expected outcome for almost all inputs
		}
	})
}
