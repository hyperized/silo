package clustertls_test

import (
	"testing"
	"time"

	"github.com/hyperized/silo/internal/clustertls"
)

// FuzzLoadCA hardens cluster CA parsing: LoadCA decodes operator- or
// volume-supplied PEM, and malformed or truncated material must surface as
// an error, never a panic. Seeded with a real CA so the fuzzer mutates
// outward from valid PEM.
func FuzzLoadCA(f *testing.F) {
	if certPEM, keyPEM, err := clustertls.GenerateCA("silo-fuzz", time.Hour); err == nil {
		f.Add(certPEM, keyPEM)
		f.Add(certPEM, []byte{})
	}
	f.Add([]byte("-----BEGIN CERTIFICATE-----\nbogus\n-----END CERTIFICATE-----\n"), []byte{})
	f.Add([]byte{}, []byte{})

	f.Fuzz(func(_ *testing.T, certPEM, keyPEM []byte) {
		_, _ = clustertls.LoadCA(certPEM, keyPEM)
	})
}
