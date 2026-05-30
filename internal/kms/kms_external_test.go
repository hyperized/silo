package kms_test

import (
	"testing"

	"github.com/hyperized/silo/internal/kms"
)

// The cloud Decrypt calls need real credentials and a real key, so they are not
// unit-tested (validated by compilation and a live deployment). What we can
// assert here is the construction and the source name each provider reports.
func TestProviderNames(t *testing.T) {
	cases := []struct {
		dec  interface{ Name() string }
		want string
	}{
		{kms.NewAWS("arn:aws:kms:..."), "aws-kms"},
		{kms.NewGCP("projects/p/locations/l/keyRings/r/cryptoKeys/k"), "gcp-kms"},
		{kms.NewAzure("https://v.vault.azure.net/", "wrapkey"), "azure-kv"},
	}
	for _, c := range cases {
		if c.dec == nil {
			t.Fatal("constructor returned nil")
		}
		if c.dec.Name() != c.want {
			t.Errorf("Name = %q, want %q", c.dec.Name(), c.want)
		}
	}
}
