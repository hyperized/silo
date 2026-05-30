package kms

import (
	"context"
	"fmt"

	gcpkms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"

	"github.com/hyperized/silo/internal/crypto"
)

// NewGCP returns a Decrypter backed by GCP Cloud KMS. keyName is the full
// crypto-key resource name
// (projects/P/locations/L/keyRings/R/cryptoKeys/K). Credentials come from
// Application Default Credentials (env, gcloud, or Workload Identity).
func NewGCP(keyName string) crypto.Decrypter { return gcpDecrypter{keyName: keyName} }

type gcpDecrypter struct{ keyName string }

func (gcpDecrypter) Name() string { return "gcp-kms" }

func (d gcpDecrypter) Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error) {
	client, err := gcpkms.NewKeyManagementClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not create the Cloud KMS client (%w); check Application Default Credentials", err)
	}
	defer func() { _ = client.Close() }()
	resp, err := client.Decrypt(ctx, &kmspb.DecryptRequest{Name: d.keyName, Ciphertext: ciphertext})
	if err != nil {
		return nil, err
	}
	return resp.Plaintext, nil
}
