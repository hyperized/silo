// Package kms holds the cloud-KMS adapters that unwrap silo's cluster
// encryption key. Each implements crypto.Decrypter for one provider (AWS KMS,
// GCP Cloud KMS, Azure Key Vault). Isolating them here keeps the cloud SDKs out
// of every other package; only silod, which wires the key provider, pulls them.
//
// The cloud calls cannot be exercised without real credentials and a real key,
// so these adapters are validated by compilation and a live deployment, not by
// unit tests — the same treatment as the kernel-facing seams elsewhere.
package kms

import (
	"context"
	"fmt"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"

	"github.com/hyperized/silo/internal/crypto"
)

// NewAWS returns a Decrypter backed by AWS KMS. keyID is optional (KMS derives
// the key from the ciphertext) but recommended as a guard. Credentials come
// from the standard AWS chain (env, shared config, or the instance/IRSA role).
func NewAWS(keyID string) crypto.Decrypter { return awsDecrypter{keyID: keyID} }

type awsDecrypter struct{ keyID string }

func (awsDecrypter) Name() string { return "aws-kms" }

func (d awsDecrypter) Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error) {
	cfg, err := awscfg.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not load AWS config (%w); provide credentials via env, shared config, or an IAM role", err)
	}
	in := &awskms.DecryptInput{CiphertextBlob: ciphertext}
	if d.keyID != "" {
		in.KeyId = &d.keyID
	}
	out, err := awskms.NewFromConfig(cfg).Decrypt(ctx, in)
	if err != nil {
		return nil, err
	}
	return out.Plaintext, nil
}
