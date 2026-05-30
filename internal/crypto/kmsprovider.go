package crypto

import (
	"context"
	"fmt"
	"os"
	"time"
)

// Decrypter unwraps a KMS-encrypted cluster key. The cloud adapters in
// internal/kms (AWS KMS, GCP Cloud KMS, Azure Key Vault) implement it; the
// crypto package stays free of any cloud SDK so only silod pulls them in.
type Decrypter interface {
	// Decrypt returns the plaintext for a KMS-encrypted ciphertext blob.
	Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error)
	// Name identifies the KMS source in logs/errors (e.g. "aws-kms").
	Name() string
}

// kmsDecryptTimeout bounds the one startup call to the cloud KMS.
const kmsDecryptTimeout = 30 * time.Second

// KMSKeyProvider builds a KeyProvider that recovers the cluster key with
// envelope encryption: the operator wraps a freshly-generated 32-byte cluster
// key with a cloud KMS key and stores the ciphertext at ciphertextPath; silod
// decrypts it once at startup. The cluster key plaintext never lives on disk.
func KMSKeyProvider(d Decrypter, ciphertextPath string) KeyProvider {
	return kmsProvider{d: d, path: ciphertextPath}
}

type kmsProvider struct {
	d    Decrypter
	path string
}

func (p kmsProvider) ClusterKey() ([]byte, error) {
	if p.path == "" {
		return nil, fmt.Errorf("silo: the %s key source needs SILO_ENCRYPTION_KEY_PATH pointing at the KMS-wrapped key file", p.d.Name())
	}
	ct, err := os.ReadFile(p.path) //nolint:gosec // operator-configured path, not request input
	if err != nil {
		return nil, fmt.Errorf("silo: could not read the KMS-wrapped key at %s (%w); wrap a 32-byte key with your KMS and write the ciphertext there", p.path, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), kmsDecryptTimeout)
	defer cancel()
	key, err := p.d.Decrypt(ctx, ct)
	if err != nil {
		return nil, fmt.Errorf("silo: %s could not decrypt the cluster key (%w); check the key id and the process's cloud credentials", p.d.Name(), err)
	}
	if len(key) != ClusterKeyBytes {
		return nil, fmt.Errorf("silo: the decrypted cluster key is %d bytes, want %d; re-wrap a freshly generated 32-byte key", len(key), ClusterKeyBytes)
	}
	return key, nil
}

func (p kmsProvider) SourceName() string { return p.d.Name() }
