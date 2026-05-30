package crypto

import (
	"fmt"
	"os"
)

// KeyProvider supplies the cluster encryption key. It is the pluggable seam
// behind SILO_ENCRYPTION_KEY_SOURCE: silod resolves a provider at startup and
// asks it for the key once, so adding a source (a KMS, Vault, an OIDC-fronted
// service) is a new KeyProvider rather than a change to the cipher.
type KeyProvider interface {
	// ClusterKey returns the raw cluster key. It must be ClusterKeyBytes long.
	ClusterKey() ([]byte, error)
	// SourceName identifies the provider in logs/errors (e.g. "static", "file").
	SourceName() string
}

// StaticKeyProvider serves an already-decoded key held in memory (the dev
// SILO_ENCRYPTION_KEY path). The bytes are validated when the cipher is built.
func StaticKeyProvider(key []byte) KeyProvider { return staticProvider{key: key} }

type staticProvider struct{ key []byte }

func (p staticProvider) ClusterKey() ([]byte, error) { return p.key, nil }
func (p staticProvider) SourceName() string          { return "static" }

// FileKeyProvider reads the key from a file of exactly ClusterKeyBytes raw bytes
// (the single-node production path). The file is read each time silod starts, so
// rotating the key is "replace the file and restart".
func FileKeyProvider(path string) KeyProvider { return fileProvider{path: path} }

type fileProvider struct{ path string }

func (p fileProvider) ClusterKey() ([]byte, error) {
	key, err := os.ReadFile(p.path) //nolint:gosec // operator-configured key path, not request input
	if err != nil {
		return nil, fmt.Errorf("silo: could not read the encryption key at %s (%w); create one with: openssl rand %d > %s && chmod 0400 %s", p.path, err, ClusterKeyBytes, p.path, p.path)
	}
	if len(key) != ClusterKeyBytes {
		return nil, fmt.Errorf("silo: the encryption key file %s holds %d bytes, but the cluster key must be exactly %d (raw, not base64); recreate it with: openssl rand %d > %s", p.path, len(key), ClusterKeyBytes, ClusterKeyBytes, p.path)
	}
	return key, nil
}

func (p fileProvider) SourceName() string { return "file" }
