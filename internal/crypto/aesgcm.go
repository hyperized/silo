// Package crypto encrypts silo chunk envelopes under the cluster encryption key.
//
// Envelope layout (96 bytes overhead, header + GCM tag):
//
//	0          4   magic            "SILO"
//	4          1   version          0x01
//	5          3   reserved         0
//	8         12   wrap_nonce
//	20        48   wrapped_dek      AES-256-GCM(cluster_key, data_key)
//	68        12   chunk_nonce
//	80         N   ciphertext+tag   AES-256-GCM(data_key, plaintext)
//
// Self-describing on purpose: while there is no metadata service, anyone
// holding the cluster key can decrypt without out-of-band state. The
// version byte reserves room to move the wrapped data key into inode
// metadata later without rewriting existing chunks.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

// ClusterKeyBytes is the required cluster-key length (AES-256).
const ClusterKeyBytes = 32

const (
	dataKeyBytes     = 32
	nonceBytes       = 12
	tagBytes         = 16
	magicSize        = 4
	versionSize      = 1
	reservedSize     = 3
	wrappedKeyOnDisk = nonceBytes + dataKeyBytes + tagBytes
	currentVersion   = byte(1)

	// HeaderBytes is the fixed-size envelope header. Ciphertext+tag follows.
	HeaderBytes = magicSize + versionSize + reservedSize + wrappedKeyOnDisk + nonceBytes
	// OverheadBytes is total on-disk overhead vs plaintext.
	OverheadBytes = HeaderBytes + tagBytes
)

var magic = [magicSize]byte{'S', 'I', 'L', 'O'}

// Sentinel errors. Each message is written as an instruction.
var (
	ErrTampered   = errors.New("silo: chunk failed authentication — the file has been modified, truncated, or encrypted with a different cluster key; restore from a healthy replica")
	ErrBadMagic   = errors.New("silo: file is not a silo chunk envelope — check the path or recreate the chunk from a healthy replica")
	ErrBadVersion = errors.New("silo: chunk envelope version is not supported by this silod — upgrade silod or restore the chunk in the current envelope format")
	ErrTooShort   = errors.New("silo: chunk envelope is too short to be valid — the file is likely truncated; restore from a healthy replica")
)

// Cipher encrypts and decrypts silo chunk envelopes under a fixed cluster
// encryption key. Safe for concurrent use: all state lives in the
// underlying AES-GCM AEAD, which crypto/cipher guarantees is reentrant.
type Cipher struct {
	wrap cipher.AEAD
}

// NewCipher returns a Cipher keyed with the cluster encryption key. The
// AES-GCM AEAD is built once here rather than per call because key
// scheduling is the expensive part of GCM; we want it amortised across
// every chunk a node handles.
func NewCipher(clusterKey []byte) (*Cipher, error) {
	if len(clusterKey) != ClusterKeyBytes {
		return nil, fmt.Errorf("silo: cluster encryption key must be %d bytes, got %d; regenerate with: openssl rand -base64 32", ClusterKeyBytes, len(clusterKey))
	}
	return &Cipher{wrap: newGCM(clusterKey)}, nil
}

// EncryptChunk produces a self-contained envelope: it embeds a fresh
// per-chunk data key, wrapped under the cluster key, so callers can store
// the envelope on disk without tracking key material out of band. The
// per-chunk key gives forward secrecy at the chunk level — leaking one
// data key cannot decrypt any other chunk. Empty plaintext is allowed.
func (c *Cipher) EncryptChunk(plaintext []byte) ([]byte, error) {
	dek, err := readRandom(dataKeyBytes, "per-chunk data key")
	if err != nil {
		return nil, err
	}
	wrapNonce, err := readRandom(nonceBytes, "wrap nonce")
	if err != nil {
		return nil, err
	}
	chunkNonce, err := readRandom(nonceBytes, "chunk nonce")
	if err != nil {
		return nil, err
	}

	wrappedDEK := c.wrap.Seal(nil, wrapNonce, dek, nil)
	ciphertext := newGCM(dek).Seal(nil, chunkNonce, plaintext, nil)

	out := make([]byte, 0, HeaderBytes+len(ciphertext))
	out = append(out, magic[:]...)
	out = append(out, currentVersion)
	out = append(out, 0, 0, 0)
	out = append(out, wrapNonce...)
	out = append(out, wrappedDEK...)
	out = append(out, chunkNonce...)
	out = append(out, ciphertext...)
	return out, nil
}

// DecryptChunk reverses EncryptChunk. All authentication failures
// (modified ciphertext, wrong cluster key, truncation past the header)
// collapse to a single ErrTampered. Distinguishing them would let an
// attacker probe the cluster key by submitting crafted envelopes.
func (c *Cipher) DecryptChunk(envelope []byte) ([]byte, error) {
	if len(envelope) < HeaderBytes+tagBytes {
		return nil, ErrTooShort
	}
	if [magicSize]byte(envelope[:magicSize]) != magic {
		return nil, ErrBadMagic
	}
	if envelope[magicSize] != currentVersion {
		return nil, ErrBadVersion
	}
	off := magicSize + versionSize + reservedSize
	wrapNonce := envelope[off : off+nonceBytes]
	off += nonceBytes
	wrappedDEK := envelope[off : off+dataKeyBytes+tagBytes]
	off += dataKeyBytes + tagBytes
	chunkNonce := envelope[off : off+nonceBytes]
	off += nonceBytes
	ciphertext := envelope[off:]

	dek, err := c.wrap.Open(nil, wrapNonce, wrappedDEK, nil)
	if err != nil {
		return nil, ErrTampered
	}
	plaintext, err := newGCM(dek).Open(nil, chunkNonce, ciphertext, nil)
	if err != nil {
		return nil, ErrTampered
	}
	return plaintext, nil
}

// readRandom names the purpose in its error message so an operator
// reading the silod log can tell which of EncryptChunk's three reads
// failed (data key, wrap nonce, chunk nonce).
func readRandom(n int, purpose string) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return nil, fmt.Errorf("silo: could not read random bytes for the %s (%v); the system entropy pool may be exhausted, check /dev/random availability", purpose, err)
	}
	return buf, nil
}

// newGCM builds an AES-GCM AEAD. It panics on non-32-byte keys because
// every caller here passes either the validated cluster key or a
// freshly-minted 32-byte data key — a failure means silo built a key
// wrong, not that the input was wrong, and surfacing it as a panic
// catches that bug loudly.
func newGCM(key []byte) cipher.AEAD {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(fmt.Sprintf("silo: aes.NewCipher failed for a %d-byte key (%v); please file a bug at https://github.com/hyperized/silo/issues", len(key), err))
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(fmt.Sprintf("silo: cipher.NewGCM failed (%v); please file a bug at https://github.com/hyperized/silo/issues", err))
	}
	return aead
}
