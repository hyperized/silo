// Package writer is silo's writer SDK. A writer opens an inode, derives its
// own chunk ids locally from its identity and a monotonic counter, and
// writes chunks straight to their primary without a metadata round-trip on
// the hot path. This file is the identity and chunk-id derivation the rest
// of the SDK builds on.
package writer

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// maxWriterPrefix bounds the node-derived portion of a writer id so that
// every derived chunk id stays within the chunk store's 128-character
// limit, even with a full epoch and counter appended.
const maxWriterPrefix = 32

// randRead is the entropy seam. Production uses crypto/rand.Read; tests
// override it to exercise the failure path.
var randRead = rand.Read

// NewWriterID mints a globally-unique, chunk-id-safe writer id rooted at the
// node id. The node portion is sanitised to the chunk-id alphabet and a
// random suffix distinguishes concurrent writers on the same node, so every
// chunk id derived from it (see ChunkID) is valid by construction.
func NewWriterID(nodeID string) (string, error) {
	var buf [8]byte
	if _, err := randRead(buf[:]); err != nil {
		return "", fmt.Errorf("writer: could not draw entropy for a writer id (%w); the host entropy pool may be exhausted", err)
	}
	return sanitize(nodeID) + "-" + hex.EncodeToString(buf[:]), nil
}

// ChunkID derives the id of the (epoch, counter)-th chunk a writer produces.
// It is deterministic — any node recomputes the same id from the same
// inputs — and, for a writer id from NewWriterID, always a valid chunk
// store id, so a writer never round-trips to learn where a chunk lives.
func ChunkID(writerID string, epoch, counter uint64) string {
	return fmt.Sprintf("%s-%d-%d", writerID, epoch, counter)
}

// sanitize maps a node id into the chunk-id alphabet ([A-Za-z0-9_-]),
// replacing anything else with '_' and capping the length. An empty result
// falls back to a constant so the writer id is never malformed.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		if b.Len() >= maxWriterPrefix {
			break
		}
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "node"
	}
	return b.String()
}
