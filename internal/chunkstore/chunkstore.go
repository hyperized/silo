// Package chunkstore persists immutable chunks on one silod node.
//
// Each chunk is written as a single encrypted envelope (see internal/crypto)
// using the temp-file + fsync + rename + fsync(dir) pattern. The file
// backend keeps every chunk in one directory for now; sharding by id
// prefix lands once a flat directory becomes inefficient.
package chunkstore

import (
	"context"
	"errors"
	"regexp"
	"time"
)

var (
	ErrNotFound  = errors.New("silo: chunk not found at this node — the chunk may live on a different replica or have been deleted; try another node, or restore from a healthy replica")
	ErrInvalidID = errors.New("silo: chunk id is invalid — chunk ids must be 1-128 characters consisting of ASCII letters, digits, dashes, and underscores")
)

// Info carries both the plaintext and on-disk sizes so callers (the
// gRPC server, the operator CLI, replication planning) can reason about
// the encryption overhead without re-stat-ing the file every time.
type Info struct {
	ID          string
	PlainBytes  int64
	StoredBytes int64
	CreatedAt   time.Time
}

// Store is the abstraction that lets the rest of silo treat the on-node
// chunk backing as opaque. FileStore is the only implementation today;
// LVM-thinpool and ZFS backends will plug in via the same interface so
// the gRPC service code never has to know which one is mounted.
type Store interface {
	Put(ctx context.Context, id string, data []byte) (Info, error)
	Get(ctx context.Context, id string) (data []byte, info Info, err error)
	Delete(ctx context.Context, id string) error
	Stat(ctx context.Context, id string) (Info, error)
}

// idPattern keeps ids filesystem-safe (no path separators, no dots, no
// whitespace) so they can be used as filenames without further escaping.
// The 128-char cap matches the longest expected derived hash size with
// generous headroom.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// ValidateID is exported so the gRPC service can fail invalid requests
// at the boundary before reading the body, instead of opening files
// with attacker-supplied paths.
func ValidateID(id string) error {
	if !idPattern.MatchString(id) {
		return ErrInvalidID
	}
	return nil
}
