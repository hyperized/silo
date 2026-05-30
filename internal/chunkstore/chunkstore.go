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

// Sentinel errors callers match with errors.Is to branch on outcome
// (e.g. the gRPC layer maps ErrNotFound to codes.NotFound). The message
// text is the human-facing instruction; the identity is the contract.
var (
	ErrNotFound  = errors.New("silo: chunk not found at this node — the chunk may live on a different replica or have been deleted; try another node, or restore from a healthy replica")
	ErrInvalidID = errors.New("silo: chunk id is invalid — chunk ids must be 1-128 characters consisting of ASCII letters, digits, dashes, and underscores")
	// ErrNoSpace is returned by Put when the data directory is at or above the
	// hard disk watermark. The coordinator treats a refused replica as a failed
	// ack (the write still completes on other replicas if quorum is met, and the
	// scrubber heals this node once it has room); the gRPC layer maps it to
	// codes.ResourceExhausted so clients can back off rather than retry-storm.
	ErrNoSpace = errors.New("silo: node is at its disk high-watermark and is refusing new chunks — add capacity or drain this node; existing chunks are still served")
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
	// List returns the ids of every chunk held on this node. The
	// re-replication scrubber walks it to find chunks whose replica set is
	// incomplete. Order is unspecified.
	List(ctx context.Context) ([]string, error)
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
