// Package blobstore is silo's object-storage layer for backups: a small Target
// interface (put, get, list and delete named objects) with a local-filesystem
// implementation and adapters for S3 (and S3-compatible services), Google Cloud
// Storage and Azure Blob. Operators select one with a URL (s3://bucket/prefix,
// gs://…, az://…, or a local path), so the backup subsystem stays
// cloud-agnostic. As with the KMS adapters, the cloud SDKs are isolated here and
// their network calls are validated by a live deployment (and, for S3, by the
// MinIO integration test), not unit tests; the local target is fully tested.
package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

// ErrNotFound is returned (wrapped) by Target.Get when the object does not
// exist. Callers branch on it with errors.Is.
var ErrNotFound = errors.New("blobstore: object not found")

// ErrInvalidName is returned (wrapped) when an object name is not a clean,
// relative, slash-separated path.
var ErrInvalidName = errors.New("blobstore: invalid object name")

// ErrTooLarge is returned (wrapped) by Target.Get when the object is bigger
// than the caller's limit. Nothing beyond the limit is read.
var ErrTooLarge = errors.New("blobstore: object is larger than allowed")

// ObjectInfo describes one stored object. Name is relative to the target's
// prefix, in the same form Put accepts.
type ObjectInfo struct {
	Name    string
	Size    int64
	ModTime time.Time
}

// Target is an object-storage backend backups are written to and restored
// from. Object names are relative, slash-separated paths joined to the
// target's prefix; every method rejects names that are absolute, contain ".."
// or "." segments, backslashes or NUL bytes, with ErrInvalidName.
//
// Implementations are safe for concurrent use.
type Target interface {
	// Put stores data under name. Overwriting an existing object is allowed,
	// and a reader never observes a partly written object.
	Put(ctx context.Context, name string, data []byte) error
	// PutIfAbsent stores data under name only when no object exists there
	// yet, and reports whether it did. An existing object is left untouched
	// and created is false. Backends that ignore the condition degrade to
	// Put; see the per-backend notes.
	PutIfAbsent(ctx context.Context, name string, data []byte) (created bool, err error)
	// Get returns the object stored under name, or an error wrapping
	// ErrNotFound when there is none. Objects larger than maxBytes are
	// refused with an error wrapping ErrTooLarge, after reading at most
	// maxBytes+1 bytes, so a hostile bucket cannot exhaust memory.
	Get(ctx context.Context, name string, maxBytes int64) ([]byte, error)
	// List returns every object whose name starts with prefix, sorted by name.
	// It pages through the backend internally, so the result is complete
	// however many objects match. An empty prefix lists the whole target.
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	// Delete removes the object stored under name. Deleting an object that
	// does not exist is not an error.
	Delete(ctx context.Context, name string) error
	// Name identifies the target in logs and status output (for example
	// "s3://bucket/prefix"). It never contains credentials.
	Name() string
}

// Option configures how Open builds a Target. Options that do not apply to the
// selected scheme are ignored.
type Option func(*options)

type options struct {
	s3Endpoint  string
	s3Region    string
	s3PathStyle bool
}

// WithS3Endpoint points an s3:// target at an S3-compatible service (MinIO,
// Ceph RGW, Backblaze B2, Hetzner, Scaleway and the like) instead of AWS. The
// value is a base URL such as https://minio.example.com:9000.
func WithS3Endpoint(endpoint string) Option {
	return func(o *options) { o.s3Endpoint = endpoint }
}

// WithS3Region sets the region an s3:// target signs requests for. Empty keeps
// the AWS SDK's own resolution (AWS_REGION, the shared config file, and so on).
func WithS3Region(region string) Option {
	return func(o *options) { o.s3Region = region }
}

// WithS3PathStyle makes an s3:// target address buckets as
// https://endpoint/bucket/key instead of https://bucket.endpoint/key. Most
// self-hosted S3-compatible services need it.
func WithS3PathStyle(enabled bool) Option {
	return func(o *options) { o.s3PathStyle = enabled }
}

// Open resolves a target URL to a Target. Recognised forms:
//
//	/path or file:///path     local filesystem (testing, single-host backup)
//	s3://bucket/prefix         AWS S3, or an S3-compatible service with WithS3Endpoint
//	gs://bucket/prefix         Google Cloud Storage
//	az://account/container/p   Azure Blob (account -> https://account.blob.core.windows.net)
//
// Credentials for the cloud targets come from each provider's standard chain
// and are never part of the URL; a URL carrying user:password@ is rejected.
func Open(rawURL string, opts ...Option) (Target, error) {
	if rawURL == "" {
		return nil, errors.New("blobstore: the backup target is empty; set SILO_BACKUP_TARGET to a directory or an s3://, gs:// or az:// URL")
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if !strings.Contains(rawURL, "://") {
		return newLocal(rawURL), nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		// url.Parse echoes the input, so do not wrap its error: the URL may
		// hold a secret the operator pasted by mistake.
		return nil, errors.New("blobstore: the backup target is not a valid URL; use a form like s3://bucket/prefix, gs://bucket/prefix, az://account/container/prefix or a local directory")
	}
	if u.User != nil {
		return nil, errors.New("blobstore: the backup target URL contains a user name or password; remove it and supply credentials through the provider's standard variables (for S3: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)")
	}
	prefix := strings.Trim(u.Path, "/")
	switch u.Scheme {
	case "file":
		return newLocal(u.Path), nil
	case "s3":
		if u.Host == "" {
			return nil, fmt.Errorf("blobstore: the s3 target %q has no bucket; use s3://bucket or s3://bucket/prefix", rawURL)
		}
		return newS3(u.Host, prefix, o), nil
	case "gs":
		if u.Host == "" {
			return nil, fmt.Errorf("blobstore: the gs target %q has no bucket; use gs://bucket or gs://bucket/prefix", rawURL)
		}
		return newGCS(u.Host, prefix), nil
	case "az":
		container, blobPrefix, _ := strings.Cut(prefix, "/")
		if u.Host == "" || container == "" {
			return nil, fmt.Errorf("blobstore: the az target %q needs an account and a container; use az://account/container or az://account/container/prefix", rawURL)
		}
		return newAzure(u.Host, container, blobPrefix), nil
	default:
		return nil, fmt.Errorf("blobstore: unsupported target scheme %q in %q; use file, s3, gs or az", u.Scheme, rawURL)
	}
}

// ValidateS3Endpoint checks a custom S3 endpoint: an http:// or https:// base
// URL with a host and no credentials, query or fragment. Errors never repeat
// the value, because an endpoint pasted with a secret in it would otherwise
// end up in logs.
func ValidateS3Endpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("the S3 endpoint must be an http:// or https:// URL with a host, for example https://minio.example.com:9000")
	}
	if u.User != nil {
		return errors.New("the S3 endpoint must not contain a user name or password; put the keys in AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY instead")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return errors.New("the S3 endpoint must be a plain base URL without ?query or #fragment, for example https://minio.example.com:9000")
	}
	return nil
}

// ValidateName reports whether name is a clean relative object name: non-empty,
// slash-separated, no leading slash, no empty, "." or ".." segments, and no
// backslash or control characters. Every Target method enforces it; it is
// exported so restore code can check names read back from a manifest before
// using them as file paths.
func ValidateName(name string) error {
	if name == "" || strings.HasPrefix(name, "/") || strings.ContainsAny(name, "\\") {
		return fmt.Errorf("%w %q", ErrInvalidName, name)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w %q", ErrInvalidName, name)
		}
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w %q", ErrInvalidName, name)
		}
	}
	return nil
}

// readCapped reads r to the end, failing with ErrTooLarge once more than
// maxBytes arrive.
func readCapped(r io.Reader, maxBytes int64, name, target string) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("blobstore: the download of %s from %s was interrupted (%w); retry, and check the network path to the storage", name, target, err)
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%w: %s in %s exceeds %d bytes; the object was not written by silo or is damaged, remove it or pick another backup", ErrTooLarge, name, target, maxBytes)
	}
	return data, nil
}

// validatePrefix accepts "" (everything) or a name that may end in a slash.
func validatePrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	return ValidateName(strings.TrimSuffix(prefix, "/"))
}

// objectKey joins a target's key prefix and a relative object name.
func objectKey(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}

// listKey turns a relative list prefix into a backend key prefix. The trailing
// slash of "chunks/" must survive, or the listing would also match "chunksX".
func listKey(prefix, listPrefix string) string {
	if prefix == "" {
		return listPrefix
	}
	if listPrefix == "" {
		return prefix + "/"
	}
	return prefix + "/" + listPrefix
}

// relativeName strips a target's key prefix from a backend key. ok is false
// for keys outside the prefix, which a backend listing should never return but
// which are skipped rather than trusted.
func relativeName(prefix, key string) (string, bool) {
	if prefix == "" {
		return key, key != ""
	}
	rel, ok := strings.CutPrefix(key, prefix+"/")
	return rel, ok && rel != ""
}
