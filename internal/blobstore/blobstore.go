// Package blobstore is silo's write side for backups: a small Target interface
// (Put a named object) with a local-filesystem implementation and adapters for
// S3, Google Cloud Storage, and Azure Blob. Operators select one with a URL
// (s3://bucket/prefix, gs://…, az://…, or a local path), so the backup
// subsystem stays cloud-agnostic. As with the KMS adapters, the cloud SDKs are
// isolated here and their network calls are validated by a live deployment, not
// unit tests; the local target is fully tested.
package blobstore

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// Target is an object-storage backend a backup writes to.
type Target interface {
	// Put stores data under name (a relative path joined to the target's
	// prefix). Overwriting an existing object is allowed.
	Put(ctx context.Context, name string, data []byte) error
	// Name identifies the target in logs (e.g. "s3://bucket/prefix").
	Name() string
}

// Open resolves a target URL to a Target. Recognised forms:
//
//	/path or file:///path     local filesystem (testing, single-host backup)
//	s3://bucket/prefix         AWS S3
//	gs://bucket/prefix         Google Cloud Storage
//	az://account/container/p   Azure Blob (account -> https://account.blob.core.windows.net)
//
// Credentials for the cloud targets come from each provider's standard chain.
func Open(rawURL string) (Target, error) {
	if rawURL == "" {
		return nil, fmt.Errorf("blobstore: empty target; set a path or s3://, gs://, az:// URL")
	}
	if !strings.Contains(rawURL, "://") {
		return newLocal(rawURL), nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("blobstore: could not parse target %q (%w)", rawURL, err)
	}
	prefix := strings.TrimPrefix(u.Path, "/")
	switch u.Scheme {
	case "file":
		return newLocal(u.Path), nil
	case "s3":
		return newS3(u.Host, prefix), nil
	case "gs":
		return newGCS(u.Host, prefix), nil
	case "az":
		container, blobPrefix, ok := strings.Cut(prefix, "/")
		if !ok && container == "" {
			return nil, fmt.Errorf("blobstore: az target needs az://account/container[/prefix], got %q", rawURL)
		}
		return newAzure(u.Host, container, blobPrefix), nil
	default:
		return nil, fmt.Errorf("blobstore: unsupported target scheme %q in %q; use file, s3, gs, or az", u.Scheme, rawURL)
	}
}
