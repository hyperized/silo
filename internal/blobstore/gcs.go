package blobstore

import (
	"context"
	"fmt"
	"path"
	"sync"

	"cloud.google.com/go/storage"
)

// gcsTarget writes backup objects to a Google Cloud Storage bucket under an
// object-name prefix. The client is built once (from Application Default
// Credentials) and reused across the run.
type gcsTarget struct {
	bucket string
	prefix string

	once    sync.Once
	client  *storage.Client
	initErr error
}

func newGCS(bucket, prefix string) *gcsTarget { return &gcsTarget{bucket: bucket, prefix: prefix} }

func (t *gcsTarget) Name() string { return "gs://" + t.bucket + "/" + t.prefix }

func (t *gcsTarget) Put(ctx context.Context, name string, data []byte) error {
	t.once.Do(func() {
		client, err := storage.NewClient(ctx)
		if err != nil {
			t.initErr = fmt.Errorf("could not create the GCS client (%w); check Application Default Credentials", err)
			return
		}
		t.client = client
	})
	if t.initErr != nil {
		return t.initErr
	}
	w := t.client.Bucket(t.bucket).Object(path.Join(t.prefix, name)).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}
