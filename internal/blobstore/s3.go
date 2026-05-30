package blobstore

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sync"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3Target writes backup objects to an S3 bucket under a key prefix. The client
// is built once on first use (from the standard AWS credential chain) and
// reused for the rest of the backup run.
type s3Target struct {
	bucket string
	prefix string

	once    sync.Once
	client  *s3.Client
	initErr error
}

func newS3(bucket, prefix string) *s3Target { return &s3Target{bucket: bucket, prefix: prefix} }

func (t *s3Target) Name() string { return "s3://" + t.bucket + "/" + t.prefix }

func (t *s3Target) Put(ctx context.Context, name string, data []byte) error {
	t.once.Do(func() {
		cfg, err := awscfg.LoadDefaultConfig(ctx)
		if err != nil {
			t.initErr = fmt.Errorf("could not load AWS config (%w)", err)
			return
		}
		t.client = s3.NewFromConfig(cfg)
	})
	if t.initErr != nil {
		return t.initErr
	}
	key := path.Join(t.prefix, name)
	_, err := t.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &t.bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	return err
}
