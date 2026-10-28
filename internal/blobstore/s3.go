package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// s3Target stores backup objects in an S3 bucket under a key prefix. The
// client is built once on first use (from the standard AWS credential chain)
// and reused, so a run does not reload credentials per object.
type s3Target struct {
	bucket string
	prefix string
	opts   options

	once    sync.Once
	client  *s3.Client
	initErr error
}

func newS3(bucket, prefix string, o options) *s3Target {
	return &s3Target{bucket: bucket, prefix: prefix, opts: o}
}

// Name deliberately leaves out the endpoint: an operator-supplied endpoint URL
// is validated to carry no credentials, but the target URL alone is what
// operators recognise in logs.
func (t *s3Target) Name() string { return "s3://" + t.bucket + "/" + t.prefix }

func (t *s3Target) init(ctx context.Context) (*s3.Client, error) {
	t.once.Do(func() {
		var loadOpts []func(*awscfg.LoadOptions) error
		if t.opts.s3Region != "" {
			loadOpts = append(loadOpts, awscfg.WithRegion(t.opts.s3Region))
		}
		cfg, err := awscfg.LoadDefaultConfig(ctx, loadOpts...)
		if err != nil {
			t.initErr = fmt.Errorf("blobstore: could not load the AWS configuration (%w); check AWS_REGION, AWS_PROFILE and the shared AWS config files", err)
			return
		}
		t.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = t.opts.s3PathStyle
			if t.opts.s3Endpoint == "" {
				return
			}
			o.BaseEndpoint = aws.String(t.opts.s3Endpoint)
			// Since early 2025 the SDK adds CRC checksums to every upload by
			// default. Several S3-compatible services reject or mishandle
			// those headers, so against a custom endpoint only send them when
			// an operation requires it.
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		})
	})
	return t.client, t.initErr
}

// hint is appended to S3 errors so the operator knows which knobs to check.
func (t *s3Target) hint() string {
	if t.opts.s3Endpoint != "" {
		return "check SILO_BACKUP_S3_ENDPOINT, SILO_BACKUP_S3_REGION, SILO_BACKUP_S3_PATH_STYLE, that the bucket exists, and that AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY are valid for it"
	}
	return "check that the bucket exists, the region is right (AWS_REGION or SILO_BACKUP_S3_REGION), and the AWS credentials allow this operation"
}

func (t *s3Target) Put(ctx context.Context, name string, data []byte) error {
	if err := ValidateName(name); err != nil {
		return err
	}
	client, err := t.init(ctx)
	if err != nil {
		return err
	}
	key := objectKey(t.prefix, name)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &t.bucket,
		Key:           &key,
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("blobstore: could not upload %s to %s (%w); %s", name, t.Name(), err, t.hint())
	}
	return nil
}

// PutIfAbsent sends If-None-Match: *. AWS S3 and most current S3-compatible
// servers honour it; an old server that ignores the header overwrites instead,
// which the backup format tolerates except for the recorded checksum (see
// docs/known-gaps.md).
func (t *s3Target) PutIfAbsent(ctx context.Context, name string, data []byte) (bool, error) {
	if err := ValidateName(name); err != nil {
		return false, err
	}
	client, err := t.init(ctx)
	if err != nil {
		return false, err
	}
	key := objectKey(t.prefix, name)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &t.bucket,
		Key:           &key,
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		IfNoneMatch:   aws.String("*"),
	})
	if err != nil {
		if s3PreconditionFailed(err) {
			return false, nil
		}
		return false, fmt.Errorf("blobstore: could not upload %s to %s (%w); %s", name, t.Name(), err, t.hint())
	}
	return true, nil
}

func (t *s3Target) Get(ctx context.Context, name string, maxBytes int64) ([]byte, error) {
	if err := ValidateName(name); err != nil {
		return nil, err
	}
	client, err := t.init(ctx)
	if err != nil {
		return nil, err
	}
	key := objectKey(t.prefix, name)
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &t.bucket, Key: &key})
	if err != nil {
		if s3NotFound(err) {
			return nil, fmt.Errorf("%w: %s in %s", ErrNotFound, name, t.Name())
		}
		return nil, fmt.Errorf("blobstore: could not download %s from %s (%w); %s", name, t.Name(), err, t.hint())
	}
	defer func() { _ = out.Body.Close() }()
	if out.ContentLength != nil && *out.ContentLength > maxBytes {
		return nil, fmt.Errorf("%w: %s in %s is %d bytes, more than the %d allowed; the object was not written by silo or is damaged, remove it or pick another backup", ErrTooLarge, name, t.Name(), *out.ContentLength, maxBytes)
	}
	return readCapped(out.Body, maxBytes, name, t.Name())
}

// List pages through ListObjectsV2, which returns at most 1000 keys per call.
func (t *s3Target) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := validatePrefix(prefix); err != nil {
		return nil, err
	}
	client, err := t.init(ctx)
	if err != nil {
		return nil, err
	}
	keyPrefix := listKey(t.prefix, prefix)
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: &t.bucket, Prefix: &keyPrefix})
	var out []ObjectInfo
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("blobstore: could not list %q in %s (%w); %s", prefix, t.Name(), err, t.hint())
		}
		for _, obj := range page.Contents {
			name, ok := relativeName(t.prefix, aws.ToString(obj.Key))
			if !ok {
				continue
			}
			out = append(out, ObjectInfo{Name: name, Size: aws.ToInt64(obj.Size), ModTime: aws.ToTime(obj.LastModified)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Delete relies on S3's DeleteObject already succeeding for a missing key.
func (t *s3Target) Delete(ctx context.Context, name string) error {
	if err := ValidateName(name); err != nil {
		return err
	}
	client, err := t.init(ctx)
	if err != nil {
		return err
	}
	key := objectKey(t.prefix, name)
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &t.bucket, Key: &key}); err != nil && !s3NotFound(err) {
		return fmt.Errorf("blobstore: could not delete %s from %s (%w); %s", name, t.Name(), err, t.hint())
	}
	return nil
}

// s3PreconditionFailed recognises the answer to If-None-Match on an existing
// key.
func s3PreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed"
}

// s3NotFound recognises a missing key. AWS returns the typed NoSuchKey; some
// S3-compatible services only send a generic error with a NoSuchKey or
// NotFound code.
func s3NotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}
