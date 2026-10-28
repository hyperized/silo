package blobstore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// gcsTarget stores backup objects in a Google Cloud Storage bucket under an
// object-name prefix. The client is built once (from Application Default
// Credentials) and reused.
type gcsTarget struct {
	bucket string
	prefix string

	once    sync.Once
	client  *storage.Client
	initErr error
}

func newGCS(bucket, prefix string) *gcsTarget { return &gcsTarget{bucket: bucket, prefix: prefix} }

func (t *gcsTarget) Name() string { return "gs://" + t.bucket + "/" + t.prefix }

const gcsHint = "check that the bucket exists and that Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS or workload identity) allow this operation"

func (t *gcsTarget) init(ctx context.Context) (*storage.Client, error) {
	t.once.Do(func() {
		// The client outlives this call, so it must not inherit a per-request
		// deadline from ctx.
		client, err := storage.NewClient(context.WithoutCancel(ctx))
		if err != nil {
			t.initErr = fmt.Errorf("blobstore: could not create the Google Cloud Storage client (%w); %s", err, gcsHint)
			return
		}
		t.client = client
	})
	return t.client, t.initErr
}

func (t *gcsTarget) Put(ctx context.Context, name string, data []byte) error {
	if err := ValidateName(name); err != nil {
		return err
	}
	client, err := t.init(ctx)
	if err != nil {
		return err
	}
	w := client.Bucket(t.bucket).Object(objectKey(t.prefix, name)).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("blobstore: could not upload %s to %s (%w); %s", name, t.Name(), err, gcsHint)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("blobstore: could not upload %s to %s (%w); %s", name, t.Name(), err, gcsHint)
	}
	return nil
}

// PutIfAbsent uses the DoesNotExist precondition, which GCS enforces.
func (t *gcsTarget) PutIfAbsent(ctx context.Context, name string, data []byte) (bool, error) {
	if err := ValidateName(name); err != nil {
		return false, err
	}
	client, err := t.init(ctx)
	if err != nil {
		return false, err
	}
	w := client.Bucket(t.bucket).Object(objectKey(t.prefix, name)).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	_, werr := w.Write(data)
	cerr := w.Close()
	err = errors.Join(werr, cerr)
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == http.StatusPreconditionFailed {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("blobstore: could not upload %s to %s (%w); %s", name, t.Name(), err, gcsHint)
	}
	return true, nil
}

func (t *gcsTarget) Get(ctx context.Context, name string, maxBytes int64) ([]byte, error) {
	if err := ValidateName(name); err != nil {
		return nil, err
	}
	client, err := t.init(ctx)
	if err != nil {
		return nil, err
	}
	r, err := client.Bucket(t.bucket).Object(objectKey(t.prefix, name)).NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil, fmt.Errorf("%w: %s in %s", ErrNotFound, name, t.Name())
	}
	if err != nil {
		return nil, fmt.Errorf("blobstore: could not download %s from %s (%w); %s", name, t.Name(), err, gcsHint)
	}
	defer func() { _ = r.Close() }()
	return readCapped(r, maxBytes, name, t.Name())
}

func (t *gcsTarget) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := validatePrefix(prefix); err != nil {
		return nil, err
	}
	client, err := t.init(ctx)
	if err != nil {
		return nil, err
	}
	it := client.Bucket(t.bucket).Objects(ctx, &storage.Query{Prefix: listKey(t.prefix, prefix)})
	var out []ObjectInfo
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("blobstore: could not list %q in %s (%w); %s", prefix, t.Name(), err, gcsHint)
		}
		name, ok := relativeName(t.prefix, attrs.Name)
		if !ok {
			continue
		}
		out = append(out, ObjectInfo{Name: name, Size: attrs.Size, ModTime: attrs.Updated})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (t *gcsTarget) Delete(ctx context.Context, name string) error {
	if err := ValidateName(name); err != nil {
		return err
	}
	client, err := t.init(ctx)
	if err != nil {
		return err
	}
	err = client.Bucket(t.bucket).Object(objectKey(t.prefix, name)).Delete(ctx)
	if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("blobstore: could not delete %s from %s (%w); %s", name, t.Name(), err, gcsHint)
	}
	return nil
}
