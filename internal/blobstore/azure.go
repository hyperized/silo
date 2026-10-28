package blobstore

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// azureTarget stores backup objects in an Azure Blob container under a
// blob-name prefix. account is the storage account name (mapped to
// https://account.blob.core.windows.net). The client is built once (from
// DefaultAzureCredential) and reused.
type azureTarget struct {
	account   string
	container string
	prefix    string

	once    sync.Once
	client  *azblob.Client
	initErr error
}

func newAzure(account, container, prefix string) *azureTarget {
	return &azureTarget{account: account, container: container, prefix: prefix}
}

func (t *azureTarget) Name() string {
	return "az://" + t.account + "/" + t.container + "/" + t.prefix
}

const azureHint = "check that the storage account and container exist and that the Azure identity (AZURE_* variables or managed identity) has the Storage Blob Data Contributor role"

func (t *azureTarget) init() (*azblob.Client, error) {
	t.once.Do(func() {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			t.initErr = fmt.Errorf("blobstore: could not obtain Azure credentials (%w); %s", err, azureHint)
			return
		}
		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", t.account)
		client, err := azblob.NewClient(serviceURL, cred, nil)
		if err != nil {
			t.initErr = fmt.Errorf("blobstore: could not create the Azure Blob client for %s (%w); %s", t.account, err, azureHint)
			return
		}
		t.client = client
	})
	return t.client, t.initErr
}

func (t *azureTarget) Put(ctx context.Context, name string, data []byte) error {
	if err := ValidateName(name); err != nil {
		return err
	}
	client, err := t.init()
	if err != nil {
		return err
	}
	if _, err := client.UploadBuffer(ctx, t.container, objectKey(t.prefix, name), data, nil); err != nil {
		return fmt.Errorf("blobstore: could not upload %s to %s (%w); %s", name, t.Name(), err, azureHint)
	}
	return nil
}

// PutIfAbsent sends If-None-Match: *, which Azure Blob enforces.
func (t *azureTarget) PutIfAbsent(ctx context.Context, name string, data []byte) (bool, error) {
	if err := ValidateName(name); err != nil {
		return false, err
	}
	client, err := t.init()
	if err != nil {
		return false, err
	}
	etagAny := azcore.ETagAny
	opts := &azblob.UploadBufferOptions{AccessConditions: &blob.AccessConditions{
		ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: &etagAny},
	}}
	_, err = client.UploadBuffer(ctx, t.container, objectKey(t.prefix, name), data, opts)
	if bloberror.HasCode(err, bloberror.BlobAlreadyExists, bloberror.ConditionNotMet) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("blobstore: could not upload %s to %s (%w); %s", name, t.Name(), err, azureHint)
	}
	return true, nil
}

func (t *azureTarget) Get(ctx context.Context, name string, maxBytes int64) ([]byte, error) {
	if err := ValidateName(name); err != nil {
		return nil, err
	}
	client, err := t.init()
	if err != nil {
		return nil, err
	}
	resp, err := client.DownloadStream(ctx, t.container, objectKey(t.prefix, name), nil)
	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		return nil, fmt.Errorf("%w: %s in %s", ErrNotFound, name, t.Name())
	}
	if err != nil {
		return nil, fmt.Errorf("blobstore: could not download %s from %s (%w); %s", name, t.Name(), err, azureHint)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.ContentLength != nil && *resp.ContentLength > maxBytes {
		return nil, fmt.Errorf("%w: %s in %s is %d bytes, more than the %d allowed; the object was not written by silo or is damaged, remove it or pick another backup", ErrTooLarge, name, t.Name(), *resp.ContentLength, maxBytes)
	}
	return readCapped(resp.Body, maxBytes, name, t.Name())
}

func (t *azureTarget) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := validatePrefix(prefix); err != nil {
		return nil, err
	}
	client, err := t.init()
	if err != nil {
		return nil, err
	}
	pager := client.NewListBlobsFlatPager(t.container, &azblob.ListBlobsFlatOptions{Prefix: to.Ptr(listKey(t.prefix, prefix))})
	var out []ObjectInfo
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("blobstore: could not list %q in %s (%w); %s", prefix, t.Name(), err, azureHint)
		}
		for _, item := range page.Segment.BlobItems {
			if item == nil || item.Name == nil {
				continue
			}
			name, ok := relativeName(t.prefix, *item.Name)
			if !ok {
				continue
			}
			info := ObjectInfo{Name: name}
			if p := item.Properties; p != nil {
				if p.ContentLength != nil {
					info.Size = *p.ContentLength
				}
				if p.LastModified != nil {
					info.ModTime = *p.LastModified
				}
			}
			out = append(out, info)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (t *azureTarget) Delete(ctx context.Context, name string) error {
	if err := ValidateName(name); err != nil {
		return err
	}
	client, err := t.init()
	if err != nil {
		return err
	}
	if _, err := client.DeleteBlob(ctx, t.container, objectKey(t.prefix, name), nil); err != nil && !bloberror.HasCode(err, bloberror.BlobNotFound) {
		return fmt.Errorf("blobstore: could not delete %s from %s (%w); %s", name, t.Name(), err, azureHint)
	}
	return nil
}
