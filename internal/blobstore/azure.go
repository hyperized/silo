package blobstore

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// azureTarget writes backup objects to an Azure Blob container under a blob-name
// prefix. account is the storage account name (mapped to
// https://account.blob.core.windows.net). The client is built once (from
// DefaultAzureCredential) and reused across the run.
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

func (t *azureTarget) Put(ctx context.Context, name string, data []byte) error {
	t.once.Do(func() {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			t.initErr = fmt.Errorf("could not obtain Azure credentials (%w)", err)
			return
		}
		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", t.account)
		client, err := azblob.NewClient(serviceURL, cred, nil)
		if err != nil {
			t.initErr = fmt.Errorf("could not create the Azure Blob client for %s (%w)", t.account, err)
			return
		}
		t.client = client
	})
	if t.initErr != nil {
		return t.initErr
	}
	_, err := t.client.UploadBuffer(ctx, t.container, path.Join(t.prefix, name), data, nil)
	return err
}
