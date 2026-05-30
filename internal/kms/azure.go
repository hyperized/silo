package kms

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"

	"github.com/hyperized/silo/internal/crypto"
)

// NewAzure returns a Decrypter backed by Azure Key Vault. The cluster key is
// wrapped with an RSA key in the vault (RSA-OAEP-256) and unwrapped at startup.
// vaultURL is the vault's base URL (https://<vault>.vault.azure.net/), keyName
// the key's name; the latest version is used. Credentials come from
// DefaultAzureCredential (env, managed identity, or az login).
func NewAzure(vaultURL, keyName string) crypto.Decrypter {
	return azureDecrypter{vaultURL: vaultURL, keyName: keyName}
}

type azureDecrypter struct {
	vaultURL string
	keyName  string
}

func (azureDecrypter) Name() string { return "azure-kv" }

func (d azureDecrypter) Decrypt(ctx context.Context, wrapped []byte) ([]byte, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("could not obtain Azure credentials (%w); set them via env, managed identity, or az login", err)
	}
	client, err := azkeys.NewClient(d.vaultURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create the Key Vault client for %s (%w)", d.vaultURL, err)
	}
	alg := azkeys.EncryptionAlgorithmRSAOAEP256
	// An empty version selects the key's current version.
	resp, err := client.UnwrapKey(ctx, d.keyName, "", azkeys.KeyOperationParameters{Algorithm: &alg, Value: wrapped}, nil)
	if err != nil {
		return nil, err
	}
	return resp.Result, nil
}
