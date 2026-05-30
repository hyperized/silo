package clustertls

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

// DefaultNodeCertLifetime is what LoadOrMintNode requests when minting.
// One year is short enough to encourage rotation tooling, long enough
// to survive the operator forgetting about it for a quarter.
const DefaultNodeCertLifetime = 365 * 24 * time.Hour

// RenewBefore is how long before expiry LoadOrMintNode re-mints an existing
// node cert. Set to a third of the lifetime so any restart in the cert's final
// ~4 months refreshes it — comfortably ahead of expiry for a cluster that does
// rolling upgrades. (A background rotation loop for long-uptime nodes is future
// work; see docs/known-gaps.md.)
const RenewBefore = DefaultNodeCertLifetime / 3

// nowFunc is the clock the rotation check reads; overridable in tests.
var nowFunc = time.Now

// LoadOrMintNode returns the per-node TLS material from dir, minting a
// fresh pair (signed by the CA) if no cert exists yet. The cert and key
// are written atomically with 0o600 permissions; subsequent boots load
// the same pair instead of regenerating, so peers don't see a churning
// identity.
func LoadOrMintNode(dir string, ca *CA, nodeID string, dnsNames []string, ipAddrs []net.IP) (*NodeCert, error) {
	if dir == "" {
		return nil, errors.New("silo: cluster TLS storage directory is empty; set SILO_DATA_DIR to a writable path so silod can persist its node certificate")
	}
	certPath := filepath.Join(dir, "node.crt")
	keyPath := filepath.Join(dir, "node.key")

	if nc, ok := loadIfPresent(certPath, keyPath); ok {
		// Keep the existing cert unless it is close to expiry and we hold the CA
		// key to mint a replacement. A node without the CA key cannot rotate, so
		// it serves the old cert until an operator refreshes it.
		if !needsRenewal(nc) || ca == nil || ca.Key == nil {
			return nc, nil
		}
		// else: fall through to mint a fresh cert (rotation).
	} else if ca == nil || ca.Key == nil {
		return nil, fmt.Errorf("silo: no node certificate at %s and no CA private key available to mint one; either copy this node's cert and key into %s manually, or set SILO_TLS_CA_KEY so silod can bootstrap its own", certPath, dir)
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("silo: could not create the TLS material directory at %s (%w); check the path is on a writable filesystem and silod has permission", dir, err)
	}

	nc, err := MintNodeCert(ca, nodeID, dnsNames, ipAddrs, DefaultNodeCertLifetime)
	if err != nil {
		return nil, err
	}

	if err := writeAtomic(certPath, nc.CertPEM); err != nil {
		return nil, fmt.Errorf("silo: could not write the new node certificate to %s (%w); check the data directory has free space and silod can write to it", certPath, err)
	}
	if err := writeAtomic(keyPath, nc.KeyPEM); err != nil {
		_ = os.Remove(certPath)
		return nil, fmt.Errorf("silo: could not write the new node key to %s (%w); the partial cert at %s was removed — check the data directory has free space and silod can write to it", keyPath, err, certPath)
	}
	return nc, nil
}

// needsRenewal reports whether a loaded node cert is within RenewBefore of
// expiry (or unparseable, which a fresh mint will fix).
func needsRenewal(nc *NodeCert) bool {
	notAfter, err := nc.NotAfter()
	if err != nil {
		return true
	}
	return notAfter.Sub(nowFunc()) < RenewBefore
}

func loadIfPresent(certPath, keyPath string) (*NodeCert, bool) {
	// Paths come from operator-supplied config (SILO_TLS_*), not request
	// input, so directory traversal is not a concern here.
	certPEM, err := os.ReadFile(certPath) // #nosec G304
	if err != nil {
		return nil, false
	}
	keyPEM, err := os.ReadFile(keyPath) // #nosec G304
	if err != nil {
		return nil, false
	}
	return &NodeCert{CertPEM: certPEM, KeyPEM: keyPEM}, true
}

// syncCloser is the subset of *os.File writeAtomic touches. Tests swap
// openExclusiveFile to return a fake so the post-open error paths
// (write, sync, close, rename) are reachable without simulating an
// actual disk fault.
type syncCloser interface {
	io.Writer
	Sync() error
	Close() error
}

// File-system seams used by writeAtomic. Production code points them at
// the os package; tests swap them per test under t.Cleanup.
var (
	openExclusiveFile = func(path string, mode os.FileMode) (syncCloser, error) {
		// path is derived from operator-supplied config, not request input.
		return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_EXCL, mode) // #nosec G304
	}
	osRename = os.Rename
	osRemove = os.Remove
)

// writeAtomic writes data via a temp file + rename so a crash leaves
// either the previous file or the new one, never a torn one. TLS material is
// always written 0o600 (owner read/write only).
func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := openExclusiveFile(tmp, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = osRemove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = osRemove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = osRemove(tmp)
		return err
	}
	return osRename(tmp, path)
}
