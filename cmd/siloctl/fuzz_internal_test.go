package main

import (
	"os"
	"path/filepath"
	"testing"
)

// FuzzLoadAuthConfig hardens the reader for siloctl's own config.json,
// which an operator can hand-edit or corrupt. Arbitrary file contents must
// surface as an error, never a panic.
func FuzzLoadAuthConfig(f *testing.F) {
	dir := f.TempDir()
	path := filepath.Join(dir, "config.json")
	f.Add([]byte(`{"default_server":"127.0.0.1:7001","default_grpc_server":"127.0.0.1:7000"}`))
	f.Add([]byte(""))
	f.Add([]byte("garbage"))

	f.Fuzz(func(t *testing.T, data []byte) {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Skip()
		}
		_, _ = loadAuthConfig(dir)
	})
}
