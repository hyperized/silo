package namespace

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/hyperized/silo/internal/hlc"
)

// TestPersist_MarshalErrorIsLoggedNotFatal forces the serialization seam to
// fail, covering both the persist-on-mutation error path (logged, not
// fatal) and Snapshot's error return.
func TestPersist_MarshalErrorIsLoggedNotFatal(t *testing.T) {
	prev := marshalNamespace
	t.Cleanup(func() { marshalNamespace = prev })
	marshalNamespace = func(any) ([]byte, error) { return nil, errors.New("forced marshal failure") }

	ns, err := Open(hlc.New("a"), filepath.Join(t.TempDir(), "ns.json"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := ns.Mkdir("/x"); err != nil {
		t.Fatalf("Mkdir should succeed despite a marshal failure: %v", err)
	}
	if _, err := ns.Snapshot(); err == nil {
		t.Error("Snapshot should surface the marshal error")
	}
}
