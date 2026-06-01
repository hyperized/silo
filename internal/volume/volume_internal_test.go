package volume

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type errMeta struct{}

func (errMeta) ExtentSize(string) (int64, error)                          { return 4096, nil }
func (errMeta) Extent(string, uint64) (string, bool, error)               { return "", false, nil }
func (errMeta) WriteExtent(string, uint64, string, string) error          { return nil }
func (errMeta) WriteExtents(string, []uint64, []string, string) error     { return nil }

func TestOpen_EntropyFailure(t *testing.T) {
	prev := newWriterID
	t.Cleanup(func() { newWriterID = prev })
	newWriterID = func(string) (string, error) { return "", errors.New("no entropy") }

	if _, err := Open(context.Background(), errMeta{}, nil, "/v", "h"); err == nil || !strings.Contains(err.Error(), "entropy") {
		t.Fatalf("Open err = %v, want an entropy error", err)
	}
}
