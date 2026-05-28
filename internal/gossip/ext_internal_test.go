package gossip

import (
	"errors"
	"testing"

	"github.com/hyperized/silo/internal/membership"
)

type fakeExt struct {
	state    []byte
	localErr error
	mergeErr error
	merged   [][]byte
}

func (f *fakeExt) LocalState() ([]byte, error) { return f.state, f.localErr }

func (f *fakeExt) MergeRemote(b []byte) error {
	f.merged = append(f.merged, b)
	return f.mergeErr
}

func TestSyncExtension_Helpers(t *testing.T) {
	// With no extension configured the helpers are nil/no-ops.
	m1, _ := membership.New("self", "self:1", "self:2")
	bare, err := New(m1, Options{Addr: "a:1", ServerTLS: dummyTLS(), ClientTLS: dummyTLS()}, discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if bare.extLocalState() != nil {
		t.Error("no extension should yield nil local state")
	}
	bare.extMergeRemote([]byte("ignored")) // must not panic

	// With an extension the helpers forward to it.
	ext := &fakeExt{state: []byte("hello")}
	m2, _ := membership.New("self", "self:1", "self:2")
	s, err := New(m2, Options{Addr: "a:1", ServerTLS: dummyTLS(), ClientTLS: dummyTLS(), Extension: ext}, discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if string(s.extLocalState()) != "hello" {
		t.Error("extLocalState should return the extension's bytes")
	}
	s.extMergeRemote([]byte("peer"))
	if len(ext.merged) != 1 || string(ext.merged[0]) != "peer" {
		t.Errorf("merge not forwarded to the extension: %v", ext.merged)
	}
	s.extMergeRemote(nil) // empty payload is skipped, not merged
	if len(ext.merged) != 1 {
		t.Error("empty bytes should be skipped")
	}

	// Extension errors are logged and swallowed so membership gossip keeps
	// flowing.
	ext.localErr = errors.New("boom")
	if s.extLocalState() != nil {
		t.Error("a local-state error should yield nil")
	}
	ext.mergeErr = errors.New("boom")
	s.extMergeRemote([]byte("z")) // must not panic
}
