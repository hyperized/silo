package volume

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type errMeta struct{}

func (errMeta) ExtentSize(string) (int64, error)                      { return 4096, nil }
func (errMeta) Extent(string, uint64) (string, bool, error)           { return "", false, nil }
func (errMeta) WriteExtent(string, uint64, string, string) error      { return nil }
func (errMeta) WriteExtents(string, []uint64, []string, string) error { return nil }

func TestOpen_EntropyFailure(t *testing.T) {
	prev := newWriterID
	t.Cleanup(func() { newWriterID = prev })
	newWriterID = func(string) (string, error) { return "", errors.New("no entropy") }

	if _, err := Open(context.Background(), errMeta{}, nil, "/v", "h"); err == nil || !strings.Contains(err.Error(), "entropy") {
		t.Fatalf("Open err = %v, want an entropy error", err)
	}
}

// capChunks records the peak number of PutChunk calls in flight at once so a
// test can assert silod's global write semaphore bounds extent-buffer
// concurrency. onPut, if set, is invoked for the duration of each Put.
type capChunks struct{ onPut func() }

func (capChunks) GetChunk(context.Context, string) ([]byte, error) { return nil, nil }

func (c capChunks) PutChunk(context.Context, string, []byte) error {
	if c.onPut != nil {
		c.onPut()
	}
	return nil
}

// TestWriteAt_GlobalConcurrencyCapAcrossVolumes proves the in-flight extent cap
// is process-wide, not per-WriteAt: many volumes writing many extents at once
// must never push more than writeAtParallelism PutChunks into flight together.
// That is the property that keeps silod's memory bounded by the cap rather than
// by the number of mounted volumes (the per-call semaphore this replaced let
// every volume run its own full batch, so memory scaled with volume count).
func TestWriteAt_GlobalConcurrencyCapAcrossVolumes(t *testing.T) {
	// Pin the cap to a small, known value so the assertion is deterministic
	// regardless of the test host's core count (writeSem is normally sized from
	// GOMAXPROCS). Same package-var test seam the file uses for newWriterID.
	const capLimit = 4
	prev := writeSem
	t.Cleanup(func() { writeSem = prev })
	writeSem = make(chan struct{}, capLimit)

	var inFlight, peak atomic.Int64
	chunks := capChunks{onPut: func() {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for { // CAS the running peak upward
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond) // hold the slot so concurrency can build
	}}

	// errMeta serves a 4096-byte extent and reports every extent unmapped, so an
	// aligned multi-extent write takes the parallel path with no read-modify-write.
	// volumes*extentsPerWrite (64) eager puts compete for cap (4) slots — without a
	// process-wide semaphore each volume would run its own batch and the peak would
	// blow well past cap.
	const extentSize, volumes, extentsPerWrite = 4096, 8, 8
	var wg sync.WaitGroup
	for vi := range volumes {
		wg.Add(1)
		go func(vi int) {
			defer wg.Done()
			v, err := Open(context.Background(), errMeta{}, chunks, "/v", "h")
			if err != nil {
				t.Errorf("Open: %v", err)
				return
			}
			if _, err := v.WriteAt(make([]byte, extentSize*extentsPerWrite), 0); err != nil {
				t.Errorf("vol %d WriteAt: %v", vi, err)
			}
		}(vi)
	}
	wg.Wait()

	if got := peak.Load(); got > capLimit {
		t.Fatalf("peak concurrent PutChunk = %d, want <= %d: the in-flight cap is not global across volumes", got, capLimit)
	}
	if peak.Load() < 2 {
		t.Fatalf("peak concurrent PutChunk = %d; test never built real concurrency, so it does not guard the cap", peak.Load())
	}
}

// mappedMeta reports every extent as mapped to the chunk in id and records the
// WriteExtents batches it receives.
type mappedMeta struct {
	id       string
	writeErr error
	batches  [][]uint64
	ids      [][]string
}

func (*mappedMeta) ExtentSize(string) (int64, error)                 { return 8, nil }
func (m *mappedMeta) Extent(string, uint64) (string, bool, error)    { return m.id, true, nil }
func (*mappedMeta) WriteExtent(string, uint64, string, string) error { return nil }
func (m *mappedMeta) WriteExtents(_ string, indexes []uint64, ids []string, _ string) error {
	m.batches = append(m.batches, append([]uint64(nil), indexes...))
	m.ids = append(m.ids, append([]string(nil), ids...))
	return m.writeErr
}

// A large trim is split so no single delta outgrows a replication message.
func TestTrim_Batches(t *testing.T) {
	prev := maxTrimBatch
	t.Cleanup(func() { maxTrimBatch = prev })
	maxTrimBatch = 2

	meta := &mappedMeta{id: "c"}
	v, err := Open(context.Background(), meta, capChunks{}, "/v", "h")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := v.Trim(0, 5*8); err != nil {
		t.Fatalf("Trim: %v", err)
	}
	want := [][]uint64{{0, 1}, {2, 3}, {4}}
	if len(meta.batches) != len(want) {
		t.Fatalf("batches = %v, want %v", meta.batches, want)
	}
	for i := range want {
		if len(meta.batches[i]) != len(want[i]) || meta.batches[i][0] != want[i][0] {
			t.Errorf("batch %d = %v, want %v", i, meta.batches[i], want[i])
		}
		for _, id := range meta.ids[i] {
			if id != "" {
				t.Errorf("batch %d unmapped with chunk id %q, want the empty id", i, id)
			}
		}
	}
}

// A failed batch stops the trim; the batches before it stay unmapped, which a
// trim is allowed to leave behind.
func TestTrim_BatchFailureStops(t *testing.T) {
	prev := maxTrimBatch
	t.Cleanup(func() { maxTrimBatch = prev })
	maxTrimBatch = 2

	boom := errors.New("fenced")
	meta := &mappedMeta{id: "c", writeErr: boom}
	v, err := Open(context.Background(), meta, capChunks{}, "/v", "h")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := v.Trim(0, 5*8); !errors.Is(err, boom) {
		t.Fatalf("Trim err = %v, want the batch error", err)
	}
	if len(meta.batches) != 1 {
		t.Errorf("trim kept going after a failed batch: %v", meta.batches)
	}
}

// An implementation that reports a trimmed extent as mapped to "" must still
// read as zeros, never as a fetch of chunk "".
func TestExtentBytes_EmptyIDReadsZeros(t *testing.T) {
	v, err := Open(context.Background(), &mappedMeta{id: ""}, failChunks{}, "/v", "h")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got := make([]byte, 8)
	if _, err := v.ReadAt(got, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(got) != string(make([]byte, 8)) {
		t.Errorf("read %q, want zeros", got)
	}
}

// failChunks fails every call, proving a code path never reaches the store.
type failChunks struct{}

func (failChunks) GetChunk(_ context.Context, id string) ([]byte, error) {
	return nil, errors.New("unexpected GetChunk of " + id)
}

func (failChunks) PutChunk(context.Context, string, []byte) error {
	return errors.New("unexpected PutChunk")
}
