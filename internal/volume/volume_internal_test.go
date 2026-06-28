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
