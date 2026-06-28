package volume_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/hyperized/silo/internal/volume"
)

var errBoom = errors.New("boom")

// --- in-memory fakes ---------------------------------------------------------

type fakeChunks struct {
	mu             sync.Mutex
	data           map[string][]byte
	getErr, putErr error
}

func (c *fakeChunks) GetChunk(_ context.Context, id string) ([]byte, error) {
	if c.getErr != nil {
		return nil, c.getErr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.data[id]
	if !ok {
		return nil, fmt.Errorf("no such chunk %q", id)
	}
	return append([]byte(nil), b...), nil
}

func (c *fakeChunks) PutChunk(_ context.Context, id string, data []byte) error {
	if c.putErr != nil {
		return c.putErr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		c.data = map[string][]byte{}
	}
	c.data[id] = append([]byte(nil), data...)
	return nil
}

func (c *fakeChunks) put(id string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		c.data = map[string][]byte{}
	}
	c.data[id] = data
}

type fakeMeta struct {
	mu        sync.Mutex
	size      int64
	sizeErr   error
	extents   map[uint64]string
	extentErr error
	writeErr  error
}

func (m *fakeMeta) ExtentSize(string) (int64, error) {
	if m.sizeErr != nil {
		return 0, m.sizeErr
	}
	return m.size, nil
}

func (m *fakeMeta) Extent(_ string, idx uint64) (string, bool, error) {
	if m.extentErr != nil {
		return "", false, m.extentErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.extents[idx]
	return id, ok, nil
}

func (m *fakeMeta) WriteExtent(_ string, idx uint64, id, _ string) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.extents == nil {
		m.extents = map[uint64]string{}
	}
	m.extents[idx] = id
	return nil
}

func (m *fakeMeta) WriteExtents(_ string, indexes []uint64, ids []string, _ string) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.extents == nil {
		m.extents = map[uint64]string{}
	}
	for i, idx := range indexes {
		m.extents[idx] = ids[i]
	}
	return nil
}

func (m *fakeMeta) set(idx uint64, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.extents == nil {
		m.extents = map[uint64]string{}
	}
	m.extents[idx] = id
}

var (
	_ = (*fakeChunks)(nil)
	_ = (*fakeMeta)(nil)
)

func openVol(t *testing.T, size int64) (*volume.Volume, *fakeMeta, *fakeChunks) {
	t.Helper()
	meta := &fakeMeta{size: size}
	chunks := &fakeChunks{}
	v, err := volume.Open(context.Background(), meta, chunks, "/vol", "holder")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return v, meta, chunks
}

// --- tests -------------------------------------------------------------------

func TestVolume_WriteReadRoundTrip(t *testing.T) {
	v, _, _ := openVol(t, 16)

	if n, err := v.WriteAt([]byte("hello"), 0); err != nil || n != 5 {
		t.Fatalf("WriteAt = (%d,%v), want (5,nil)", n, err)
	}
	p := make([]byte, 5)
	if n, err := v.ReadAt(p, 0); err != nil || n != 5 || string(p) != "hello" {
		t.Fatalf("ReadAt = (%d,%q,%v), want (5,hello,nil)", n, p, err)
	}

	// Bytes past the write, still inside the extent, read as zeros.
	tail := make([]byte, 4)
	if _, err := v.ReadAt(tail, 5); err != nil {
		t.Fatalf("ReadAt tail: %v", err)
	}
	if !bytes.Equal(tail, make([]byte, 4)) {
		t.Errorf("tail = %v, want zeros", tail)
	}
}

func TestVolume_WriteSpanningExtents(t *testing.T) {
	v, _, _ := openVol(t, 8) // small extents to force a multi-extent write

	payload := []byte("ABCDEFGHIJKLMNOPQRST") // 20 bytes -> extents 1,2,3 partially
	if n, err := v.WriteAt(payload, 6); err != nil || n != len(payload) {
		t.Fatalf("WriteAt = (%d,%v), want (%d,nil)", n, err, len(payload))
	}
	got := make([]byte, len(payload))
	if _, err := v.ReadAt(got, 6); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip across extents = %q, want %q", got, payload)
	}
	// The byte just before the write is still zero (untouched).
	pre := make([]byte, 1)
	if _, err := v.ReadAt(pre, 5); err != nil || pre[0] != 0 {
		t.Errorf("byte before write = %v (err %v), want 0", pre[0], err)
	}
}

func TestVolume_FullExtentOverwriteSkipsRead(t *testing.T) {
	v, meta, chunks := openVol(t, 4)
	// Seed extent 0 with a chunk, then overwrite the whole extent. A
	// full-extent write must not read the old chunk, so a get error would not
	// matter — prove that by failing reads and still succeeding.
	if _, err := v.WriteAt([]byte("AAAA"), 0); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	chunks.getErr = errBoom // any read would now fail
	if _, err := v.WriteAt([]byte("BBBB"), 0); err != nil {
		t.Fatalf("full-extent overwrite should not read: %v", err)
	}
	chunks.getErr = nil
	got := make([]byte, 4)
	if _, err := v.ReadAt(got, 0); err != nil || string(got) != "BBBB" {
		t.Errorf("after overwrite = (%q,%v), want BBBB", got, err)
	}
	_ = meta
}

func TestVolume_UnmappedReadsAsZeros(t *testing.T) {
	v, _, _ := openVol(t, 16)
	got := make([]byte, 32) // two fully-unmapped extents
	if _, err := v.ReadAt(got, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 32)) {
		t.Errorf("unmapped read = %v, want zeros", got)
	}
}

func TestVolume_ShortBackingChunkZeroPads(t *testing.T) {
	v, meta, chunks := openVol(t, 16)
	// A backing chunk shorter than the extent (e.g. a legacy/truncated write)
	// is zero-padded up to the extent size on read.
	meta.set(0, "short")
	chunks.put("short", []byte("hi"))
	got := make([]byte, 16)
	if _, err := v.ReadAt(got, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	want := append([]byte("hi"), make([]byte, 14)...)
	if !bytes.Equal(got, want) {
		t.Errorf("short-chunk read = %v, want %v", got, want)
	}
}

func TestVolume_NegativeOffsets(t *testing.T) {
	v, _, _ := openVol(t, 16)
	if _, err := v.ReadAt(make([]byte, 4), -1); err == nil {
		t.Error("ReadAt negative offset should error")
	}
	if _, err := v.WriteAt([]byte("x"), -1); err == nil {
		t.Error("WriteAt negative offset should error")
	}
}

func TestVolume_OpenExtentSizeError(t *testing.T) {
	meta := &fakeMeta{sizeErr: errBoom}
	if _, err := volume.Open(context.Background(), meta, &fakeChunks{}, "/v", "h"); !errors.Is(err, errBoom) {
		t.Errorf("Open err = %v, want errBoom", err)
	}
}

func TestVolume_OpenNonPositiveExtentSize(t *testing.T) {
	// A zero (or negative) extent size would make locate divide by zero and
	// panic on the first I/O; Open must refuse it up front.
	for _, size := range []int64{0, -4096} {
		meta := &fakeMeta{size: size}
		_, err := volume.Open(context.Background(), meta, &fakeChunks{}, "/v", "h")
		if err == nil || !strings.Contains(err.Error(), "extent size") {
			t.Errorf("Open with extent size %d: err = %v, want an 'extent size' error", size, err)
		}
	}
}

func TestVolume_ErrorPaths(t *testing.T) {
	t.Run("put fails", func(t *testing.T) {
		v, _, chunks := openVol(t, 8)
		chunks.putErr = errBoom
		if _, err := v.WriteAt([]byte("x"), 0); !errors.Is(err, errBoom) {
			t.Errorf("WriteAt put err = %v, want errBoom", err)
		}
	})
	t.Run("write-extent fails (fenced)", func(t *testing.T) {
		v, meta, _ := openVol(t, 8)
		meta.writeErr = errBoom
		if _, err := v.WriteAt([]byte("x"), 0); !errors.Is(err, errBoom) {
			t.Errorf("WriteAt rebind err = %v, want errBoom", err)
		}
	})
	t.Run("extent lookup fails", func(t *testing.T) {
		v, meta, _ := openVol(t, 8)
		meta.extentErr = errBoom
		if _, err := v.ReadAt(make([]byte, 8), 0); !errors.Is(err, errBoom) {
			t.Errorf("ReadAt extent-lookup err = %v, want errBoom", err)
		}
	})
	t.Run("read get fails", func(t *testing.T) {
		v, _, chunks := openVol(t, 8)
		if _, err := v.WriteAt([]byte("seeded!!"), 0); err != nil { // map extent 0
			t.Fatalf("seed: %v", err)
		}
		chunks.getErr = errBoom
		if _, err := v.ReadAt(make([]byte, 8), 0); !errors.Is(err, errBoom) {
			t.Errorf("ReadAt get err = %v, want errBoom", err)
		}
	})
	t.Run("partial write read fails", func(t *testing.T) {
		v, _, chunks := openVol(t, 8)
		if _, err := v.WriteAt([]byte("seeded!!"), 0); err != nil { // map extent 0
			t.Fatalf("seed: %v", err)
		}
		chunks.getErr = errBoom
		if _, err := v.WriteAt([]byte("xy"), 2); !errors.Is(err, errBoom) { // partial -> RMW read
			t.Errorf("partial WriteAt read err = %v, want errBoom", err)
		}
	})

	// The cases above stay within one extent (the single-extent fast path). The
	// ones below span two extents (16B / partial over 8B extents) to drive the
	// parallel multi-extent path, whose per-worker error handling and coalesced
	// rebind are separate branches.
	t.Run("multi-extent put fails", func(t *testing.T) {
		v, _, chunks := openVol(t, 8)
		chunks.putErr = errBoom
		// 16B over extents 0,1: both workers' PutChunk fails, so every result
		// carries an error and the result walk breaks at the first one.
		if _, err := v.WriteAt(make([]byte, 16), 0); !errors.Is(err, errBoom) {
			t.Errorf("multi-extent WriteAt put err = %v, want errBoom", err)
		}
	})
	t.Run("multi-extent RMW read fails", func(t *testing.T) {
		v, meta, _ := openVol(t, 8)
		meta.extentErr = errBoom
		// A partial write straddling two extents forces a read-modify-write in
		// each worker; the extent lookup error surfaces from inside the worker.
		if _, err := v.WriteAt(make([]byte, 8), 4); !errors.Is(err, errBoom) {
			t.Errorf("multi-extent RMW err = %v, want errBoom", err)
		}
	})
	t.Run("multi-extent rebind fails", func(t *testing.T) {
		v, meta, _ := openVol(t, 8)
		meta.writeErr = errBoom
		// All workers' puts succeed, so the run reaches the coalesced
		// WriteExtents rebind, which then fails.
		if _, err := v.WriteAt(make([]byte, 16), 0); !errors.Is(err, errBoom) {
			t.Errorf("multi-extent rebind err = %v, want errBoom", err)
		}
	})
}

func TestVolume_ConcurrentWritesDistinctExtents(t *testing.T) {
	v, _, _ := openVol(t, 8)
	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			_, _ = v.WriteAt([]byte{byte('A' + i)}, int64(i)*8) // one byte per distinct extent
		}()
	}
	wg.Wait()

	for i := range n {
		got := make([]byte, 1)
		if _, err := v.ReadAt(got, int64(i)*8); err != nil || got[0] != byte('A'+i) {
			t.Errorf("extent %d = (%q,%v), want %q", i, got[0], err, byte('A'+i))
		}
	}
}
