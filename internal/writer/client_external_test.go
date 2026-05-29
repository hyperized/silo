package writer_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	"github.com/hyperized/silo/internal/writer"
)

var errBoom = errors.New("boom")

// --- fake gRPC clients -------------------------------------------------------

type putRec struct {
	id   string
	data []byte
}

// fakeChunks records every chunk Put. Embedding the generated client means
// only Put is implemented; any other method would panic if the writer called
// it, which is the assertion we want.
type fakeChunks struct {
	chunkv1.ChunkStoreClient
	mu        sync.Mutex
	puts      []*putRec
	openErr   error
	headerErr error
	dataErr   error
	closeErr  error
}

func (c *fakeChunks) Put(_ context.Context, _ ...grpc.CallOption) (grpc.ClientStreamingClient[chunkv1.PutRequest, chunkv1.PutResponse], error) {
	if c.openErr != nil {
		return nil, c.openErr
	}
	rec := &putRec{}
	c.mu.Lock()
	c.puts = append(c.puts, rec)
	c.mu.Unlock()
	return &fakePutStream{rec: rec, headerErr: c.headerErr, dataErr: c.dataErr, closeErr: c.closeErr}, nil
}

func (c *fakeChunks) records() []*putRec {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*putRec(nil), c.puts...)
}

type fakePutStream struct {
	grpc.ClientStream
	rec       *putRec
	headerErr error
	dataErr   error
	closeErr  error
}

func (s *fakePutStream) Send(r *chunkv1.PutRequest) error {
	if h := r.GetHeader(); h != nil {
		s.rec.id = h.GetChunkId()
		return s.headerErr
	}
	s.rec.data = append(s.rec.data, r.GetData()...)
	return s.dataErr
}

func (s *fakePutStream) CloseAndRecv() (*chunkv1.PutResponse, error) {
	return &chunkv1.PutResponse{}, s.closeErr
}

type appendRec struct{ path, id string }

type fakeNS struct {
	namespacev1.NamespaceStoreClient
	mu        sync.Mutex
	touchErr  error
	appendErr error
	appends   []appendRec
}

func (n *fakeNS) Touch(_ context.Context, _ *namespacev1.TouchRequest, _ ...grpc.CallOption) (*namespacev1.TouchResponse, error) {
	return &namespacev1.TouchResponse{}, n.touchErr
}

func (n *fakeNS) AppendChunk(_ context.Context, req *namespacev1.AppendChunkRequest, _ ...grpc.CallOption) (*namespacev1.AppendChunkResponse, error) {
	if n.appendErr != nil {
		return nil, n.appendErr
	}
	n.mu.Lock()
	n.appends = append(n.appends, appendRec{req.GetPath(), req.GetChunkId()})
	n.mu.Unlock()
	return &namespacev1.AppendChunkResponse{}, nil
}

func (n *fakeNS) recorded() []appendRec {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]appendRec(nil), n.appends...)
}

// --- tests -------------------------------------------------------------------

// Stream is the file-writing surface; it must be a drop-in io.WriteCloser.
var _ io.WriteCloser = (*writer.Stream)(nil)

func newWriter(t *testing.T, c *fakeChunks, n *fakeNS, opts ...writer.Option) *writer.Writer {
	t.Helper()
	w, err := writer.New(c, n, "silo-a", opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return w
}

func TestWriter_SealsChunksAndRecordsManifest(t *testing.T) {
	chunks, ns := &fakeChunks{}, &fakeNS{}
	w := newWriter(t, chunks, ns, writer.WithChunkSize(4))

	st, err := w.Create(context.Background(), "/big")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := st.Write([]byte("0123456789")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	want := []putRec{
		{id: writer.ChunkID(w.ID(), 0, 0), data: []byte("0123")},
		{id: writer.ChunkID(w.ID(), 0, 1), data: []byte("4567")},
		{id: writer.ChunkID(w.ID(), 0, 2), data: []byte("89")},
	}
	got := chunks.records()
	if len(got) != len(want) {
		t.Fatalf("sealed %d chunks, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].id != w.id || !bytes.Equal(got[i].data, w.data) {
			t.Errorf("chunk %d = (%s, %q), want (%s, %q)", i, got[i].id, got[i].data, w.id, w.data)
		}
	}

	// Every sealed chunk is recorded in the manifest, in the same order, and
	// against the same path.
	appends := ns.recorded()
	if len(appends) != len(want) {
		t.Fatalf("recorded %d manifest entries, want %d", len(appends), len(want))
	}
	for i, w := range want {
		if appends[i] != (appendRec{"/big", w.id}) {
			t.Errorf("manifest %d = %+v, want {/big %s}", i, appends[i], w.id)
		}
	}
}

func TestWriter_IOCopy(t *testing.T) {
	chunks, ns := &fakeChunks{}, &fakeNS{}
	w := newWriter(t, chunks, ns, writer.WithChunkSize(8))
	st, err := w.Create(context.Background(), "/f")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	payload := bytes.Repeat([]byte("ab"), 21) // 42 bytes -> 5 full + 1 partial
	if _, err := io.Copy(st, bytes.NewReader(payload)); err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var reassembled []byte
	for _, r := range chunks.records() {
		reassembled = append(reassembled, r.data...)
	}
	if !bytes.Equal(reassembled, payload) {
		t.Errorf("reassembled %d bytes, want %d", len(reassembled), len(payload))
	}
}

func TestWriter_EmptyFileSealsNoChunks(t *testing.T) {
	chunks, ns := &fakeChunks{}, &fakeNS{}
	w := newWriter(t, chunks, ns)
	st, err := w.Create(context.Background(), "/empty")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := len(chunks.records()); n != 0 {
		t.Errorf("sealed %d chunks for an empty file, want 0", n)
	}
	if n := len(ns.recorded()); n != 0 {
		t.Errorf("recorded %d manifest entries for an empty file, want 0", n)
	}
}

func TestWriter_CreateAppendsToExistingFile(t *testing.T) {
	chunks := &fakeChunks{}
	ns := &fakeNS{touchErr: status.Error(codes.AlreadyExists, "namespace: \"/f\" already exists")}
	w := newWriter(t, chunks, ns, writer.WithChunkSize(2))
	st, err := w.Create(context.Background(), "/f")
	if err != nil {
		t.Fatalf("Create on an existing file should append, got: %v", err)
	}
	if _, err := st.Write([]byte("ab")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := len(chunks.records()); n != 1 {
		t.Errorf("sealed %d chunks, want 1", n)
	}
}

func TestWriter_CreateSurfacesTouchError(t *testing.T) {
	ns := &fakeNS{touchErr: status.Error(codes.Internal, "disk on fire")}
	w := newWriter(t, &fakeChunks{}, ns)
	if _, err := w.Create(context.Background(), "/f"); err == nil {
		t.Fatal("Create should surface a non-AlreadyExists Touch error")
	}
}

func TestWriter_Options(t *testing.T) {
	t.Run("non-positive chunk size falls back to the default", func(t *testing.T) {
		chunks, ns := &fakeChunks{}, &fakeNS{}
		w := newWriter(t, chunks, ns, writer.WithChunkSize(0))
		st, err := w.Create(context.Background(), "/f")
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		// A few bytes against the multi-MiB default must not seal anything
		// until Close — a zero chunk size would have sealed per byte.
		if _, err := st.Write([]byte("abc")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if n := len(chunks.records()); n != 0 {
			t.Fatalf("default chunk size sealed %d chunks early", n)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if got := chunks.records(); len(got) != 1 || !bytes.Equal(got[0].data, []byte("abc")) {
			t.Errorf("on close: got %v, want one chunk of \"abc\"", got)
		}
	})

	t.Run("epoch feeds the derived id", func(t *testing.T) {
		chunks, ns := &fakeChunks{}, &fakeNS{}
		w := newWriter(t, chunks, ns, writer.WithChunkSize(2), writer.WithEpoch(7))
		st, err := w.Create(context.Background(), "/f")
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if _, err := st.Write([]byte("ab")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if got := chunks.records(); len(got) != 1 || got[0].id != writer.ChunkID(w.ID(), 7, 0) {
			t.Errorf("epoch not applied: got %v, want id %s", got, writer.ChunkID(w.ID(), 7, 0))
		}
	})
}

func TestWriter_PutFailures(t *testing.T) {
	cases := []struct {
		name   string
		chunks *fakeChunks
		want   string
	}{
		{"open fails", &fakeChunks{openErr: errBoom}, "could not open a chunk put stream"},
		{"header send fails", &fakeChunks{headerErr: errBoom}, "could not send the header"},
		{"data send fails", &fakeChunks{dataErr: errBoom}, "could not send data"},
		{"silod rejects on close", &fakeChunks{closeErr: errBoom}, "silod rejected chunk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWriter(t, tc.chunks, &fakeNS{}, writer.WithChunkSize(2))
			st, err := w.Create(context.Background(), "/f")
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			_, err = st.Write([]byte("ab")) // a full chunk triggers a flush
			if err == nil || !contains(err.Error(), tc.want) {
				t.Errorf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestWriter_ManifestAppendFailureIsActionable(t *testing.T) {
	w := newWriter(t, &fakeChunks{}, &fakeNS{appendErr: errBoom}, writer.WithChunkSize(2))
	st, err := w.Create(context.Background(), "/f")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, err = st.Write([]byte("ab"))
	if err == nil || !contains(err.Error(), "could not record it in the manifest") {
		t.Errorf("got %v, want a manifest-append error that names the orphaned chunk", err)
	}
}

func TestWriter_CloseFlushFailure(t *testing.T) {
	// A large chunk size means the small write only buffers; the failing
	// put happens when Close seals the remainder.
	w := newWriter(t, &fakeChunks{openErr: errBoom}, &fakeNS{}, writer.WithChunkSize(1024))
	st, err := w.Create(context.Background(), "/f")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := st.Write([]byte("ab")); err != nil {
		t.Fatalf("Write should only buffer here: %v", err)
	}
	if err := st.Close(); err == nil {
		t.Fatal("Close should fail when sealing the final chunk fails")
	}
}

func TestWriter_StickyError(t *testing.T) {
	w := newWriter(t, &fakeChunks{openErr: errBoom}, &fakeNS{}, writer.WithChunkSize(2))
	st, err := w.Create(context.Background(), "/f")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, first := st.Write([]byte("ab")) // fails on flush
	if first == nil {
		t.Fatal("first Write should fail")
	}
	if _, again := st.Write([]byte("cd")); !errors.Is(again, first) {
		t.Errorf("second Write returned %v, want the sticky error %v", again, first)
	}
	if onClose := st.Close(); !errors.Is(onClose, first) {
		t.Errorf("Close returned %v, want the sticky error %v", onClose, first)
	}
}

func TestWriter_ConcurrentStreamsDeriveUniqueIDs(t *testing.T) {
	chunks, ns := &fakeChunks{}, &fakeNS{}
	w := newWriter(t, chunks, ns, writer.WithChunkSize(2))

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			st, err := w.Create(context.Background(), "/shared")
			if err != nil {
				return
			}
			_, _ = st.Write([]byte("abcd")) // two chunks each
			_ = st.Close()
		}()
	}
	wg.Wait()

	recs := chunks.records()
	if len(recs) != goroutines*2 {
		t.Fatalf("sealed %d chunks, want %d", len(recs), goroutines*2)
	}
	seen := make(map[string]bool, len(recs))
	for _, r := range recs {
		if seen[r.id] {
			t.Fatalf("duplicate chunk id %q across concurrent streams", r.id)
		}
		seen[r.id] = true
	}
}

func contains(s, sub string) bool { return bytes.Contains([]byte(s), []byte(sub)) }
