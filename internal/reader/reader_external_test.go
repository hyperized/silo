package reader_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	"github.com/hyperized/silo/internal/reader"
)

var errBoom = errors.New("boom")

// --- fake gRPC clients -------------------------------------------------------

type fakeGetStream struct {
	grpc.ServerStreamingClient[chunkv1.GetResponse]
	frames  [][]byte
	i       int
	recvErr error // returned after the frames run out, instead of io.EOF, when set
}

func (s *fakeGetStream) Recv() (*chunkv1.GetResponse, error) {
	if s.i < len(s.frames) {
		f := s.frames[s.i]
		s.i++
		return &chunkv1.GetResponse{Body: &chunkv1.GetResponse_Data{Data: f}}, nil
	}
	if s.recvErr != nil {
		return nil, s.recvErr
	}
	return nil, io.EOF
}

type fakeChunks struct {
	chunkv1.ChunkStoreClient
	data    map[string][][]byte // chunk id -> frames
	openErr error
	recvErr error
}

func (c *fakeChunks) Get(ctx context.Context, req *chunkv1.GetRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[chunkv1.GetResponse], error) {
	if c.openErr != nil {
		return nil, c.openErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err // honour cancellation, as a real stream would
	}
	return &fakeGetStream{frames: c.data[req.GetChunkId()], recvErr: c.recvErr}, nil
}

type fakeNS struct {
	namespacev1.NamespaceStoreClient
	ids []string
	err error
}

func (n *fakeNS) Manifest(_ context.Context, _ *namespacev1.ManifestRequest, _ ...grpc.CallOption) (*namespacev1.ManifestResponse, error) {
	if n.err != nil {
		return nil, n.err
	}
	return &namespacev1.ManifestResponse{ChunkIds: n.ids}, nil
}

// --- tests -------------------------------------------------------------------

var _ io.ReadCloser = (*reader.Stream)(nil)

func TestReader_ReassemblesChunksInOrder(t *testing.T) {
	chunks := &fakeChunks{data: map[string][][]byte{
		"c0": {[]byte("he"), nil, []byte("llo")}, // an empty frame (e.g. the server's leading Info) is skipped
		"c1": {[]byte(" wor")},
		"c2": {[]byte("ld")},
	}}
	ns := &fakeNS{ids: []string{"c0", "c1", "c2"}}

	st, err := reader.New(chunks, ns).Open(context.Background(), "/f")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello world" {
		t.Errorf("reassembled %q, want %q", got, "hello world")
	}
}

func TestReader_TinyBufferSpansFramesAndChunks(t *testing.T) {
	chunks := &fakeChunks{data: map[string][][]byte{
		"c0": {[]byte("ab"), []byte("c")},
		"c1": {[]byte("de")},
	}}
	ns := &fakeNS{ids: []string{"c0", "c1"}}
	st, err := reader.New(chunks, ns).Open(context.Background(), "/f")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	// One byte at a time forces the partial-buffer copy path and the
	// frame/chunk advance to interleave.
	var got []byte
	p := make([]byte, 1)
	for {
		n, err := st.Read(p)
		got = append(got, p[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	if string(got) != "abcde" {
		t.Errorf("got %q, want %q", got, "abcde")
	}
}

func TestReader_EmptyManifestIsEmptyFile(t *testing.T) {
	st, err := reader.New(&fakeChunks{}, &fakeNS{}).Open(context.Background(), "/empty")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty file read %d bytes, want 0", len(got))
	}
}

func TestReader_OpenSurfacesManifestError(t *testing.T) {
	_, err := reader.New(&fakeChunks{}, &fakeNS{err: errBoom}).Open(context.Background(), "/missing")
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("could not read the manifest")) {
		t.Errorf("got %v, want a manifest error", err)
	}
}

func TestReader_GetFailures(t *testing.T) {
	cases := []struct {
		name   string
		chunks *fakeChunks
		want   string
	}{
		{"fetch open fails", &fakeChunks{openErr: errBoom}, "could not fetch chunk"},
		{"recv fails mid-chunk", &fakeChunks{recvErr: errBoom, data: map[string][][]byte{"c0": {[]byte("hi")}}}, "could not read chunk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := reader.New(tc.chunks, &fakeNS{ids: []string{"c0"}}).Open(context.Background(), "/f")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer st.Close()
			if _, err := io.ReadAll(st); err == nil || !bytes.Contains([]byte(err.Error()), []byte(tc.want)) {
				t.Errorf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestReader_CloseAbortsInFlightFetch(t *testing.T) {
	chunks := &fakeChunks{data: map[string][][]byte{"c0": {[]byte("hi")}}}
	st, err := reader.New(chunks, &fakeNS{ids: []string{"c0"}}).Open(context.Background(), "/f")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Close is idempotent and cancels the read context, so a subsequent fetch
	// fails rather than hanging.
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := io.ReadAll(st); !errors.Is(err, context.Canceled) {
		t.Errorf("read after Close = %v, want context.Canceled", err)
	}
}
