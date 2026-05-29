package silod

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/hyperized/silo/internal/namespace"
	"github.com/hyperized/silo/internal/nbd"
	"github.com/hyperized/silo/internal/transport"
	"github.com/hyperized/silo/internal/volume"
)

// nsVolumes is the namespace surface the NBD backend drives: the block-I/O
// metadata the volume SDK needs, the device size, and lease management.
// *namespace.Namespace satisfies it.
type nsVolumes interface {
	ExtentSize(path string) (int64, error)
	Extent(path string, index uint64) (string, bool, error)
	WriteExtent(path string, index uint64, chunkID, holder string) error
	Size(path string) (int64, error)
	AcquireLease(path, holder string) (namespace.Lease, error)
	ReleaseLease(path, holder string) error
}

// coordChunks adapts the replication coordinator to the volume SDK's Chunks
// interface: plaintext get/put, with placement and encryption underneath.
type coordChunks struct{ coord transport.Coordinator }

func (c coordChunks) GetChunk(ctx context.Context, id string) ([]byte, error) {
	data, _, err := c.coord.Read(ctx, id)
	return data, err
}

func (c coordChunks) PutChunk(ctx context.Context, id string, data []byte) error {
	_, err := c.coord.Write(ctx, id, data)
	return err
}

// volumeDevice presents a volume's block-I/O SDK as a sized NBD device.
type volumeDevice struct {
	*volume.Volume
	size int64
}

func (d volumeDevice) Size() int64 { return d.size }

// volumeBackend opens a volume for an NBD export: it acquires the volume's
// lease for this node (fencing any previous holder), opens the block-I/O SDK,
// and releases the lease when the client disconnects.
type volumeBackend struct {
	ns     nsVolumes
	chunks volume.Chunks
	holder string
	logger *slog.Logger
}

func newVolumeBackend(ns nsVolumes, coord transport.Coordinator, holder string, logger *slog.Logger) *volumeBackend {
	return &volumeBackend{ns: ns, chunks: coordChunks{coord: coord}, holder: holder, logger: logger}
}

func (b *volumeBackend) Open(ctx context.Context, export string) (nbd.Device, func(), error) {
	size, err := b.ns.Size(export)
	if err != nil {
		return nil, nil, fmt.Errorf("nbd: cannot serve %q (%w)", export, err)
	}
	if size <= 0 {
		return nil, nil, fmt.Errorf("nbd: volume %q has no size; create it with a size before mounting it", export)
	}
	if _, err := b.ns.AcquireLease(export, b.holder); err != nil {
		return nil, nil, fmt.Errorf("nbd: could not acquire the lease on %q (%w)", export, err)
	}
	vol, err := volume.Open(ctx, b.ns, b.chunks, export, b.holder)
	if err != nil {
		_ = b.ns.ReleaseLease(export, b.holder)
		return nil, nil, err
	}
	release := func() {
		if err := b.ns.ReleaseLease(export, b.holder); err != nil {
			b.logger.Warn("nbd: releasing the volume lease failed", "export", export, "error", err)
		}
	}
	return volumeDevice{Volume: vol, size: size}, release, nil
}

// nbdSub runs the NBD listener as a silod subsystem. Its cancel context is
// created up front so Shutdown works even if it races ahead of Start binding
// the listener — a shutdown before Start makes Start a no-op rather than
// leaving Serve blocked on a listener nobody closes.
type nbdSub struct {
	addr   string
	srv    *nbd.Server
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	ln     net.Listener
	closed bool
}

func newNBDSub(addr string, srv *nbd.Server, logger *slog.Logger) *nbdSub {
	ctx, cancel := context.WithCancel(context.Background())
	return &nbdSub{addr: addr, srv: srv, logger: logger, ctx: ctx, cancel: cancel}
}

func (s *nbdSub) Name() string { return "nbd" }

func (s *nbdSub) Start() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil // shut down before we got a chance to serve
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("could not bind NBD listener at %q (%w); set SILO_NBD_ADDR to a free, reachable host:port, e.g. 0.0.0.0:10809", s.addr, err)
	}
	s.ln = ln
	s.mu.Unlock()
	s.logger.Info("nbd listener started", "addr", ln.Addr().String())
	return s.srv.Serve(s.ctx, ln)
}

func (s *nbdSub) Shutdown(context.Context) error {
	s.mu.Lock()
	s.closed = true
	ln := s.ln
	s.mu.Unlock()
	s.cancel()
	if ln != nil {
		return ln.Close()
	}
	return nil
}

// boundAddr returns the listener address once Start has bound it ("" before).
func (s *nbdSub) boundAddr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}
