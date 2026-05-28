package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	"github.com/hyperized/silo/internal/chunkstore"
)

const (
	// replicaFrameSize bounds each Put data frame sent to a peer, mirroring
	// the server's Get frame size and staying well under gRPC's default
	// 4 MiB message cap.
	replicaFrameSize = 64 * 1024
	// peerCallTimeout bounds one replica store/fetch. The coordinator
	// detaches the request context for background fan-out, so without a
	// per-call bound a hung peer would leak a goroutine.
	peerCallTimeout = 30 * time.Second
)

// newClientConn is the dial seam. Production uses grpc.NewClient; tests
// override it to force a dial failure without a real bad target.
var newClientConn = grpc.NewClient

// GRPCPeers is the production Peers: it stores and fetches replicas on
// other nodes over the mTLS data plane, caching one gRPC client connection
// per peer address. Safe for concurrent use.
type GRPCPeers struct {
	creds  credentials.TransportCredentials
	logger *slog.Logger

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewGRPCPeers builds a peer client that dials with creds. Production
// passes credentials.NewTLS(peerTLS); tests pass insecure credentials
// against an in-process server.
func NewGRPCPeers(creds credentials.TransportCredentials, logger *slog.Logger) *GRPCPeers {
	return &GRPCPeers{creds: creds, logger: logger, conns: make(map[string]*grpc.ClientConn)}
}

// client returns a cached ChunkStore client for addr, dialing lazily on
// first use. gRPC connections are safe to share and self-heal, so one per
// peer is reused across calls.
func (p *GRPCPeers) client(addr string) (chunkv1.ChunkStoreClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[addr]; ok {
		return chunkv1.NewChunkStoreClient(conn), nil
	}
	conn, err := newClientConn(addr, grpc.WithTransportCredentials(p.creds))
	if err != nil {
		return nil, fmt.Errorf("replication: could not create a data-plane client for peer %s (%w); check the advertised address is a reachable host:port", addr, err)
	}
	p.conns[addr] = conn
	return chunkv1.NewChunkStoreClient(conn), nil
}

// Store streams the chunk to addr as a replica write (replica=true) so the
// peer persists it locally without coordinating further replication.
func (p *GRPCPeers) Store(ctx context.Context, addr, id string, data []byte) (chunkstore.Info, error) {
	cli, err := p.client(addr)
	if err != nil {
		return chunkstore.Info{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, peerCallTimeout)
	defer cancel()

	stream, err := cli.Put(ctx)
	if err != nil {
		return chunkstore.Info{}, fmt.Errorf("replication: could not open a replica stream to peer %s for chunk %q (%w)", addr, id, err)
	}
	if err := stream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Header{Header: &chunkv1.PutHeader{ChunkId: id, Replica: true}}}); err != nil {
		return chunkstore.Info{}, fmt.Errorf("replication: could not send the replica header for chunk %q to peer %s (%w)", id, addr, err)
	}
	for off := 0; off < len(data); off += replicaFrameSize {
		end := off + replicaFrameSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Data{Data: data[off:end]}}); err != nil {
			return chunkstore.Info{}, fmt.Errorf("replication: could not send replica data for chunk %q to peer %s (%w)", id, addr, err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return chunkstore.Info{}, fmt.Errorf("replication: peer %s did not accept the replica of chunk %q (%w)", addr, id, err)
	}
	return infoFromProto(resp.GetInfo()), nil
}

// Fetch retrieves a chunk from addr as a local_only read so the peer
// serves it from its own disk without falling back across the ring.
func (p *GRPCPeers) Fetch(ctx context.Context, addr, id string) ([]byte, chunkstore.Info, error) {
	cli, err := p.client(addr)
	if err != nil {
		return nil, chunkstore.Info{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, peerCallTimeout)
	defer cancel()

	stream, err := cli.Get(ctx, &chunkv1.GetRequest{ChunkId: id, LocalOnly: true})
	if err != nil {
		return nil, chunkstore.Info{}, fmt.Errorf("replication: could not open a replica fetch from peer %s for chunk %q (%w)", addr, id, err)
	}
	var (
		data []byte
		info chunkstore.Info
	)
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, chunkstore.Info{}, fmt.Errorf("replication: error fetching replica of chunk %q from peer %s (%w)", id, addr, err)
		}
		switch body := msg.Body.(type) {
		case *chunkv1.GetResponse_Info:
			info = infoFromProto(body.Info)
		case *chunkv1.GetResponse_Data:
			data = append(data, body.Data...)
		}
	}
	return data, info, nil
}

// Stat asks addr whether it already holds id, reading only that peer's
// local store (the scrubber uses it to decide whether a replica is
// missing before sending the chunk). A NotFound from the peer surfaces as
// an error, which the scrubber treats as "replica absent".
func (p *GRPCPeers) Stat(ctx context.Context, addr, id string) (chunkstore.Info, error) {
	cli, err := p.client(addr)
	if err != nil {
		return chunkstore.Info{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, peerCallTimeout)
	defer cancel()

	resp, err := cli.Stat(ctx, &chunkv1.StatRequest{ChunkId: id, LocalOnly: true})
	if err != nil {
		return chunkstore.Info{}, fmt.Errorf("replication: could not stat chunk %q on peer %s (%w)", id, addr, err)
	}
	return infoFromProto(resp.GetInfo()), nil
}

// Delete removes a chunk from addr's local store (local_only so the peer
// does not coordinate its own fan-out). A NotFound is mapped to
// chunkstore.ErrNotFound so the coordinator can treat an already-absent
// replica as a successful delete.
func (p *GRPCPeers) Delete(ctx context.Context, addr, id string) error {
	cli, err := p.client(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, peerCallTimeout)
	defer cancel()

	if _, err := cli.Delete(ctx, &chunkv1.DeleteRequest{ChunkId: id, LocalOnly: true}); err != nil {
		if status.Code(err) == codes.NotFound {
			return chunkstore.ErrNotFound
		}
		return fmt.Errorf("replication: could not delete replica of chunk %q on peer %s (%w)", id, addr, err)
	}
	return nil
}

// Close shuts every cached peer connection. Called on silod shutdown so
// the data-plane clients do not outlive the daemon.
func (p *GRPCPeers) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []error
	for addr, conn := range p.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("peer %s: %w", addr, err))
		}
		delete(p.conns, addr)
	}
	return errors.Join(errs...)
}

// infoFromProto converts the wire ChunkInfo into the internal type. All
// accessors are nil-safe, and the timestamp is only read when present.
func infoFromProto(pi *chunkv1.ChunkInfo) chunkstore.Info {
	info := chunkstore.Info{
		ID:          pi.GetChunkId(),
		PlainBytes:  pi.GetPlainBytes(),
		StoredBytes: pi.GetStoredBytes(),
	}
	if ts := pi.GetCreatedAt(); ts != nil {
		info.CreatedAt = ts.AsTime()
	}
	return info
}
