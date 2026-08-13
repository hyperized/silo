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
	"google.golang.org/grpc/credentials"

	extentv1 "github.com/hyperized/silo/api/proto/silo/extent/v1"
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

// extentPeerTimeout bounds one extent-map replica call. The coordinator
// detaches the request context for background fan-out, so without a per-call
// bound a hung peer would leak a goroutine.
const extentPeerTimeout = 30 * time.Second

// ExtentGRPCPeers is the production extent-map peer client: it applies deltas,
// fetches maps, and stats replicas on other nodes over the mTLS data plane,
// caching one gRPC connection per peer address. Safe for concurrent use.
type ExtentGRPCPeers struct {
	creds  credentials.TransportCredentials
	logger *slog.Logger

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewExtentGRPCPeers builds an extent-map peer client that dials with creds.
func NewExtentGRPCPeers(creds credentials.TransportCredentials, logger *slog.Logger) *ExtentGRPCPeers {
	return &ExtentGRPCPeers{creds: creds, logger: logger, conns: make(map[string]*grpc.ClientConn)}
}

// client returns a cached ExtentMap client for addr, dialing lazily on first
// use. gRPC connections are safe to share and self-heal, so one per peer is
// reused across calls.
func (p *ExtentGRPCPeers) client(addr string) (extentv1.ExtentMapClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[addr]; ok {
		return extentv1.NewExtentMapClient(conn), nil
	}
	conn, err := newClientConn(addr, grpc.WithTransportCredentials(p.creds))
	if err != nil {
		return nil, fmt.Errorf("replication: could not create an extent-map client for peer %s (%w); check the advertised address is a reachable host:port", addr, err)
	}
	p.conns[addr] = conn
	return extentv1.NewExtentMapClient(conn), nil
}

// Apply replicates a batch of extent bindings to addr (replica=true so the peer
// folds them into its local map without coordinating further). With no entries
// and ensure=true it asks the peer to create an empty map.
func (p *ExtentGRPCPeers) Apply(ctx context.Context, addr, volumeID string, entries []crdt.MapEntry[uint64, string], ensure bool) error {
	cli, err := p.client(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, extentPeerTimeout)
	defer cancel()
	if _, err := cli.Apply(ctx, &extentv1.ApplyRequest{
		VolumeId: volumeID,
		Entries:  extentEntriesToProto(entries),
		Replica:  true,
		Ensure:   ensure,
	}); err != nil {
		return fmt.Errorf("replication: peer %s did not accept the extent-map update for volume %q (%w)", addr, volumeID, err)
	}
	return nil
}

// Fetch streams the whole extent map for volumeID from addr and reassembles it.
func (p *ExtentGRPCPeers) Fetch(ctx context.Context, addr, volumeID string) ([]crdt.MapEntry[uint64, string], error) {
	cli, err := p.client(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, extentPeerTimeout)
	defer cancel()
	stream, err := cli.Get(ctx, &extentv1.GetRequest{VolumeId: volumeID})
	if err != nil {
		return nil, fmt.Errorf("replication: could not open an extent-map fetch from peer %s for volume %q (%w)", addr, volumeID, err)
	}
	var out []crdt.MapEntry[uint64, string]
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("replication: error fetching the extent map of volume %q from peer %s (%w)", volumeID, addr, err)
		}
		out = append(out, extentEntriesFromProto(msg.GetEntries())...)
	}
	return out, nil
}

// Stat asks addr whether it holds volumeID's map, how many extents it has, and
// a digest of its bindings — the warming and repair probe. The digest is empty
// when the peer does not hold the map or predates the field, so callers must
// read empty as unknown rather than as agreement.
func (p *ExtentGRPCPeers) Stat(ctx context.Context, addr, volumeID string) (bool, int64, []byte, error) {
	cli, err := p.client(addr)
	if err != nil {
		return false, 0, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, extentPeerTimeout)
	defer cancel()
	resp, err := cli.Stat(ctx, &extentv1.StatRequest{VolumeId: volumeID})
	if err != nil {
		return false, 0, nil, fmt.Errorf("replication: could not stat the extent map of volume %q on peer %s (%w)", volumeID, addr, err)
	}
	return resp.GetHas(), resp.GetCount(), resp.GetDigest(), nil
}

// Delete asks addr to remove volumeID's map from its local store — the per-peer
// half of the coordinator's delete fan-out. The peer treats an unknown map as
// success, so this is idempotent.
func (p *ExtentGRPCPeers) Delete(ctx context.Context, addr, volumeID string) error {
	cli, err := p.client(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, extentPeerTimeout)
	defer cancel()
	if _, err := cli.Delete(ctx, &extentv1.DeleteRequest{VolumeId: volumeID}); err != nil {
		return fmt.Errorf("replication: peer %s did not delete the extent map of volume %q (%w)", addr, volumeID, err)
	}
	return nil
}

// Close shuts every cached peer connection. Called on silod shutdown.
func (p *ExtentGRPCPeers) Close() error {
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

// extentEntriesToProto / extentEntriesFromProto bridge CRDT map entries and
// their wire form.
func extentEntriesToProto(in []crdt.MapEntry[uint64, string]) []*extentv1.ExtentEntry {
	out := make([]*extentv1.ExtentEntry, 0, len(in))
	for _, e := range in {
		out = append(out, &extentv1.ExtentEntry{Index: e.Key, ChunkId: e.Value, Ts: &extentv1.Hlc{Wall: e.TS.Wall, Logical: e.TS.Logical, Node: e.TS.Node}})
	}
	return out
}

func extentEntriesFromProto(in []*extentv1.ExtentEntry) []crdt.MapEntry[uint64, string] {
	out := make([]crdt.MapEntry[uint64, string], 0, len(in))
	for _, e := range in {
		out = append(out, crdt.MapEntry[uint64, string]{
			Key:   e.GetIndex(),
			Value: e.GetChunkId(),
			TS:    hlc.Timestamp{Wall: e.GetTs().GetWall(), Logical: e.GetTs().GetLogical(), Node: e.GetTs().GetNode()},
		})
	}
	return out
}
