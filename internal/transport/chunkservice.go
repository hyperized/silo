// Package transport hosts silod's gRPC surface: server bootstrap and the
// service implementations that wrap the internal storage packages.
package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	"github.com/hyperized/silo/internal/chunkstore"
)

// dataFrameSize bounds each streamed Get frame. Picked to stay well under
// gRPC's default 4 MiB max-message-size so we can deliver a default-sized
// chunk (4 MiB) plus header in two or three frames without the operator
// having to tune anything.
const dataFrameSize = 64 * 1024

// Coordinator spreads a chunk across its replica set and reads it back
// from the nearest available replica. *replication.Coordinator implements
// it. ChunkService routes client-facing Put/Get through the coordinator,
// while peer-to-peer replica traffic (PutHeader.replica /
// GetRequest.local_only) bypasses it and hits the local store directly.
type Coordinator interface {
	Write(ctx context.Context, chunkID string, data []byte) (chunkstore.Info, error)
	Read(ctx context.Context, chunkID string) ([]byte, chunkstore.Info, error)
}

// ChunkService adapts a chunkstore.Store and the replication coordinator to
// the gRPC ChunkStore service. Errors from the store are mapped to gRPC
// status codes so clients get a machine-distinguishable result for
// not-found, invalid-id, and internal failures — but the human-readable
// message stays the actionable text from the store.
type ChunkService struct {
	chunkv1.UnimplementedChunkStoreServer

	store  chunkstore.Store
	coord  Coordinator
	logger *slog.Logger
}

// NewChunkService wires the local chunk store and the replication
// coordinator onto the gRPC ChunkStore service.
func NewChunkService(store chunkstore.Store, coord Coordinator, logger *slog.Logger) *ChunkService {
	return &ChunkService{store: store, coord: coord, logger: logger}
}

// Put receives a PutHeader followed by zero or more data frames and
// stores the assembled chunk. Protocol violations return InvalidArgument.
func (s *ChunkService) Put(stream chunkv1.ChunkStore_PutServer) error {
	var (
		chunkID   string
		buf       []byte
		gotHeader bool
		replica   bool
	)

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		switch body := msg.Body.(type) {
		case *chunkv1.PutRequest_Header:
			if gotHeader {
				return status.Error(codes.InvalidArgument, "Put: a header was sent twice on the same stream; send PutHeader exactly once, as the first message")
			}
			if body.Header == nil || body.Header.ChunkId == "" {
				return status.Error(codes.InvalidArgument, "Put: PutHeader.chunk_id is required; set it before sending any data frames")
			}
			chunkID = body.Header.ChunkId
			replica = body.Header.Replica
			gotHeader = true
		case *chunkv1.PutRequest_Data:
			if !gotHeader {
				return status.Error(codes.InvalidArgument, "Put: the first message must be a PutHeader, not a data frame")
			}
			buf = append(buf, body.Data...)
		default:
			return status.Error(codes.InvalidArgument, "Put: unrecognised PutRequest body; send a PutHeader followed by zero or more data frames")
		}
	}

	if !gotHeader {
		return status.Error(codes.InvalidArgument, "Put: the stream ended before any messages were received; send at least a PutHeader")
	}

	info, err := s.putChunk(stream.Context(), chunkID, buf, replica)
	if err != nil {
		return mapStoreError(err, chunkID)
	}

	return stream.SendAndClose(&chunkv1.PutResponse{Info: toProtoInfo(info)})
}

// putChunk stores a client write through the replication coordinator, or a
// peer replica write straight to the local store. replica is the loop
// guard: a coordinator's fan-out to this node must not kick off another
// round of replication.
func (s *ChunkService) putChunk(ctx context.Context, chunkID string, data []byte, replica bool) (chunkstore.Info, error) {
	if replica {
		return s.store.Put(ctx, chunkID, data)
	}
	return s.coord.Write(ctx, chunkID, data)
}

// Get streams an Info frame followed by the chunk payload in
// dataFrameSize-bounded frames so a large chunk stays under gRPC limits.
func (s *ChunkService) Get(req *chunkv1.GetRequest, stream chunkv1.ChunkStore_GetServer) error {
	data, info, err := s.getChunk(stream.Context(), req.GetChunkId(), req.GetLocalOnly())
	if err != nil {
		return mapStoreError(err, req.GetChunkId())
	}

	if err := stream.Send(&chunkv1.GetResponse{
		Body: &chunkv1.GetResponse_Info{Info: toProtoInfo(info)},
	}); err != nil {
		return err
	}

	for off := 0; off < len(data); off += dataFrameSize {
		end := off + dataFrameSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&chunkv1.GetResponse{
			Body: &chunkv1.GetResponse_Data{Data: data[off:end]},
		}); err != nil {
			return err
		}
	}
	return nil
}

// getChunk reads a client request through the replication coordinator
// (local replica first, then peers), or a peer's local_only request
// straight from the local store. local_only is the loop guard: a
// coordinator fetching a replica from this node must not fan back out.
func (s *ChunkService) getChunk(ctx context.Context, chunkID string, localOnly bool) ([]byte, chunkstore.Info, error) {
	if localOnly {
		return s.store.Get(ctx, chunkID)
	}
	return s.coord.Read(ctx, chunkID)
}

// Delete removes a chunk by id. A missing chunk surfaces as NotFound.
func (s *ChunkService) Delete(ctx context.Context, req *chunkv1.DeleteRequest) (*chunkv1.DeleteResponse, error) {
	if err := s.store.Delete(ctx, req.GetChunkId()); err != nil {
		return nil, mapStoreError(err, req.GetChunkId())
	}
	return &chunkv1.DeleteResponse{}, nil
}

// Stat returns chunk metadata without transferring the payload.
func (s *ChunkService) Stat(ctx context.Context, req *chunkv1.StatRequest) (*chunkv1.StatResponse, error) {
	info, err := s.store.Stat(ctx, req.GetChunkId())
	if err != nil {
		return nil, mapStoreError(err, req.GetChunkId())
	}
	return &chunkv1.StatResponse{Info: toProtoInfo(info)}, nil
}

func toProtoInfo(info chunkstore.Info) *chunkv1.ChunkInfo {
	return &chunkv1.ChunkInfo{
		ChunkId:     info.ID,
		PlainBytes:  info.PlainBytes,
		StoredBytes: info.StoredBytes,
		CreatedAt:   timestamppb.New(info.CreatedAt),
	}
}

// mapStoreError translates a chunkstore error into a gRPC status that
// keeps the store's actionable text intact. Clients can branch on the
// code; humans still get the fix-it sentence in the message.
func mapStoreError(err error, chunkID string) error {
	switch {
	case errors.Is(err, chunkstore.ErrNotFound):
		return status.Errorf(codes.NotFound, "chunk %q: %v", chunkID, err)
	case errors.Is(err, chunkstore.ErrInvalidID):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	default:
		return status.Errorf(codes.Internal, "chunk %q: %v", chunkID, err)
	}
}

// Compile-time check that ChunkService satisfies the generated server
// interface; buf regenerations that change the contract fail at build
// time rather than at first RPC.
var _ chunkv1.ChunkStoreServer = (*ChunkService)(nil)
