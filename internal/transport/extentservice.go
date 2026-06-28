package transport

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	extentv1 "github.com/hyperized/silo/api/proto/silo/extent/v1"
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

// extentGetBatch bounds how many extent entries ride in one streamed Get frame,
// keeping each message well under gRPC's default 4 MiB cap even for a volume
// with millions of extents.
const extentGetBatch = 1024

// ExtentStore is the local extent-map store the service applies to and serves
// from. internal/extentmap.Store satisfies it. The service is purely the
// replica-apply endpoint: it folds peer deltas into the local store and streams
// the local map back. Coordination (placement, quorum fan-out) lives in the
// replication coordinator on the writing node, never here.
type ExtentStore interface {
	Merge(volumeID string, entries []crdt.MapEntry[uint64, string])
	Ensure(volumeID string)
	Snapshot(volumeID string) []crdt.MapEntry[uint64, string]
	Has(volumeID string) bool
	Len(volumeID string) int
}

// ExtentService adapts an extent-map store to the gRPC ExtentMap service.
type ExtentService struct {
	extentv1.UnimplementedExtentMapServer

	store  ExtentStore
	logger *slog.Logger
}

// NewExtentService wires the local extent-map store onto the gRPC service.
func NewExtentService(store ExtentStore, logger *slog.Logger) *ExtentService {
	return &ExtentService{store: store, logger: logger}
}

// Apply folds a peer's extent deltas into the local map (LWW by the HLC each
// entry carries). With no entries and ensure set, it just creates an empty map
// so a freshly-created volume exists on this replica before any write. The
// volume id is required.
func (s *ExtentService) Apply(_ context.Context, req *extentv1.ApplyRequest) (*extentv1.ApplyResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "extent: a volume id is required")
	}
	if len(req.GetEntries()) > 0 {
		s.store.Merge(req.GetVolumeId(), entriesFromProto(req.GetEntries()))
	} else if req.GetEnsure() {
		s.store.Ensure(req.GetVolumeId())
	}
	return &extentv1.ApplyResponse{}, nil
}

// Get streams the volume's whole extent map back in batches. An unknown volume
// yields an empty stream — the caller distinguishes "empty map" from "no map"
// via Stat.
func (s *ExtentService) Get(req *extentv1.GetRequest, stream extentv1.ExtentMap_GetServer) error {
	if req.GetVolumeId() == "" {
		return status.Error(codes.InvalidArgument, "extent: a volume id is required")
	}
	entries := s.store.Snapshot(req.GetVolumeId())
	for off := 0; off < len(entries); off += extentGetBatch {
		end := off + extentGetBatch
		if end > len(entries) {
			end = len(entries)
		}
		if err := stream.Send(&extentv1.GetResponse{Entries: entriesToProto(entries[off:end])}); err != nil {
			return err
		}
	}
	return nil
}

// Stat reports whether this node holds the volume's map and its extent count —
// the probe a warming serving node and the extent scrubber use.
func (s *ExtentService) Stat(_ context.Context, req *extentv1.StatRequest) (*extentv1.StatResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "extent: a volume id is required")
	}
	return &extentv1.StatResponse{
		Has:   s.store.Has(req.GetVolumeId()),
		Count: int64(s.store.Len(req.GetVolumeId())),
	}, nil
}

// entriesFromProto converts wire entries into CRDT map entries.
func entriesFromProto(in []*extentv1.ExtentEntry) []crdt.MapEntry[uint64, string] {
	out := make([]crdt.MapEntry[uint64, string], 0, len(in))
	for _, e := range in {
		out = append(out, crdt.MapEntry[uint64, string]{Key: e.GetIndex(), Value: e.GetChunkId(), TS: hlcFromProto(e.GetTs())})
	}
	return out
}

// entriesToProto converts CRDT map entries into wire entries.
func entriesToProto(in []crdt.MapEntry[uint64, string]) []*extentv1.ExtentEntry {
	out := make([]*extentv1.ExtentEntry, 0, len(in))
	for _, e := range in {
		out = append(out, &extentv1.ExtentEntry{Index: e.Key, ChunkId: e.Value, Ts: hlcToProto(e.TS)})
	}
	return out
}

// hlcFromProto / hlcToProto bridge silo's HLC timestamp and its wire form.
func hlcFromProto(h *extentv1.Hlc) hlc.Timestamp {
	return hlc.Timestamp{Wall: h.GetWall(), Logical: h.GetLogical(), Node: h.GetNode()}
}

func hlcToProto(ts hlc.Timestamp) *extentv1.Hlc {
	return &extentv1.Hlc{Wall: ts.Wall, Logical: ts.Logical, Node: ts.Node}
}
