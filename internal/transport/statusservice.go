package transport

import (
	"context"

	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
)

// StatusBuilder produces the status snapshot the service returns.
// *clusterstatus.Builder satisfies it; the JSON endpoint behind the web UI
// renders the same builder, so both surfaces report identical data.
type StatusBuilder interface {
	Build(ctx context.Context) *statusv1.GetStatusResponse
}

// StatusService answers the operator-facing ClusterStatus RPC with the
// responding node's own snapshot. There is no global aggregator, so this is one
// node's honest view, which is the decentralised model silo runs on.
type StatusService struct {
	builder StatusBuilder
}

// NewStatusService serves snapshots from builder.
func NewStatusService(builder StatusBuilder) *StatusService {
	return &StatusService{builder: builder}
}

// GetStatus returns the builder's current snapshot. It never fails: the builder
// degrades individual figures rather than the whole answer.
func (s *StatusService) GetStatus(ctx context.Context, _ *statusv1.GetStatusRequest) (*statusv1.GetStatusResponse, error) {
	return s.builder.Build(ctx), nil
}

// Compile-time check that StatusService satisfies the generated server.
var _ statusv1.ClusterStatusServer = (*StatusService)(nil)
