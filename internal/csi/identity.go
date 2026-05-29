package csi

import (
	"context"

	"google.golang.org/protobuf/types/known/wrapperspb"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
)

// IdentityService answers the CSI Identity RPCs: the plugin's name and version,
// the optional services it offers, and a liveness probe. The kubelet and the
// external CSI sidecars call these first and often, so they must stay cheap and
// must not depend on silod being reachable — readiness is reported through
// Probe, not by failing the other calls.
type IdentityService struct {
	version string
	ready   func(context.Context) (bool, error)
}

// IdentityOption configures an IdentityService.
type IdentityOption func(*IdentityService)

// WithReadiness supplies a probe that reports whether the driver can currently
// serve requests (typically: can it reach silod). Without it the driver always
// reports ready, which is right for the Identity service itself since it has no
// backend dependency.
func WithReadiness(fn func(context.Context) (bool, error)) IdentityOption {
	return func(s *IdentityService) { s.ready = fn }
}

// NewIdentityService builds the Identity service. version is the silo build
// version reported to Kubernetes (it surfaces in `kubectl get csidrivers` and
// in sidecar logs).
func NewIdentityService(version string, opts ...IdentityOption) *IdentityService {
	s := &IdentityService{version: version}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// GetPluginInfo returns the driver name and version. The name is the external
// contract every StorageClass and CSIDriver object keys on.
func (s *IdentityService) GetPluginInfo(_ context.Context, _ *csiv1.GetPluginInfoRequest) (*csiv1.GetPluginInfoResponse, error) {
	return &csiv1.GetPluginInfoResponse{Name: DriverName, VendorVersion: s.version}, nil
}

// GetPluginCapabilities advertises that this plugin runs a Controller service
// (so the external-provisioner and friends engage). silo derives placement from
// the chunk-id hash rather than CSI topology, so no accessibility constraints
// are advertised.
func (s *IdentityService) GetPluginCapabilities(_ context.Context, _ *csiv1.GetPluginCapabilitiesRequest) (*csiv1.GetPluginCapabilitiesResponse, error) {
	return &csiv1.GetPluginCapabilitiesResponse{
		Capabilities: []*csiv1.PluginCapability{{
			Type: &csiv1.PluginCapability_Service_{
				Service: &csiv1.PluginCapability_Service{Type: csiv1.PluginCapability_Service_CONTROLLER_SERVICE},
			},
		}},
	}, nil
}

// Probe reports whether the driver is ready to serve. A not-ready probe tells
// the kubelet to back off and retry rather than treat the plugin as failed.
func (s *IdentityService) Probe(ctx context.Context, _ *csiv1.ProbeRequest) (*csiv1.ProbeResponse, error) {
	ready := true
	if s.ready != nil {
		ok, err := s.ready(ctx)
		if err != nil {
			return nil, err
		}
		ready = ok
	}
	return &csiv1.ProbeResponse{Ready: wrapperspb.Bool(ready)}, nil
}

// Compile-time check that IdentityService satisfies the generated server.
var _ csiv1.IdentityServer = (*IdentityService)(nil)
