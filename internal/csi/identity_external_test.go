package csi_test

import (
	"context"
	"errors"
	"testing"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
	"github.com/hyperized/silo/internal/csi"
)

func TestIdentityService_PluginInfoAndCapabilities(t *testing.T) {
	svc := csi.NewIdentityService("v1.2.3")
	ctx := context.Background()

	info, err := svc.GetPluginInfo(ctx, &csiv1.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo: %v", err)
	}
	if info.GetName() != csi.DriverName || info.GetVendorVersion() != "v1.2.3" {
		t.Errorf("plugin info = (%q, %q), want (%q, v1.2.3)", info.GetName(), info.GetVendorVersion(), csi.DriverName)
	}

	caps, err := svc.GetPluginCapabilities(ctx, &csiv1.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetPluginCapabilities: %v", err)
	}
	if len(caps.GetCapabilities()) != 1 || caps.GetCapabilities()[0].GetService().GetType() != csiv1.PluginCapability_Service_CONTROLLER_SERVICE {
		t.Errorf("capabilities = %v, want one CONTROLLER_SERVICE", caps.GetCapabilities())
	}
}

func TestIdentityService_Probe(t *testing.T) {
	ctx := context.Background()

	// Default: always ready.
	resp, err := csi.NewIdentityService("v1").Probe(ctx, &csiv1.ProbeRequest{})
	if err != nil || !resp.GetReady().GetValue() {
		t.Fatalf("default Probe = (%v, %v), want ready", resp, err)
	}

	// A readiness probe that reports not-ready surfaces through Ready=false.
	notReady := csi.NewIdentityService("v1", csi.WithReadiness(func(context.Context) (bool, error) { return false, nil }))
	if resp, err := notReady.Probe(ctx, &csiv1.ProbeRequest{}); err != nil || resp.GetReady().GetValue() {
		t.Errorf("not-ready Probe = (%v, %v), want ready=false", resp, err)
	}

	// A readiness probe error propagates.
	boom := csi.NewIdentityService("v1", csi.WithReadiness(func(context.Context) (bool, error) { return false, errors.New("silod unreachable") }))
	if _, err := boom.Probe(ctx, &csiv1.ProbeRequest{}); err == nil {
		t.Error("Probe should propagate a readiness error")
	}
}
