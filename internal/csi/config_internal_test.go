package csi

import (
	"testing"
	"time"
)

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Endpoint != DefaultEndpoint || cfg.Mode != DefaultMode || cfg.SilodAddr != DefaultSilodAddr || cfg.NBDAddr != DefaultNBDAddr {
		t.Errorf("defaults = %+v", cfg)
	}
	if cfg.NBDReconnectTimeout != DefaultNBDReconnectTimeout || cfg.StateDir != DefaultStateDir {
		t.Errorf("reconnect/state defaults = %+v", cfg)
	}
	if cfg.NBDRequestTimeout != DefaultNBDRequestTimeout || cfg.HTTPAddr != "" {
		t.Errorf("request-timeout/http defaults = %+v", cfg)
	}
	if !cfg.Mode.RunsController() || !cfg.Mode.RunsNode() {
		t.Error("default mode should run both controller and node")
	}
}

func TestLoadConfig_Overrides(t *testing.T) {
	env := map[string]string{
		"SILO_CSI_ENDPOINT":              "unix:///tmp/x.sock",
		"SILO_CSI_MODE":                  "node",
		"SILO_SERVER":                    "silod:7000",
		"SILO_CSI_NODE_ID":               "node-9",
		"SILO_CSI_NBD_ADDR":              "silod:10809",
		"SILO_CSI_NBD_RECONNECT_TIMEOUT": "90s",
		"SILO_CSI_NBD_REQUEST_TIMEOUT":   "45s",
		"SILO_CSI_HTTP_ADDR":             "127.0.0.1:7090",
		"SILO_CSI_STATE_DIR":             "/var/lib/silo-csi",
		"SILO_LOG_LEVEL":                 "debug",
		"SILO_LOG_FORMAT":                "text",
	}
	cfg, err := LoadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Mode != ModeNode || cfg.NodeID != "node-9" || cfg.SilodAddr != "silod:7000" || cfg.LogLevel != "debug" || cfg.LogFormat != "text" {
		t.Errorf("overrides = %+v", cfg)
	}
	if cfg.NBDReconnectTimeout != 90*time.Second || cfg.StateDir != "/var/lib/silo-csi" {
		t.Errorf("reconnect/state overrides = %+v", cfg)
	}
	if cfg.NBDRequestTimeout != 45*time.Second || cfg.HTTPAddr != "127.0.0.1:7090" {
		t.Errorf("request-timeout/http overrides = %+v", cfg)
	}
	if cfg.Mode.RunsController() {
		t.Error("node mode should not run the controller")
	}
	if !cfg.Mode.RunsNode() {
		t.Error("node mode should run the node service")
	}
}

func TestLoadConfig_Errors(t *testing.T) {
	if _, err := LoadConfig(func(k string) string {
		if k == "SILO_CSI_MODE" {
			return "sideways"
		}
		return ""
	}); err == nil {
		t.Error("an invalid mode should error")
	}
	if _, err := LoadConfig(func(k string) string {
		if k == "SILO_CSI_ENDPOINT" {
			return ""
		}
		return ""
	}); err != nil {
		t.Errorf("empty endpoint should default, got %v", err)
	}
	for _, bad := range []string{"not-a-duration", "-5m", "0s"} {
		if _, err := LoadConfig(func(k string) string {
			if k == "SILO_CSI_NBD_RECONNECT_TIMEOUT" {
				return bad
			}
			return ""
		}); err == nil {
			t.Errorf("reconnect timeout %q should error", bad)
		}
	}
	for _, bad := range []string{"soon", "-1m"} {
		if _, err := LoadConfig(func(k string) string {
			if k == "SILO_CSI_NBD_REQUEST_TIMEOUT" {
				return bad
			}
			return ""
		}); err == nil {
			t.Errorf("request timeout %q should error", bad)
		}
	}
	// Zero explicitly disables the request-timeout bound.
	cfg, err := LoadConfig(func(k string) string {
		if k == "SILO_CSI_NBD_REQUEST_TIMEOUT" {
			return "0"
		}
		return ""
	})
	if err != nil || cfg.NBDRequestTimeout != 0 {
		t.Errorf("request timeout 0 = (%v, %v), want disabled", cfg.NBDRequestTimeout, err)
	}
}

func TestModeControllerOnly(t *testing.T) {
	if !ModeController.RunsController() || ModeController.RunsNode() {
		t.Error("controller mode should run only the controller")
	}
}
