package csi

import "testing"

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Endpoint != DefaultEndpoint || cfg.Mode != DefaultMode || cfg.SilodAddr != DefaultSilodAddr || cfg.NBDAddr != DefaultNBDAddr {
		t.Errorf("defaults = %+v", cfg)
	}
	if !cfg.Mode.RunsController() || !cfg.Mode.RunsNode() {
		t.Error("default mode should run both controller and node")
	}
}

func TestLoadConfig_Overrides(t *testing.T) {
	env := map[string]string{
		"SILO_CSI_ENDPOINT": "unix:///tmp/x.sock",
		"SILO_CSI_MODE":     "node",
		"SILO_SERVER":       "silod:7000",
		"SILO_CSI_NODE_ID":  "node-9",
		"SILO_CSI_NBD_ADDR": "silod:10809",
		"SILO_LOG_LEVEL":    "debug",
		"SILO_LOG_FORMAT":   "text",
	}
	cfg, err := LoadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Mode != ModeNode || cfg.NodeID != "node-9" || cfg.SilodAddr != "silod:7000" || cfg.LogLevel != "debug" || cfg.LogFormat != "text" {
		t.Errorf("overrides = %+v", cfg)
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
}

func TestModeControllerOnly(t *testing.T) {
	if !ModeController.RunsController() || ModeController.RunsNode() {
		t.Error("controller mode should run only the controller")
	}
}
