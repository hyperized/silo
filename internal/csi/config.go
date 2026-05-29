package csi

import "fmt"

// Mode selects which CSI services a silo-csi process runs. In Kubernetes the
// controller runs once per cluster (a Deployment) and the node plugin runs on
// every node (a DaemonSet); "all" runs both in one process, handy for local
// development and single-node clusters.
type Mode string

const (
	// ModeController runs only the Controller service (provision/snapshot).
	ModeController Mode = "controller"
	// ModeNode runs only the Node service (attach/mount).
	ModeNode Mode = "node"
	// ModeAll runs the Controller and Node services together.
	ModeAll Mode = "all"
)

// RunsController reports whether this mode serves the Controller service.
func (m Mode) RunsController() bool { return m == ModeController || m == ModeAll }

// RunsNode reports whether this mode serves the Node service.
func (m Mode) RunsNode() bool { return m == ModeNode || m == ModeAll }

// Config is silo-csi's process configuration, read from the environment.
type Config struct {
	// Endpoint is the CSI gRPC socket the kubelet and sidecars connect to,
	// typically a unix:// path under the plugin directory.
	Endpoint string
	// Mode selects which services to run.
	Mode Mode
	// SilodAddr is the silod gRPC address the controller dials for namespace
	// operations.
	SilodAddr string
	// NodeID identifies this node to Kubernetes and is the volume lease holder
	// for volumes attached here.
	NodeID string
	// NBDAddr is the local silod NBD address the node plugin attaches volumes
	// through.
	NBDAddr string
	// LogLevel and LogFormat configure structured logging, matching silod.
	LogLevel  string
	LogFormat string
}

// Config defaults. The endpoint and NBD address match the conventional CSI
// socket path and silod's default NBD port; the silod address matches siloctl's
// default so the same value works across the tools.
const (
	DefaultEndpoint  = "unix:///csi/csi.sock"
	DefaultMode      = ModeAll
	DefaultSilodAddr = "127.0.0.1:7000"
	DefaultNBDAddr   = "127.0.0.1:10809"
)

// LoadConfig reads silo-csi's configuration from getenv (os.Getenv in
// production), applying defaults and validating the mode. NodeID is left as the
// caller finds it — the binary fills an empty value with the host name — so
// LoadConfig stays free of host lookups and easy to test.
func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := Config{
		Endpoint:  envOr(getenv, "SILO_CSI_ENDPOINT", DefaultEndpoint),
		Mode:      Mode(envOr(getenv, "SILO_CSI_MODE", string(DefaultMode))),
		SilodAddr: envOr(getenv, "SILO_SERVER", DefaultSilodAddr),
		NodeID:    getenv("SILO_CSI_NODE_ID"),
		NBDAddr:   envOr(getenv, "SILO_CSI_NBD_ADDR", DefaultNBDAddr),
		LogLevel:  envOr(getenv, "SILO_LOG_LEVEL", "info"),
		LogFormat: envOr(getenv, "SILO_LOG_FORMAT", "json"),
	}
	switch cfg.Mode {
	case ModeController, ModeNode, ModeAll:
	default:
		return Config{}, fmt.Errorf("SILO_CSI_MODE %q is not valid; set it to controller, node, or all", cfg.Mode)
	}
	return cfg, nil
}

// envOr returns the environment value for key, or fallback when it is unset.
func envOr(getenv func(string) string, key, fallback string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return fallback
}
