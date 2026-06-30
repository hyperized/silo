package config_test

import (
	"testing"

	"github.com/hyperized/silo/internal/config"
)

// FuzzConfigLoad hardens environment parsing. Every SILO_* value is
// operator-edited config (env or a .env file), so arbitrary strings for the
// address, integer, duration, and base64-key fields must produce an error,
// never a panic. The fuzzed values flow through net.SplitHostPort,
// strconv.ParseInt, time.ParseDuration, and base64 decoding.
func FuzzConfigLoad(f *testing.F) {
	f.Add("0.0.0.0:7000", "3", "30s", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	f.Add("", "", "", "")
	f.Add("::bad::", "-1", "soon", "!!not-base64")

	f.Fuzz(func(_ *testing.T, addr, repl, dur, key string) {
		env := func(k string) string {
			switch k {
			case "SILO_NODE_ID":
				return "fuzz-node"
			case "SILO_GRPC_ADDR":
				return addr
			case "SILO_REPLICATION", "SILO_MAX_CONCURRENT_WRITES":
				return repl
			case "SILO_SCRUB_INTERVAL", "SILO_TOMBSTONE_RETENTION", "SILO_MAX_CLOCK_SKEW",
				"SILO_EXTENT_REAP_AFTER", "SILO_EXTENT_REAP_INTERVAL", "SILO_EXTENT_SCRUB_INTERVAL",
				"SILO_CHUNK_GC_INTERVAL", "SILO_CHUNK_GC_GRACE":
				return dur
			case "SILO_ENCRYPTION_KEY":
				return key
			default:
				return ""
			}
		}
		_, _ = config.Load(env)
	})
}
