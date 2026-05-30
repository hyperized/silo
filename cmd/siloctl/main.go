// Command siloctl is the operator CLI for silo: a single binary that
// talks to silod over gRPC. The dispatcher is hand-rolled on stdlib
// flag/os to avoid pulling in a CLI framework for what is a small
// command surface.
package main

import (
	"fmt"
	"io"
	"os"
)

// version is set at build time via -ldflags "-X main.version=…".
var version = "dev"

const defaultServer = "127.0.0.1:7000"

func main() {
	os.Exit(runMain(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// runMain isolates the top-level dispatch so tests can exercise CLI
// flows without spawning a subprocess. exit codes follow the usual
// convention: 0 success, 1 runtime failure, 2 usage error.
func runMain(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelpFlag(args[0]) {
		printUsage(stdout)
		if len(args) == 0 {
			return 2
		}
		return 0
	}

	cmd := args[0]
	rest := args[1:]

	switch cmd {
	case "auth":
		return runAuth(rest, stdout, stderr)
	case "chunk":
		return runChunk(rest, stdin, stdout, stderr)
	case "ns":
		return runNS(rest, stdout, stderr)
	case "volume":
		return runVolume(rest, stdout, stderr)
	case "status":
		return runStatus(rest, stdout, stderr)
	case "node":
		return runNode(rest, stdout, stderr)
	case "version":
		fmt.Fprintf(stdout, "siloctl %s\n", version)
		return 0
	default:
		fmt.Fprintf(stderr, "siloctl: unknown command %q. Run 'siloctl help' for the list of commands.\n", cmd)
		return 2
	}
}

func isHelpFlag(s string) bool {
	return s == "help" || s == "-h" || s == "--help"
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `siloctl — operator CLI for the silo storage system

Usage:
  siloctl <command> [args...]

Commands:
  auth       Claim cluster credentials from silod (run once per machine)
  chunk      Manage individual chunks on a silod node
  ns         Inspect and mutate the cluster namespace
  volume     Create and manage block volumes
  status     Show cluster health and this node's storage
  node       Node lifecycle operations (drain)
  version    Print the siloctl version
  help       Show this message

Run 'siloctl <command> help' for command-specific help.

Connection:
  By default siloctl talks to a silod on 127.0.0.1:7000. Override with
  --server=host:port on any chunk subcommand, or set SILO_SERVER in your
  shell environment.
`)
}

// envDefault returns the value of the env var, or the fallback if unset.
func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
