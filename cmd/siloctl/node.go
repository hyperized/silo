package main

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"

	nodev1 "github.com/hyperized/silo/api/proto/silo/node/v1"
)

// newNodeAdminClient is a seam so tests can substitute a fake client.
var newNodeAdminClient = func(conn *grpc.ClientConn) nodev1.NodeAdminClient {
	return nodev1.NewNodeAdminClient(conn)
}

func runNode(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelpFlag(args[0]) {
		printNodeUsage(stdout)
		if len(args) == 0 {
			return 2
		}
		return 0
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "drain":
		return runNodeDrain(rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "siloctl node: unknown subcommand %q. Run 'siloctl node help'.\n", sub)
		return 2
	}
}

func printNodeUsage(w io.Writer) {
	fmt.Fprint(w, `siloctl node — node lifecycle operations

Usage:
  siloctl node drain [--server=host:port]

Drain marks the target node as leaving the cluster and announces it over gossip.
Peers re-replicate the chunks it held onto other nodes; the node keeps running
and serving until you remove it — drain is not shutdown.

After draining, watch silo_replication_shortfall_chunks fall to zero (on the
node's /metrics endpoint) before you stop and remove the node. While it is
above zero, the cluster is still rebuilding the drained node's replicas.

Each subcommand accepts --server=host:port to target a specific silod.
`)
}

func runNodeDrain(args []string, stdout, stderr io.Writer) int {
	fs, server := newSubFlagSet("node drain", stderr)
	if err := parseFlexible(fs, args); err != nil {
		return 2
	}
	if len(fs.Args()) != 0 {
		fmt.Fprintln(stderr, "Usage: siloctl node drain [--server=host:port]")
		return 2
	}

	conn, err := dialer(*server)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not dial silod at %q (%v); check that silod is running and SILO_SERVER points at its gRPC address\n", *server, err)
		return 1
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := newNodeAdminClient(conn).Drain(ctx, &nodev1.DrainRequest{})
	if err != nil {
		return reportRPC(stderr, "node drain", err)
	}
	if resp.GetAnnounced() {
		fmt.Fprintf(stdout, "Node %s is draining. Its chunks are re-replicating to survivors.\n", resp.GetNodeId())
		fmt.Fprintln(stdout, "Keep it running until silo_replication_shortfall_chunks reaches zero, then it is safe to remove.")
	} else {
		fmt.Fprintf(stdout, "Node %s was already draining; no change.\n", resp.GetNodeId())
	}
	return 0
}
