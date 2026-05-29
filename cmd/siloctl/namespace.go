package main

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"

	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
)

// newNamespaceClient is a seam so tests can substitute a fake client.
var newNamespaceClient = func(conn *grpc.ClientConn) namespacev1.NamespaceStoreClient {
	return namespacev1.NewNamespaceStoreClient(conn)
}

func runNS(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelpFlag(args[0]) {
		printNSUsage(stdout)
		if len(args) == 0 {
			return 2
		}
		return 0
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "mkdir":
		return runNSMutate(rest, stdout, stderr, "ns mkdir", "Created directory")
	case "touch":
		return runNSMutate(rest, stdout, stderr, "ns touch", "Created file")
	case "rm":
		return runNSMutate(rest, stdout, stderr, "ns rm", "Removed")
	case "ls":
		return runNSList(rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "siloctl ns: unknown subcommand %q. Run 'siloctl ns help'.\n", sub)
		return 2
	}
}

func printNSUsage(w io.Writer) {
	fmt.Fprint(w, `siloctl ns — inspect and mutate the cluster namespace

Usage:
  siloctl ns mkdir <path>   Create a directory
  siloctl ns touch <path>   Create an empty file
  siloctl ns ls   [<path>]  List a directory (default: /)
  siloctl ns rm   <path>    Remove an entry

Each subcommand accepts --server=host:port to point at a different silod.
Mutations converge to other nodes over gossip within a few seconds.
`)
}

// runNSMutate handles the single-path mutating verbs (mkdir/touch/rm),
// which share dial + error handling and differ only in the RPC and the
// success line.
func runNSMutate(args []string, stdout, stderr io.Writer, op, success string) int {
	fs, server := newSubFlagSet(op, stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) != 1 {
		fmt.Fprintf(stderr, "Usage: siloctl %s [--server=host:port] <path>\n", op)
		return 2
	}
	path := rest[0]

	conn, err := dialer(*server)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not dial silod at %q (%v); check that silod is running and SILO_SERVER points at its gRPC address\n", *server, err)
		return 1
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	client := newNamespaceClient(conn)

	switch op {
	case "ns mkdir":
		_, err = client.Mkdir(ctx, &namespacev1.MkdirRequest{Path: path})
	case "ns touch":
		_, err = client.Touch(ctx, &namespacev1.TouchRequest{Path: path})
	default: // ns rm
		_, err = client.Remove(ctx, &namespacev1.RemoveRequest{Path: path})
	}
	if err != nil {
		return reportRPC(stderr, op, err)
	}
	fmt.Fprintf(stdout, "%s %s\n", success, path)
	return 0
}

func runNSList(args []string, stdout, stderr io.Writer) int {
	fs, server := newSubFlagSet("ns ls", stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	path := "/"
	if rest := fs.Args(); len(rest) == 1 {
		path = rest[0]
	} else if len(rest) > 1 {
		fmt.Fprintln(stderr, "Usage: siloctl ns ls [--server=host:port] [<path>]")
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
	resp, err := newNamespaceClient(conn).List(ctx, &namespacev1.ListRequest{Path: path})
	if err != nil {
		return reportRPC(stderr, "ns ls", err)
	}
	for _, e := range resp.GetEntries() {
		suffix := ""
		if e.GetType() == namespacev1.EntryType_ENTRY_TYPE_DIR {
			suffix = "/"
		}
		fmt.Fprintf(stdout, "%s%s\n", e.GetName(), suffix)
	}
	return 0
}
