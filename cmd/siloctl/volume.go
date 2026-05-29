package main

import (
	"context"
	"fmt"
	"io"
	"strconv"

	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
)

func runVolume(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelpFlag(args[0]) {
		printVolumeUsage(stdout)
		if len(args) == 0 {
			return 2
		}
		return 0
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "create":
		return runVolumeCreate(rest, stdout, stderr)
	case "snapshot":
		return runVolumeSnapshot(rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "siloctl volume: unknown subcommand %q. Run 'siloctl volume help'.\n", sub)
		return 2
	}
}

func printVolumeUsage(w io.Writer) {
	fmt.Fprint(w, `siloctl volume — create and manage block volumes

Usage:
  siloctl volume create <path> --size <size> [--extent-size <size>]
  siloctl volume snapshot <source-path> <dest-path>

Sizes take an optional unit suffix: K, M, G, or T (powers of 1024); a bare
number is bytes. A volume backs an NBD block device — mount it with
nbd-client using the volume path as the export name.

A snapshot is a point-in-time, copy-on-write copy: it shares the source's
data until either side is written, so it is cheap to take and never blocks
the source. Take a snapshot while the source is idle (unmounted or quiesced)
for a crash-consistent image.

Each subcommand accepts --server=host:port to point at a different silod.
`)
}

func runVolumeCreate(args []string, stdout, stderr io.Writer) int {
	fs, server := newSubFlagSet("volume create", stderr)
	sizeStr := fs.String("size", "", "block-device size, e.g. 10G, 512M, or a bare byte count (required)")
	extentStr := fs.String("extent-size", "", "copy-on-write unit, e.g. 64K; empty uses the server default")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) != 1 {
		fmt.Fprintln(stderr, "Usage: siloctl volume create [--server=host:port] --size <size> [--extent-size <size>] <path>")
		return 2
	}
	path := rest[0]

	if *sizeStr == "" {
		fmt.Fprintln(stderr, "siloctl: --size is required, e.g. --size 10G")
		return 2
	}
	size, err := parseSize(*sizeStr)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: invalid --size %q (%v); use a number optionally suffixed with K, M, G, or T\n", *sizeStr, err)
		return 2
	}
	var extentSize int64
	if *extentStr != "" {
		extentSize, err = parseSize(*extentStr)
		if err != nil {
			fmt.Fprintf(stderr, "siloctl: invalid --extent-size %q (%v); use a number optionally suffixed with K, M, G, or T\n", *extentStr, err)
			return 2
		}
	}

	conn, err := dialer(*server)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not dial silod at %q (%v); check that silod is running and SILO_SERVER points at its gRPC address\n", *server, err)
		return 1
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := newNamespaceClient(conn).CreateVolume(ctx, &namespacev1.CreateVolumeRequest{
		Path:            path,
		SizeBytes:       size,
		ExtentSizeBytes: extentSize,
	})
	if err != nil {
		return reportRPC(stderr, "volume create", err)
	}
	fmt.Fprintf(stdout, "Created volume %s (%d bytes) as inode %s.\n", path, size, resp.GetInode())
	return 0
}

func runVolumeSnapshot(args []string, stdout, stderr io.Writer) int {
	fs, server := newSubFlagSet("volume snapshot", stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) != 2 {
		fmt.Fprintln(stderr, "Usage: siloctl volume snapshot [--server=host:port] <source-path> <dest-path>")
		return 2
	}
	source, dest := rest[0], rest[1]

	conn, err := dialer(*server)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not dial silod at %q (%v); check that silod is running and SILO_SERVER points at its gRPC address\n", *server, err)
		return 1
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := newNamespaceClient(conn).SnapshotVolume(ctx, &namespacev1.SnapshotVolumeRequest{
		SourcePath: source,
		DestPath:   dest,
	})
	if err != nil {
		return reportRPC(stderr, "volume snapshot", err)
	}
	fmt.Fprintf(stdout, "Snapshotted %s to %s as inode %s.\n", source, dest, resp.GetInode())
	return 0
}

// parseSize parses a byte count with an optional binary unit suffix
// (K, M, G, T = powers of 1024). A bare number is bytes. It rejects negative
// values and multiplications that would overflow int64.
func parseSize(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	mult := int64(1)
	switch s[len(s)-1] {
	case 'K', 'k':
		mult = 1 << 10
	case 'M', 'm':
		mult = 1 << 20
	case 'G', 'g':
		mult = 1 << 30
	case 'T', 't':
		mult = 1 << 40
	}
	num := s
	if mult != 1 {
		num = s[:len(s)-1]
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("size must not be negative")
	}
	result := n * mult
	if mult != 0 && result/mult != n {
		return 0, fmt.Errorf("size overflows a 64-bit byte count")
	}
	return result, nil
}
