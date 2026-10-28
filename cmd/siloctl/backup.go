package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/hyperized/silo/api/proto/silo/node/v1"
	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/blobstore"
)

// openBackupTarget is a seam so tests can inject a local or in-memory target.
var openBackupTarget = blobstore.Open

// restoreBackup is a seam so tests can drive restore outcomes that depend on
// the host, such as running out of disk space.
var restoreBackup = backup.Restore

const maxMissingListed = 50

func runBackup(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelpFlag(args[0]) {
		printBackupUsage(stdout)
		if len(args) == 0 {
			return 2
		}
		return 0
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "run":
		return runBackupRun(rest, stdout, stderr)
	case "list":
		return runBackupList(rest, stdout, stderr)
	case "verify":
		return runBackupVerify(rest, stdout, stderr)
	case "restore":
		return runBackupRestore(rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "siloctl backup: unknown subcommand %q. Run 'siloctl backup help'.\n", sub)
		return 2
	}
}

func printBackupUsage(w io.Writer) {
	fmt.Fprint(w, `siloctl backup: run, list, verify and restore backups

Usage:
  siloctl backup run [--server=host:port] [--wait=1h]
  siloctl backup list --target URL [target flags]
  siloctl backup verify --target URL --node ID [--manifest latest|STAMP] [--deep] [target flags]
  siloctl backup restore --target URL --node ID --data-dir DIR [--manifest latest|STAMP] [--concurrency=4] [target flags]

run asks the silod you point at to back up its own data now and waits for the
result. The other commands read the backup target directly and do not need a
running silod.

Target flags (each has an environment variable, used when the flag is absent):
  --target URL        where backups live, such as s3://bucket/prefix or a
                      directory path (SILO_BACKUP_TARGET)
  --s3-endpoint URL   S3-compatible service address, for non-AWS storage
                      (SILO_BACKUP_S3_ENDPOINT)
  --s3-region NAME    S3 region (SILO_BACKUP_S3_REGION)
  --s3-path-style     address buckets by path; on by default when
                      --s3-endpoint is set (SILO_BACKUP_S3_PATH_STYLE)

S3 credentials come from the standard AWS variables (AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_PROFILE). They are never passed on the command line.

verify checks that every object the backup needs is present with the size the
backup recorded. That only takes a few listings. Add --deep to also download
every object and compare its checksum, which reads the whole backup.

restore checks every object's checksum before writing it, and writes into DIR
only when it does not exist or is empty. It never deletes or overwrites files.
`)
}

// targetFlags holds the flags every target-reading subcommand shares.
type targetFlags struct {
	fs        *flag.FlagSet
	target    *string
	endpoint  *string
	region    *string
	pathStyle *bool
}

func addTargetFlags(fs *flag.FlagSet) *targetFlags {
	return &targetFlags{
		fs:        fs,
		target:    fs.String("target", os.Getenv("SILO_BACKUP_TARGET"), "backup target URL or directory (env SILO_BACKUP_TARGET)"),
		endpoint:  fs.String("s3-endpoint", os.Getenv("SILO_BACKUP_S3_ENDPOINT"), "S3-compatible endpoint URL (env SILO_BACKUP_S3_ENDPOINT)"),
		region:    fs.String("s3-region", os.Getenv("SILO_BACKUP_S3_REGION"), "S3 region (env SILO_BACKUP_S3_REGION)"),
		pathStyle: fs.Bool("s3-path-style", false, "address buckets by path (env SILO_BACKUP_S3_PATH_STYLE; default on when --s3-endpoint is set)"),
	}
}

// parseBoolEnv accepts the spellings operators commonly use for a switch.
func parseBoolEnv(v string) (value, ok bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true, true
	case "0", "false", "no", "off":
		return false, true
	default:
		return false, false
	}
}

// open validates the shared flags and opens the target. A non-zero code means
// the problem is already reported.
func (t *targetFlags) open(stderr io.Writer) (blobstore.Target, int) {
	if *t.target == "" {
		fmt.Fprintln(stderr, "siloctl: no backup target given. Pass --target, or set SILO_BACKUP_TARGET, to a location such as s3://bucket/prefix or a directory path.")
		return nil, 2
	}
	opts := make([]blobstore.Option, 0, 3)
	if *t.endpoint != "" {
		if err := blobstore.ValidateS3Endpoint(*t.endpoint); err != nil {
			fmt.Fprintf(stderr, "siloctl: --s3-endpoint is not usable: %v\n", err)
			return nil, 2
		}
		opts = append(opts, blobstore.WithS3Endpoint(*t.endpoint))
	}
	region := *t.region
	if region == "" && *t.endpoint != "" {
		region = "us-east-1"
	}
	if region != "" {
		opts = append(opts, blobstore.WithS3Region(region))
	}

	pathStyle, err := t.resolvePathStyle()
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: %v\n", err)
		return nil, 2
	}
	opts = append(opts, blobstore.WithS3PathStyle(pathStyle))

	target, err := openBackupTarget(*t.target, opts...)
	if err != nil {
		// blobstore.Open never repeats the URL, which may hold a pasted
		// secret; do not add it back here.
		fmt.Fprintf(stderr, "siloctl: could not open the backup target (%s). Check the --target value.\n", termSafe(err.Error()))
		return nil, 2
	}
	return target, 0
}

// resolvePathStyle applies the precedence: an explicit flag wins (even
// --s3-path-style=false), then the environment variable, then on-when-custom-endpoint.
func (t *targetFlags) resolvePathStyle() (bool, error) {
	explicit := false
	t.fs.Visit(func(f *flag.Flag) {
		if f.Name == "s3-path-style" {
			explicit = true
		}
	})
	if explicit {
		return *t.pathStyle, nil
	}
	if env := os.Getenv("SILO_BACKUP_S3_PATH_STYLE"); env != "" {
		v, ok := parseBoolEnv(env)
		if !ok {
			return false, fmt.Errorf("SILO_BACKUP_S3_PATH_STYLE=%q is not a yes/no value. Use true or false", env)
		}
		return v, nil
	}
	return *t.endpoint != "", nil
}

func newBackupFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("siloctl backup "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func runBackupRun(args []string, stdout, stderr io.Writer) int {
	fs, server := newSubFlagSet("backup run", stderr)
	wait := fs.Duration("wait", time.Hour, "how long to wait for the backup to finish")
	if err := parseFlexible(fs, args); err != nil {
		return 2
	}
	if len(fs.Args()) != 0 {
		fmt.Fprintln(stderr, "Usage: siloctl backup run [--server=host:port] [--wait=1h]")
		return 2
	}
	if *wait <= 0 {
		fmt.Fprintln(stderr, "siloctl: --wait must be longer than zero, for example --wait=1h.")
		return 2
	}

	conn, err := dialer(*server)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not dial silod at %q (%v); check that silod is running and SILO_SERVER points at its gRPC address\n", *server, err)
		return 1
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), *wait)
	defer cancel()
	resp, err := newNodeAdminClient(conn).RunBackup(ctx, &nodev1.RunBackupRequest{})
	if err != nil {
		if status.Code(err) == codes.DeadlineExceeded {
			fmt.Fprintf(stderr, "siloctl: stopped waiting after %s. The backup keeps running on the node; run 'siloctl status' later to see the result, or raise --wait.\n", *wait)
			return 1
		}
		return reportRPC(stderr, "backup run", err)
	}
	run := resp.GetRun()
	if !run.GetOk() {
		fmt.Fprintf(stderr, "Backup on node %s failed: %s\n", resp.GetNodeId(), termSafe(run.GetError()))
		return 1
	}
	elapsed := time.Duration(run.GetFinishedUnix()-run.GetStartedUnix()) * time.Second
	fmt.Fprintf(stdout, "Backup finished on node %s in %s.\n", resp.GetNodeId(), elapsed)
	fmt.Fprintf(stdout, "Uploaded %d chunk%s (%s); %d %s already on the target.\n",
		run.GetChunksUploaded(), plural(int(run.GetChunksUploaded())), humanizeBytes(run.GetBytesUploaded()),
		run.GetChunksSkipped(), wasWere(run.GetChunksSkipped()))
	fmt.Fprintf(stdout, "Restore point: %s\n", run.GetManifest())
	return 0
}

func wasWere(n int64) string {
	if n == 1 {
		return "was"
	}
	return "were"
}

func runBackupList(args []string, stdout, stderr io.Writer) int {
	fs := newBackupFlagSet("list", stderr)
	tf := addTargetFlags(fs)
	if err := parseFlexible(fs, args); err != nil {
		return 2
	}
	if len(fs.Args()) != 0 {
		fmt.Fprintln(stderr, "Usage: siloctl backup list --target URL [--s3-endpoint URL] [--s3-region NAME] [--s3-path-style]")
		return 2
	}
	target, code := tf.open(stderr)
	if code != 0 {
		return code
	}

	catalog, err := backup.ListManifests(context.Background(), target)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not list backups (%s). Check that the target is reachable and the credentials may read it.\n", termSafe(err.Error()))
		return 1
	}
	if len(catalog) == 0 {
		fmt.Fprintf(stdout, "No backups were found at %s. Check that --target (or SILO_BACKUP_TARGET) points at the location, including its prefix, that silod writes to.\n", target.Name())
		return 0
	}

	nodes := make([]string, 0, len(catalog))
	for n := range catalog {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	tw := tabwriter.NewWriter(stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tBACKUP\tCHUNKS\tSIZE\tSILO VERSION")
	for _, n := range nodes {
		for _, info := range catalog[n] {
			m := info.Manifest
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\n", n, info.Name, m.ChunkCount, humanizeBytes(m.ChunkBytes), orDash(m.SiloVersion))
		}
	}
	_ = tw.Flush()
	fmt.Fprintf(stdout, "\nRestore one with: siloctl backup restore --target %s --node NODE --data-dir DIR [--manifest BACKUP]\n", target.Name())
	return 0
}

// resolveNodeAndManifest validates the --node and --manifest values shared by
// verify and restore.
func resolveNodeAndManifest(node, manifest string, stderr io.Writer) (string, int) {
	if node == "" {
		fmt.Fprintln(stderr, "siloctl: --node is required. Pass the id of the node whose backup you want; 'siloctl backup list' shows the ids.")
		return "", 2
	}
	if err := backup.ValidateNodeID(node); err != nil {
		fmt.Fprintf(stderr, "siloctl: %v\n", err)
		return "", 2
	}
	name, err := backup.ParseManifestName(manifest)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: %v\n", err)
		return "", 2
	}
	return name, 0
}

func loadBackup(target blobstore.Target, node, name string, stderr io.Writer) (*backup.Manifest, int) {
	m, err := backup.LoadManifest(context.Background(), target, node, name)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not load backup %q of node %s (%s). Run 'siloctl backup list' to see what the target holds.\n", name, node, termSafe(err.Error()))
		return nil, 1
	}
	return m, 0
}

func backupStamp(m *backup.Manifest) string { return m.Stamp() }

func runBackupVerify(args []string, stdout, stderr io.Writer) int {
	fs := newBackupFlagSet("verify", stderr)
	tf := addTargetFlags(fs)
	node := fs.String("node", "", "id of the node whose backup to check")
	manifest := fs.String("manifest", backup.LatestManifest, "backup to check: latest or a timestamp from 'siloctl backup list'")
	deep := fs.Bool("deep", false, "download every object and compare its checksum (slower; reads the whole backup)")
	if err := parseFlexible(fs, args); err != nil {
		return 2
	}
	if len(fs.Args()) != 0 {
		fmt.Fprintln(stderr, "Usage: siloctl backup verify --target URL --node ID [--manifest latest|STAMP] [--deep]")
		return 2
	}
	name, code := resolveNodeAndManifest(*node, *manifest, stderr)
	if code != 0 {
		return code
	}
	target, code := tf.open(stderr)
	if code != 0 {
		return code
	}
	m, code := loadBackup(target, *node, name, stderr)
	if code != 0 {
		return code
	}

	problems, err := backup.Verify(context.Background(), target, m, backup.VerifyOptions{Deep: *deep})
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not check the backup (%s). Check that the target is reachable and the credentials may read it.\n", termSafe(err.Error()))
		return 1
	}
	stamp := backupStamp(m)
	if len(problems) == 0 {
		total := len(m.Chunks) + len(m.Extents) + 1
		how := "present with the recorded sizes"
		if *deep {
			how = "present and match their recorded checksums"
		}
		fmt.Fprintf(stdout, "All %d objects referenced by backup %s of node %s are %s.\n", total, stamp, *node, how)
		return 0
	}
	fmt.Fprintf(stderr, "Backup %s of node %s has %d problem object%s:\n", stamp, *node, len(problems), plural(len(problems)))
	for i, p := range problems {
		if i == maxMissingListed {
			fmt.Fprintf(stderr, "  …and %d more\n", len(problems)-maxMissingListed)
			break
		}
		fmt.Fprintf(stderr, "  %s: %s\n", p.Name, p.Reason)
	}
	fmt.Fprintln(stderr, "This backup cannot be restored completely. Restore an older backup instead; 'siloctl backup list' shows which ones exist, and 'siloctl backup verify --manifest STAMP' checks one.")
	return 1
}

// termSafe replaces control characters in text that came from a bucket or a
// storage service, so an error message cannot smuggle terminal escape
// sequences onto the operator's screen.
func termSafe(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f) {
			return '?'
		}
		return r
	}, s)
}

func runBackupRestore(args []string, stdout, stderr io.Writer) int {
	fs := newBackupFlagSet("restore", stderr)
	tf := addTargetFlags(fs)
	node := fs.String("node", "", "id of the node whose backup to restore")
	dir := fs.String("data-dir", "", "new or empty directory to restore into")
	manifest := fs.String("manifest", backup.LatestManifest, "backup to restore: latest or a timestamp from 'siloctl backup list'")
	concurrency := fs.Int("concurrency", backup.DefaultConcurrency, "number of chunks to download at once")
	if err := parseFlexible(fs, args); err != nil {
		return 2
	}
	if len(fs.Args()) != 0 {
		fmt.Fprintln(stderr, "Usage: siloctl backup restore --target URL --node ID --data-dir DIR [--manifest latest|STAMP] [--concurrency=4]")
		return 2
	}
	if *dir == "" {
		fmt.Fprintln(stderr, "siloctl: --data-dir is required. Pass a new or empty directory to restore into, for example --data-dir=/var/lib/silo.")
		return 2
	}
	if *concurrency < 1 {
		fmt.Fprintln(stderr, "siloctl: --concurrency must be at least 1, for example --concurrency=4.")
		return 2
	}
	name, code := resolveNodeAndManifest(*node, *manifest, stderr)
	if code != 0 {
		return code
	}
	target, code := tf.open(stderr)
	if code != 0 {
		return code
	}
	m, code := loadBackup(target, *node, name, stderr)
	if code != 0 {
		return code
	}

	fmt.Fprintf(stdout, "Restoring node %s from the backup of %s: %d chunk%s, %s, made by silo %s.\n",
		*node, backupStamp(m), m.ChunkCount, plural(m.ChunkCount), humanizeBytes(m.ChunkBytes), orDash(m.SiloVersion))

	var mu sync.Mutex
	lastDecile := 0
	opts := backup.RestoreOptions{
		Concurrency: *concurrency,
		Progress: func(done, total int) {
			mu.Lock()
			defer mu.Unlock()
			pct := done * 100 / total
			if pct/10 > lastDecile || (done == total && lastDecile < 10) {
				lastDecile = pct / 10
				fmt.Fprintf(stdout, "  %d%% (%d of %d chunks)\n", pct, done, total)
			}
		},
		Notice: func(msg string) { fmt.Fprintf(stdout, "Note: %s\n", termSafe(msg)) },
	}
	res, err := restoreBackup(context.Background(), target, m, *dir, opts)
	if err != nil {
		if errors.Is(err, backup.ErrDataDirNotEmpty) {
			fmt.Fprintf(stderr, "siloctl: %s\nChoose a new directory; siloctl never deletes or overwrites files.\n", termSafe(err.Error()))
			return 1
		}
		if errors.Is(err, backup.ErrInsufficientSpace) {
			fmt.Fprintf(stderr, "siloctl: %s\n", termSafe(err.Error()))
			return 1
		}
		fmt.Fprintf(stderr, "siloctl: restore failed (%s). Delete %s, check the target and your credentials, then run the restore again.\n", termSafe(err.Error()), *dir)
		return 1
	}

	fmt.Fprintf(stdout, "\nRestored %d chunk%s (%s) and %d volume%s into %s.\n\n",
		res.Chunks, plural(res.Chunks), humanizeBytes(res.Bytes), res.Volumes, plural(res.Volumes), *dir)
	fmt.Fprintf(stdout, `Next steps:
  1. Start silod with SILO_DATA_DIR=%s and SILO_NODE_ID=%s.
  2. Use the same encryption key the cluster used (SILO_ENCRYPTION_KEY, or the
     same SILO_ENCRYPTION_KEY_SOURCE and SILO_ENCRYPTION_KEY_PATH). The chunks
     are still encrypted with it and cannot be read without it.
  3. Backups do not contain TLS keys. Copy the node's ca.crt, node.crt and
     node.key (and ca.key on the node that seeds the CA) into %s, or point
     SILO_TLS_CA_CERT, SILO_TLS_NODE_CERT and SILO_TLS_NODE_KEY at them.
  4. When restoring a whole cluster, restore every node from its own backup
     before starting any of them.
`, *dir, *node, *dir)
	return 0
}
