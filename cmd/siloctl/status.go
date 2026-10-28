package main

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"

	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
)

// newStatusClient is a seam so tests can substitute a fake client.
var newStatusClient = func(conn *grpc.ClientConn) statusv1.ClusterStatusClient {
	return statusv1.NewClusterStatusClient(conn)
}

// statusNow is the clock used to render node-state ages; overridable in tests.
var statusNow = time.Now

func runStatus(args []string, stdout, stderr io.Writer) int {
	fs, server := newSubFlagSet("status", stderr)
	if err := parseFlexible(fs, args); err != nil {
		return 2
	}
	if len(fs.Args()) != 0 {
		fmt.Fprintln(stderr, "Usage: siloctl status [--server=host:port]")
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
	resp, err := newStatusClient(conn).GetStatus(ctx, &statusv1.GetStatusRequest{})
	if err != nil {
		return reportRPC(stderr, "status", err)
	}
	printStatus(stdout, resp)
	return 0
}

// printStatus renders a cluster-health summary, a per-node table, the
// responding node's storage, the volumes it knows about and its backup state.
func printStatus(w io.Writer, resp *statusv1.GetStatusResponse) {
	nodes := resp.GetNodes()
	counts := map[statusv1.NodeState]int{}
	for _, n := range nodes {
		counts[n.GetState()]++
	}
	fmt.Fprintf(w, "Cluster: %d node%s%s\n", len(nodes), plural(len(nodes)), healthSummary(counts))
	fmt.Fprintf(w, "Queried %s (silo %s)\n\n", resp.GetRespondingNodeId(), resp.GetVersion())

	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tSTATE\tGOSSIP\tDATA\tCAPACITY\tLAST CHANGE")
	for _, n := range nodes {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			n.GetId(), nodeStateString(n.GetState()),
			orDash(n.GetGossipAddress()), orDash(n.GetDataAddress()),
			capacitySummary(n.GetUsedBytes(), n.GetCapacityBytes()),
			humanizeAge(n.GetLastChangeUnix()))
	}
	_ = tw.Flush()

	if st := resp.GetStorage(); st != nil {
		fmt.Fprintf(w, "\nStorage on %s (%s): %s used of %s (%s free), %d chunk%s\n",
			resp.GetRespondingNodeId(), orDash(st.GetDataDir()),
			humanizeBytes(st.GetUsedBytes()), humanizeBytes(st.GetCapacityBytes()),
			humanizeBytes(st.GetAvailableBytes()), st.GetChunkCount(), plural(int(st.GetChunkCount())))
	}
	printVolumes(w, resp.GetVolumes())
	printBackupStatus(w, resp.GetRespondingNodeId(), resp.GetBackup())
}

// printVolumes renders the volume table. ALLOCATED is extents written times
// the extent size, so it rounds up to whole extents; it reads 0 B on a node
// that does not hold a copy of that volume's extent map.
func printVolumes(w io.Writer, vols []*statusv1.VolumeStatus) {
	if len(vols) == 0 {
		fmt.Fprintln(w, "\nVolumes: none yet (create one with 'siloctl volume create --size 10G /my-volume')")
		return
	}
	fmt.Fprintf(w, "\nVolumes: %d\n", len(vols))
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "PATH\tSIZE\tALLOCATED\tWRITER")
	for _, v := range vols {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			v.GetPath(), volumeSize(v.GetSizeBytes()),
			humanizeBytes(v.GetExtentsWritten()*v.GetExtentSizeBytes()),
			orDash(v.GetLeaseHolder()))
	}
	_ = tw.Flush()
}

// volumeSize shows a dash for a volume created without a size, which cannot be
// attached as a block device until it has one.
func volumeSize(n int64) string {
	if n <= 0 {
		return "—"
	}
	return humanizeBytes(n)
}

// printBackupStatus renders the responding node's backup schedule and last
// result.
func printBackupStatus(w io.Writer, node string, b *statusv1.BackupStatus) {
	if !b.GetConfigured() {
		fmt.Fprintf(w, "\nBackup: not configured on %s (set SILO_BACKUP_TARGET to enable)\n", node)
		return
	}
	fmt.Fprintf(w, "\nBackup: %s, every %s\n", termSafe(b.GetTarget()), time.Duration(b.GetIntervalSeconds())*time.Second)
	if b.GetRunning() {
		fmt.Fprintln(w, "  running now")
	}
	switch last := b.GetLastRun(); {
	case last == nil:
		fmt.Fprintln(w, "  last run: none yet")
	case last.GetOk():
		fmt.Fprintf(w, "  last run: ok %s (%d uploaded, %d already stored)\n", humanizeAge(last.GetFinishedUnix()), last.GetChunksUploaded(), last.GetChunksSkipped())
	default:
		fmt.Fprintf(w, "  last run: FAILED %s: %s\n", humanizeAge(last.GetFinishedUnix()), termSafe(last.GetError()))
	}
	if b.GetNextRunUnix() == 0 {
		fmt.Fprintln(w, "  next run: not scheduled")
		return
	}
	fmt.Fprintf(w, "  next run: %s\n", humanizeUntil(b.GetNextRunUnix()))
}

// humanizeUntil renders an epoch-seconds timestamp as a time from now.
func humanizeUntil(unix int64) string {
	d := time.Unix(unix, 0).Sub(statusNow())
	switch {
	case d <= 0:
		return "due now"
	case d < time.Minute:
		return fmt.Sprintf("in %ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("in %dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("in %dh", int(d.Hours()))
	default:
		return fmt.Sprintf("in %dd", int(d.Hours())/24)
	}
}

// healthSummary turns the per-state counts into " — 2 alive, 1 suspect", or
// "" when there are no nodes.
func healthSummary(counts map[statusv1.NodeState]int) string {
	order := []statusv1.NodeState{
		statusv1.NodeState_NODE_STATE_ALIVE,
		statusv1.NodeState_NODE_STATE_SUSPECT,
		statusv1.NodeState_NODE_STATE_DEAD,
		statusv1.NodeState_NODE_STATE_LEFT,
	}
	parts := make([]string, 0, len(order))
	for _, s := range order {
		if counts[s] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[s], nodeStateString(s)))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	out := " — "
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}

func nodeStateString(s statusv1.NodeState) string {
	switch s {
	case statusv1.NodeState_NODE_STATE_ALIVE:
		return "alive"
	case statusv1.NodeState_NODE_STATE_SUSPECT:
		return "suspect"
	case statusv1.NodeState_NODE_STATE_DEAD:
		return "dead"
	case statusv1.NodeState_NODE_STATE_LEFT:
		return "left"
	default:
		return "unknown"
	}
}

// humanizeAge renders an epoch-seconds timestamp as an age relative to now.
func humanizeAge(unix int64) string {
	if unix <= 0 {
		return "—"
	}
	d := statusNow().Sub(time.Unix(unix, 0))
	switch {
	case d < 0:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours())/24)
	}
}

// humanizeBytes renders a byte count with a binary unit (KiB, MiB, …).
func humanizeBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// capacitySummary renders a node's "used/capacity (pct)", or a dash when the
// node has not advertised its capacity yet.
func capacitySummary(used, capacity int64) string {
	if capacity <= 0 {
		return "—"
	}
	pct := float64(used) / float64(capacity) * 100
	return fmt.Sprintf("%s/%s (%.0f%%)", humanizeBytes(used), humanizeBytes(capacity), pct)
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
