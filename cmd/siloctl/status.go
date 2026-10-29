package main

import (
	"context"
	"fmt"
	"io"
	"strings"
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
	fmt.Fprintf(w, "Queried %s (silo %s)\n", resp.GetRespondingNodeId(), resp.GetVersion())
	printHealth(w, resp.GetRespondingNodeId(), resp.GetHealth())
	fmt.Fprintln(w)

	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tSTATE\tGOSSIP\tDATA\tCAPACITY\tSKEW\tLAST CHANGE")
	for _, n := range nodes {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			n.GetId(), nodeStateString(n.GetState()),
			orDash(n.GetGossipAddress()), orDash(n.GetDataAddress()),
			nodeCapacity(n),
			nodeSkew(n, resp.GetRespondingNodeId()),
			humanizeAge(n.GetLastChangeUnix()))
	}
	_ = tw.Flush()

	if st := resp.GetStorage(); st != nil {
		fmt.Fprintf(w, "\nStorage on %s (%s): %s used of %s (%s free), %d chunk%s\n",
			resp.GetRespondingNodeId(), orDash(st.GetDataDir()),
			humanizeBytes(st.GetUsedBytes()), humanizeBytes(st.GetCapacityBytes()),
			humanizeBytes(st.GetAvailableBytes()), st.GetChunkCount(), plural(int(st.GetChunkCount())))
	}
	printGC(w, resp.GetGc())
	printVolumes(w, resp.GetVolumes())
	printBackupStatus(w, resp.GetRespondingNodeId(), resp.GetBackup())
}

// printHealth renders the responding node's health line and a warning line
// for each signal that needs the operator. A server too old to send a health
// block prints nothing.
func printHealth(w io.Writer, node string, h *statusv1.ClusterHealth) {
	if h == nil {
		return
	}
	line := fmt.Sprintf("Health: replication %d, %d alive, max clock skew %s", h.GetReplicationFactor(), h.GetAliveNodes(), skewString(h.GetClockSkewMs()))
	if h.GetStartedUnix() > 0 {
		line += ", up " + humanizeDuration(statusNow().Sub(time.Unix(h.GetStartedUnix(), 0)))
	}
	fmt.Fprintln(w, line)
	if h.GetIsolated() {
		fmt.Fprintf(w, "  WARNING: %s sees fewer alive peers than it needs and is retrying its seeds (%d rejoin attempt%s); check SILO_SEEDS and gossip reachability\n",
			node, h.GetRejoinAttempts(), pluralCount(h.GetRejoinAttempts()))
	}
	if int64(h.GetReplicationFactor()) > int64(h.GetAliveNodes()) {
		fmt.Fprintf(w, "  WARNING: fewer alive nodes than the replication factor; new writes cannot reach a full replica set\n")
	}
	if a := h.GetClockSkewAlerts(); a > 0 {
		fmt.Fprintf(w, "  WARNING: %d clock-skew alert%s since start; check time sync (NTP/chrony) on every node\n", a, pluralCount(a))
	}
}

// printGC renders the chunk GC's mode and its last sweep. A server too old to
// send a GC block prints nothing.
func printGC(w io.Writer, gc *statusv1.GCStatus) {
	if gc == nil {
		return
	}
	mode := "reclaiming"
	if !gc.GetEnabled() {
		mode = "dry run (SILO_CHUNK_GC_ENABLE is off)"
	}
	fmt.Fprintf(w, "\nChunk GC: %s\n", mode)
	if gc.GetRunning() {
		fmt.Fprintln(w, "  sweeping now")
	}
	switch {
	case gc.GetLastRunUnix() == 0:
		fmt.Fprintln(w, "  last sweep: none since start")
	case gc.GetLastError() != "":
		fmt.Fprintf(w, "  last sweep: %s, did not complete: %s\n", humanizeAge(gc.GetLastRunUnix()), termSafe(gc.GetLastError()))
	default:
		fmt.Fprintf(w, "  last sweep: %s in %s, %d chunk%s reclaimed (%s)\n",
			humanizeAge(gc.GetLastRunUnix()), time.Duration(gc.GetLastDurationMs())*time.Millisecond,
			gc.GetLastOrphansDeleted(), pluralCount(gc.GetLastOrphansDeleted()), humanizeBytes(gc.GetLastBytesFreed()))
	}
}

// nodeCapacity is capacitySummary plus a marker when the node advertised disk
// pressure.
func nodeCapacity(n *statusv1.NodeStatus) string {
	s := capacitySummary(n.GetUsedBytes(), n.GetCapacityBytes())
	if n.GetPressured() {
		s += " PRESSURE"
	}
	return s
}

// nodeSkew shows the responding node's skew reading for a peer, and a dash for
// the responding node itself, which has nothing to compare against.
func nodeSkew(n *statusv1.NodeStatus, self string) string {
	if n.GetId() == self {
		return "—"
	}
	return skewString(n.GetClockSkewMs())
}

func skewString(ms int64) string {
	return (time.Duration(ms) * time.Millisecond).String()
}

// humanizeDuration renders an uptime coarsely: seconds, minutes, hours, days.
func humanizeDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(max(d, 0).Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours())/24)
	}
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
	fmt.Fprintln(tw, "PATH\tSIZE\tALLOCATED\tWRITER\tREPLICAS\tNOTES")
	for _, v := range vols {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			v.GetPath(), volumeSize(v.GetSizeBytes()),
			humanizeBytes(v.GetExtentsWritten()*v.GetExtentSizeBytes()),
			orDash(v.GetLeaseHolder()),
			orDash(strings.Join(v.GetReplicas(), ",")),
			volumeNotes(v))
	}
	_ = tw.Flush()
}

// volumeNotes flags what an operator should know about a volume on the
// responding node: whether it is attached here and whether this node's copy of
// its extent map is waiting to be rebuilt.
func volumeNotes(v *statusv1.VolumeStatus) string {
	notes := make([]string, 0, 2)
	if v.GetAttachedHere() {
		notes = append(notes, "attached here")
	}
	if v.GetMapDamaged() {
		notes = append(notes, "MAP DAMAGED")
	}
	return orDash(strings.Join(notes, ", "))
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

// pluralCount is plural for the wire's 64-bit counters.
func pluralCount[T int64 | uint64](n T) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
