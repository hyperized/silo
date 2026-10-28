package transport

import (
	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
	"github.com/hyperized/silo/internal/backup"
)

// BackupReporter is the slice of the backup subsystem the status service
// reads. *backup.Subsystem satisfies it.
type BackupReporter interface {
	Status() backup.Status
}

// WithBackupReporter makes GetStatus report the node's backup state. Without
// it (backups not configured) the response says configured=false.
func WithBackupReporter(r BackupReporter) StatusOption {
	return func(s *StatusService) { s.backup = r }
}

// backupStatusProto converts the subsystem's snapshot to its wire form. A nil
// reporter is an unconfigured node.
func backupStatusProto(r BackupReporter) *statusv1.BackupStatus {
	if r == nil {
		return &statusv1.BackupStatus{}
	}
	st := r.Status()
	out := &statusv1.BackupStatus{
		Configured:      st.Configured,
		Target:          st.Target,
		IntervalSeconds: int64(st.Interval.Seconds()),
		Running:         st.Running,
		LastRun:         backupRunProto(st.LastRun),
	}
	if !st.NextRun.IsZero() {
		out.NextRunUnix = st.NextRun.Unix()
	}
	return out
}

// backupRunProto converts one run outcome; nil stays nil (no run yet).
func backupRunProto(r *backup.Run) *statusv1.BackupRun {
	if r == nil {
		return nil
	}
	return &statusv1.BackupRun{
		StartedUnix:    r.StartedAt.Unix(),
		FinishedUnix:   r.FinishedAt.Unix(),
		Ok:             r.OK,
		Error:          r.Error,
		ChunksUploaded: r.ChunksUploaded,
		ChunksSkipped:  r.ChunksSkipped,
		BytesUploaded:  r.BytesUploaded,
		Manifest:       r.Manifest,
	}
}
