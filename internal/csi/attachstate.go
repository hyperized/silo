package csi

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// attachmentRecord is one attached volume as remembered across node-plugin
// restarts: which device serves which export, and at what size. Without this
// file a restarted plugin would forget its live attachments — it cannot ask
// the kubelet (published volumes are not re-published) and it must not orphan
// devices that pods are still writing through.
type attachmentRecord struct {
	Volume string `json:"volume"`
	Index  uint32 `json:"index"`
	Size   uint64 `json:"size"`
}

// attachmentStore persists the records under the plugin directory, which is
// host-backed in Kubernetes and therefore survives pod restarts.
type attachmentStore struct{ path string }

func newAttachmentStore(dir string) *attachmentStore {
	return &attachmentStore{path: filepath.Join(dir, "attachments.json")}
}

// load returns the recorded attachments; a missing file is an empty state.
func (s *attachmentStore) load() ([]attachmentRecord, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("csi: could not read the attachment state at %s (%w)", s.path, err)
	}
	var records []attachmentRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("csi: the attachment state at %s is corrupt (%w); attached volumes will not be supervised until they are re-published", s.path, err)
	}
	return records, nil
}

// save replaces the recorded attachments atomically (write, fsync, rename) so
// a crash mid-write can never leave a half-written state file.
func (s *attachmentStore) save(records []attachmentRecord) error {
	sort.Slice(records, func(i, j int) bool { return records[i].Volume < records[j].Volume })
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("csi: could not encode the attachment state (%w)", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".attachments-*")
	if err != nil {
		return fmt.Errorf("csi: could not write the attachment state next to %s (%w)", s.path, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("csi: could not write the attachment state (%w)", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("csi: could not sync the attachment state (%w)", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("csi: could not close the attachment state (%w)", err)
	}
	if err := os.Rename(tmp.Name(), s.path); err != nil {
		return fmt.Errorf("csi: could not replace the attachment state at %s (%w)", s.path, err)
	}
	return nil
}
