package atomicfile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var errInjected = errors.New("injected")

// fakeFile records which steps ran and fails the ones it is told to.
type fakeFile struct {
	failWrite, failSync, failClose bool

	wrote  []byte
	synced bool
	closed bool
}

func (f *fakeFile) Write(p []byte) (int, error) {
	if f.failWrite {
		return 0, errInjected
	}
	f.wrote = append(f.wrote, p...)
	return len(p), nil
}

func (f *fakeFile) Sync() error {
	if f.failSync {
		return errInjected
	}
	f.synced = true
	return nil
}

func (f *fakeFile) Close() error {
	f.closed = true
	if f.failClose {
		return errInjected
	}
	return nil
}

// seams captures what Write did through the swapped file-system hooks.
type seams struct {
	file, dir      *fakeFile
	openFileErr    error
	openDirErr     error
	renameErr      error
	removed        []string
	renamedFrom    string
	renamedTo      string
	openedFilePath string
	openedDirPath  string
}

func install(t *testing.T, s *seams) {
	t.Helper()
	origFile, origDir, origRename, origRemove := openFile, openDir, osRename, osRemove
	t.Cleanup(func() { openFile, openDir, osRename, osRemove = origFile, origDir, origRename, origRemove })
	openFile = func(path string, _ os.FileMode) (syncCloser, error) {
		s.openedFilePath = path
		if s.openFileErr != nil {
			return nil, s.openFileErr
		}
		return s.file, nil
	}
	openDir = func(dir string) (syncCloser, error) {
		s.openedDirPath = dir
		if s.openDirErr != nil {
			return nil, s.openDirErr
		}
		return s.dir, nil
	}
	osRename = func(from, to string) error {
		s.renamedFrom, s.renamedTo = from, to
		return s.renameErr
	}
	osRemove = func(path string) error {
		s.removed = append(s.removed, path)
		return nil
	}
}

func TestWrite_SyncsFileThenDirectory(t *testing.T) {
	s := &seams{file: &fakeFile{}, dir: &fakeFile{}}
	install(t, s)

	path := filepath.Join("some", "dir", "vol.emap.json")
	if err := Write(path, []byte("payload"), 0o600); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if s.openedFilePath != path+TempSuffix {
		t.Errorf("temp file = %q, want %q", s.openedFilePath, path+TempSuffix)
	}
	if string(s.file.wrote) != "payload" || !s.file.synced || !s.file.closed {
		t.Errorf("temp file not written, synced and closed: %+v", s.file)
	}
	if s.renamedFrom != path+TempSuffix || s.renamedTo != path {
		t.Errorf("rename %q -> %q, want temp -> target", s.renamedFrom, s.renamedTo)
	}
	if s.openedDirPath != filepath.Dir(path) || !s.dir.synced || !s.dir.closed {
		t.Errorf("parent directory not synced and closed: opened %q, %+v", s.openedDirPath, s.dir)
	}
	if len(s.removed) != 0 {
		t.Errorf("a successful write removed %v", s.removed)
	}
}

func TestWrite_ErrorBranches(t *testing.T) {
	for _, tc := range []struct {
		name        string
		s           *seams
		wantMsg     string
		wantRemoved bool
	}{
		{"open temp fails", &seams{openFileErr: errInjected}, "could not create", false},
		{"write fails", &seams{file: &fakeFile{failWrite: true}}, "could not write", true},
		{"file sync fails", &seams{file: &fakeFile{failSync: true}}, "could not flush", true},
		{"close fails", &seams{file: &fakeFile{failClose: true}}, "could not close", true},
		{"rename fails", &seams{file: &fakeFile{}, renameErr: errInjected}, "could not move", true},
		{"open dir fails", &seams{file: &fakeFile{}, openDirErr: errInjected}, "could not open directory", false},
		{"dir sync fails", &seams{file: &fakeFile{}, dir: &fakeFile{failSync: true}}, "could not flush directory", false},
		{"dir close fails", &seams{file: &fakeFile{}, dir: &fakeFile{failClose: true}}, "could not close directory", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			install(t, tc.s)
			err := Write("target", []byte("x"), 0o600)
			if !errors.Is(err, errInjected) {
				t.Fatalf("err = %v, want it to wrap the injected failure", err)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("err = %q, want it to mention %q", err, tc.wantMsg)
			}
			if removed := len(tc.s.removed) == 1 && tc.s.removed[0] == "target"+TempSuffix; removed != tc.wantRemoved {
				t.Errorf("temp file removed = %v (%v), want %v", removed, tc.s.removed, tc.wantRemoved)
			}
			if tc.s.file != nil && !tc.s.file.closed {
				t.Error("the temp file handle leaked")
			}
			if tc.s.dir != nil && !tc.s.dir.closed {
				t.Error("the directory handle leaked")
			}
		})
	}
}
