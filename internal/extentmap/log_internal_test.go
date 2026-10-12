package extentmap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

type entries = []crdt.MapEntry[uint64, string]

func entry(key uint64, chunk string, wall int64) crdt.MapEntry[uint64, string] {
	return crdt.MapEntry[uint64, string]{Key: key, Value: chunk, TS: hlc.Timestamp{Wall: wall, Node: "n"}}
}

// frame wraps a hand-built body in a valid header, so decode errors can be
// reached past the CRC check.
func frame(body []byte) []byte {
	out := binary.LittleEndian.AppendUint32(nil, uint32(len(body)))
	out = binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(body))
	return append(out, body...)
}

// collect replays b and returns every applied entry in order.
func collect(b []byte) (entries, int, bool, error) {
	var got entries
	n, torn, err := replayLog(b, func(e entries) { got = append(got, e...) })
	return got, n, torn, err
}

func TestRecord_RoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		batches []entries
	}{
		{"one entry", []entries{{entry(0, "c0", 1)}}},
		{"empty batch", []entries{{}}},
		{"many batches", []entries{{entry(1, "a", 1), entry(2, "b", 2)}, {entry(1, "c", 3)}}},
		{"extreme fields", []entries{{{
			Key:   ^uint64(0),
			Value: strings.Repeat("x", 65535),
			TS:    hlc.Timestamp{Wall: -1, Logical: ^uint32(0), Node: "node/with:odd@chars"},
		}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var log []byte
			var want entries
			for _, b := range tc.batches {
				size, ok := recordSize(b)
				before := len(log)
				log = appendRecord(log, b)
				if !ok || len(log)-before != size {
					t.Fatalf("recordSize = (%d,%v), encoded %d bytes", size, ok, len(log)-before)
				}
				want = append(want, b...)
			}
			got, n, torn, err := collect(log)
			if err != nil || torn || n != len(tc.batches) {
				t.Fatalf("replay = (%d records, torn=%v, %v), want (%d, false, nil)", n, torn, err, len(tc.batches))
			}
			if len(want) == 0 {
				want = nil
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("round trip = %v, want %v", got, want)
			}
		})
	}
}

func TestRecordSize_RejectsIDsTooLongForTheLengthField(t *testing.T) {
	long := strings.Repeat("x", 65536)
	for _, e := range []crdt.MapEntry[uint64, string]{
		{Key: 0, Value: long},
		{Key: 0, Value: "c", TS: hlc.Timestamp{Node: long}},
	} {
		if _, ok := recordSize(entries{e}); ok {
			t.Errorf("recordSize accepted a %d-byte id", len(long))
		}
	}
}

func TestReplayLog_TornTailsAndDamage(t *testing.T) {
	one := appendRecord(nil, entries{entry(0, "c0", 1)})
	two := appendRecord(bytes.Clone(one), entries{entry(1, "c1", 2)})
	flipped := func(b []byte, at int) []byte {
		c := bytes.Clone(b)
		c[at] ^= 0xff
		return c
	}
	for _, tc := range []struct {
		name      string
		log       []byte
		wantN     int
		wantTorn  bool
		wantErrIs error
	}{
		{"empty", nil, 0, false, nil},
		{"two whole records", two, 2, false, nil},
		{"header cut short", two[:len(one)+5], 1, true, nil},
		{"body cut short", two[:len(two)-1], 1, true, nil},
		{"zero tail after a record", append(bytes.Clone(one), make([]byte, 64)...), 1, true, nil},
		{"only zeroes", make([]byte, 16), 0, true, nil},
		{"checksum mismatch in the first record", flipped(two, recordHeaderLen+3), 0, false, errLogChecksum},
		{"checksum mismatch in the last whole record", flipped(two, len(two)-1), 1, false, errLogChecksum},
		{"malformed body behind a good checksum", append(bytes.Clone(one), frame([]byte{9})...), 1, false, errLogMalformed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, n, torn, err := collect(tc.log)
			if n != tc.wantN || torn != tc.wantTorn || !errors.Is(err, tc.wantErrIs) {
				t.Errorf("replay = (%d, torn=%v, %v), want (%d, torn=%v, %v)", n, torn, err, tc.wantN, tc.wantTorn, tc.wantErrIs)
			}
		})
	}
}

func TestDecodeBody_Errors(t *testing.T) {
	body := func(version byte, count uint32, rest ...byte) []byte {
		b := binary.LittleEndian.AppendUint32([]byte{version}, count)
		return append(b, rest...)
	}
	fixed := func(nodeLen uint16) []byte {
		b := make([]byte, 20)
		return binary.LittleEndian.AppendUint16(b, nodeLen)
	}
	str := func(s string) []byte {
		return append(binary.LittleEndian.AppendUint16(nil, uint16(len(s))), s...)
	}
	good := append(append(fixed(1), 'n'), str("c")...)
	for _, tc := range []struct {
		name, body, want string
	}{
		{"shorter than its header", string([]byte{1, 0}), "shorter"},
		{"newer version", string(body(2, 0)), "upgrade"},
		{"count larger than the body", string(body(1, 5)), "claims 5 entries"},
		{"node runs past the end", string(body(1, 1, append(fixed(9), make([]byte, 2)...)...)), "entry 0"},
		{"chunk length missing", string(body(1, 1, append(fixed(3), "abc"...)...)), "entry 0"},
		{"chunk runs past the end", string(body(1, 1, append(append(fixed(0), 0x05, 0x00), make([]byte, 2)...)...)), "entry 0"},
		{"second entry shorter than fixed", string(body(1, 2, append(append(fixed(20), make([]byte, 20)...), append(str(""), 0, 0, 0, 0)...)...)), "entry 1"},
		{"trailing bytes", string(body(1, 1, append(bytes.Clone(good), make([]byte, 30)...)...)), "follow the last entry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeBody([]byte(tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("decodeBody error = %v, want one mentioning %q", err, tc.want)
			}
		})
	}
	if got, err := decodeBody(body(1, 1, good...)); err != nil || len(got) != 1 || got[0].Value != "c" || got[0].TS.Node != "n" {
		t.Errorf("a well-formed body = (%v, %v)", got, err)
	}
}

func FuzzReplayLog(f *testing.F) {
	f.Add(appendRecord(nil, entries{entry(3, "c3", 7)}))
	f.Add(appendRecord(appendRecord(nil, entries{}), entries{entry(0, "", 0)}))
	f.Add([]byte{})
	f.Add(make([]byte, 9))
	f.Fuzz(func(t *testing.T, b []byte) {
		// Whatever replays cleanly must re-encode to the same bytes, or replay
		// accepted something the writer never produces.
		var again []byte
		if _, torn, err := replayLog(b, func(e entries) { again = appendRecord(again, e) }); err != nil || torn {
			return
		}
		if !bytes.Equal(again, b) {
			t.Errorf("clean replay of %x re-encodes to %x", b, again)
		}
	})
}

func TestOptions(t *testing.T) {
	for _, tc := range []struct {
		name        string
		opts        []Option
		wantBytes   int64
		wantRecords int
	}{
		{"defaults", nil, defaultCompactBytes, defaultCompactRecords},
		{"non-positive values keep the defaults", []Option{WithCompactBytes(0), WithCompactRecords(-1)}, defaultCompactBytes, defaultCompactRecords},
		{"explicit values", []Option{WithCompactBytes(100), WithCompactRecords(3)}, 100, 3},
		{"bytes are capped", []Option{WithCompactBytes(1 << 40)}, maxCompactBytes, defaultCompactRecords},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Open(t.TempDir(), quiet(), tc.opts...)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if s.compactBytes != tc.wantBytes || s.compactRecords != tc.wantRecords {
				t.Errorf("thresholds = (%d,%d), want (%d,%d)", s.compactBytes, s.compactRecords, tc.wantBytes, tc.wantRecords)
			}
		})
	}
}

// A record too big to keep around does not pin its buffer.
func TestAppend_ReleasesLargeBuffer(t *testing.T) {
	s, err := Open(t.TempDir(), quiet())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = s.Ensure("v")
	if err := s.Set("v", 0, "c", hlc.Timestamp{Wall: 1}); err != nil || s.recBuf == nil {
		t.Fatalf("a small append should keep its buffer: err=%v buf=%v", err, s.recBuf == nil)
	}
	big := strings.Repeat("x", maxRetainedRecordBuf/2)
	if err := s.SetBatch("v", []uint64{1, 2}, []string{big, big}, hlc.Timestamp{Wall: 2}); err != nil {
		t.Fatalf("SetBatch: %v", err)
	}
	if s.recBuf != nil {
		t.Errorf("a %d-byte buffer should be dropped after use", cap(s.recBuf))
	}
	if st := s.logs["v"]; st.records != 2 {
		t.Errorf("both writes should be log records, got %d", st.records)
	}
}

// fakeLog fails the steps it is told to and records the bytes it was given.
type fakeLog struct {
	failWrite, failSync, failClose bool
	wrote                          []byte
}

var errInjected = errors.New("injected")

func (f *fakeLog) Write(p []byte) (int, error) {
	if f.failWrite {
		return 0, errInjected
	}
	f.wrote = append(f.wrote, p...)
	return len(p), nil
}

func (f *fakeLog) Sync() error {
	if f.failSync {
		return errInjected
	}
	return nil
}

func (f *fakeLog) Close() error {
	if f.failClose {
		return errInjected
	}
	return nil
}

// fakeFS routes openLog by what the caller is doing: appending, truncating, or
// fsyncing a directory. A nil file for a step falls through to the real one.
type fakeFS struct {
	appendFile, truncFile, dirFile *fakeLog
	appendErr, truncErr, dirErr    error
}

func (fs *fakeFS) install(t *testing.T) {
	t.Helper()
	orig := openLog
	openLog = func(path string, flag int) (logFile, error) {
		var (
			f   *fakeLog
			err error
		)
		switch {
		case flag&os.O_APPEND != 0:
			f, err = fs.appendFile, fs.appendErr
		case flag&os.O_TRUNC != 0:
			f, err = fs.truncFile, fs.truncErr
		default:
			f, err = fs.dirFile, fs.dirErr
		}
		if err != nil {
			return nil, err
		}
		if f == nil {
			return orig(path, flag)
		}
		return f, nil
	}
	t.Cleanup(func() { openLog = orig })
}

// A failure at any step of an append is returned with what to check, leaves the
// change in memory, and makes the next save rewrite the snapshot, so a binding
// whose record never landed is not silently missing from disk.
func TestAppend_FailuresMarkTheLogStale(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fs       fakeFS
		firstLog bool // fail on the append that creates the log
	}{
		{"open", fakeFS{appendErr: errInjected}, false},
		{"write", fakeFS{appendFile: &fakeLog{failWrite: true}}, false},
		{"fsync", fakeFS{appendFile: &fakeLog{failSync: true}}, false},
		{"close", fakeFS{appendFile: &fakeLog{failClose: true}}, false},
		{"directory open", fakeFS{dirErr: errInjected}, true},
		{"directory fsync", fakeFS{dirFile: &fakeLog{failSync: true}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(dir, quiet())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := s.Ensure("v"); err != nil {
				t.Fatalf("Ensure: %v", err)
			}
			if !tc.firstLog {
				if err := s.Set("v", 9, "c9", hlc.Timestamp{Wall: 1}); err != nil {
					t.Fatalf("first append: %v", err)
				}
			}
			fs := tc.fs
			fs.install(t)
			err = s.SetBatch("v", []uint64{0}, []string{"c0"}, hlc.Timestamp{Wall: 2})
			if err == nil || !strings.Contains(err.Error(), "free space") || !errors.Is(err, errInjected) {
				t.Fatalf("SetBatch error = %v, want the injected failure with advice", err)
			}
			if !s.logs["v"].stale {
				t.Fatal("a failed append must mark the log stale")
			}
			openLog = func(path string, flag int) (logFile, error) { return os.OpenFile(path, flag, 0o600) }
			if err := s.Set("v", 1, "c1", hlc.Timestamp{Wall: 3}); err != nil {
				t.Fatalf("the save after a failure: %v", err)
			}
			if st := s.logs["v"]; st.stale || st.bytes != 0 {
				t.Errorf("the save after a failure should compact: %+v", *st)
			}
			re, err := Open(dir, quiet())
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if id, ok := re.Get("v", 0); !ok || id != "c0" {
				t.Errorf("the binding whose append failed should be on disk after the next save: (%q,%v)", id, ok)
			}
		})
	}
}

// When the snapshot is written but the log cannot be emptied, the save still
// succeeds (the snapshot holds everything) and the next save tries again.
func TestCompact_TruncateFailureIsRetried(t *testing.T) {
	for _, tc := range []struct {
		name string
		fs   fakeFS
	}{
		{"open", fakeFS{truncErr: errInjected}},
		{"fsync", fakeFS{truncFile: &fakeLog{failSync: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Open(t.TempDir(), quiet(), WithCompactRecords(1))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			_ = s.Ensure("v")
			_ = s.Set("v", 0, "c0", hlc.Timestamp{Wall: 1})
			fs := tc.fs
			fs.install(t)
			if err := s.Set("v", 1, "c1", hlc.Timestamp{Wall: 2}); err != nil {
				t.Fatalf("a save whose snapshot landed should succeed: %v", err)
			}
			if !s.logs["v"].stale {
				t.Error("an unemptied log must be retried on the next save")
			}
		})
	}
}

// A snapshot that cannot be written during compaction keeps the volume stale.
func TestCompact_SnapshotFailureKeepsStale(t *testing.T) {
	s, err := Open(t.TempDir(), quiet(), WithCompactRecords(1))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = s.Ensure("v")
	_ = s.Set("v", 0, "c0", hlc.Timestamp{Wall: 1})
	orig := marshal
	marshal = func(any) ([]byte, error) { return nil, errInjected }
	defer func() { marshal = orig }()
	if err := s.Set("v", 1, "c1", hlc.Timestamp{Wall: 2}); !errors.Is(err, errInjected) {
		t.Fatalf("Set error = %v, want the marshal failure", err)
	}
	if !s.logs["v"].stale {
		t.Error("a failed compaction must leave the volume stale")
	}
}

// An empty Merge into a volume whose files are current costs no I/O.
func TestMerge_EmptyIsFree(t *testing.T) {
	s, err := Open(t.TempDir(), quiet())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = s.Ensure("v")
	(&fakeFS{appendErr: errInjected, truncErr: errInjected}).install(t)
	if err := s.Merge("v", nil); err != nil {
		t.Errorf("an empty merge should not touch disk: %v", err)
	}
}

// A log whose snapshot could not be read leaves the volume lost and the log in
// place for the next boot.
func TestReplay_LogOfLostVolumeStaysPut(t *testing.T) {
	dir := t.TempDir()
	probe := &Store{dir: dir}
	if err := os.WriteFile(probe.filename("v"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(probe.logPath("v"), appendRecord(nil, entries{entry(0, "c0", 1)}), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir, quiet())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !s.Lost("v") || s.Has("v") {
		t.Errorf("Lost=%v Has=%v, want a lost volume with no map", s.Lost("v"), s.Has("v"))
	}
	if _, err := os.Stat(probe.logPath("v")); err != nil {
		t.Errorf("the log of a lost volume should stay where it is: %v", err)
	}
}
