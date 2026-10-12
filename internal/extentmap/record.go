package extentmap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

// The log is a sequence of records, one per SetBatch, Set or Merge, each laid
// out little-endian as
//
//	u32 body length | u32 CRC-32 (IEEE) of the body | body
//	body  = u8 version | u32 entry count | entry...
//	entry = u64 index | i64 wall | u32 logical | u16 len | node | u16 len | chunk id
//
// One record per call keeps a batch all-or-nothing on replay: a crash mid-append
// tears the last record as a whole, never half of a batch. The node id rides
// along with wall and logical because it is part of the HLC: it breaks LWW ties
// and feeds Digest, so a replay without it would converge differently from the
// peers and look diverged to the scrubber.
const (
	recordVersion   byte = 1
	recordHeaderLen      = 8
	recordBodyFixed      = 5
	entryFixed           = 8 + 8 + 4 + 2 + 2
)

var (
	// errLogChecksum marks a record whose bytes are all present but do not
	// match their CRC: damage, not a crash.
	errLogChecksum = errors.New("a record does not match its checksum")
	// errLogMalformed marks a record that passed its CRC but does not parse,
	// which a crash cannot produce either.
	errLogMalformed = errors.New("a record is malformed")
)

// recordSize returns the encoded size of a record holding entries, header
// included, and false when an id is too long for the u16 length field. Such a
// batch is saved by compaction instead, whose JSON has no length limit.
func recordSize(entries []crdt.MapEntry[uint64, string]) (int, bool) {
	n := recordHeaderLen + recordBodyFixed
	for _, e := range entries {
		if len(e.TS.Node) > math.MaxUint16 || len(e.Value) > math.MaxUint16 {
			return 0, false
		}
		n += entryFixed + len(e.TS.Node) + len(e.Value)
	}
	return n, true
}

// appendRecord appends the record for entries to dst. The caller has checked
// recordSize, and the store never appends a record larger than maxCompactBytes,
// so every length below fits its field.
func appendRecord(dst []byte, entries []crdt.MapEntry[uint64, string]) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, recordHeaderLen)...)
	dst = append(dst, recordVersion)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(entries))) // #nosec G115 -- bounded by maxCompactBytes
	for _, e := range entries {
		dst = binary.LittleEndian.AppendUint64(dst, e.Key)
		dst = binary.LittleEndian.AppendUint64(dst, uint64(e.TS.Wall)) // #nosec G115 -- bit pattern round-trips through int64
		dst = binary.LittleEndian.AppendUint32(dst, e.TS.Logical)
		dst = appendString(dst, e.TS.Node)
		dst = appendString(dst, e.Value)
	}
	body := dst[start+recordHeaderLen:]
	binary.LittleEndian.PutUint32(dst[start:], uint32(len(body))) // #nosec G115 -- bounded by maxCompactBytes
	binary.LittleEndian.PutUint32(dst[start+4:], crc32.ChecksumIEEE(body))
	return dst
}

func appendString(dst []byte, s string) []byte {
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(s))) // #nosec G115 -- recordSize rejected anything longer
	return append(dst, s...)
}

// replayLog feeds every intact record in b to apply, in order. torn reports that
// the log ends in a record a crash cut short (an incomplete record, or a tail of
// zeroes, which is what XFS shows for an extended file whose data never landed).
// Such a record was never acknowledged, so stopping before it loses nothing. A
// complete record that fails its CRC or does not parse is damage, and comes back
// as an error.
//
// A length field damaged so that it points past the end of the file reads as a
// torn tail and drops the records after it. That needs a bit flip the drive's
// own ECC missed; the bindings it drops were acknowledged by a majority of the
// volume's replicas, and the extent scrubber's digest comparison pulls them back.
func replayLog(b []byte, apply func([]crdt.MapEntry[uint64, string])) (records int, torn bool, err error) {
	for off := 0; off < len(b); {
		rest := b[off:]
		if allZero(rest) || len(rest) < recordHeaderLen {
			return records, true, nil
		}
		n := binary.LittleEndian.Uint32(rest)
		if int64(n) > int64(len(rest)-recordHeaderLen) {
			return records, true, nil
		}
		body := rest[recordHeaderLen : recordHeaderLen+int(n)]
		if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(rest[4:]) {
			return records, false, fmt.Errorf("%w at byte %d", errLogChecksum, off)
		}
		entries, err := decodeBody(body)
		if err != nil {
			return records, false, fmt.Errorf("%w at byte %d (%w)", errLogMalformed, off, err)
		}
		apply(entries)
		records++
		off += recordHeaderLen + int(n)
	}
	return records, false, nil
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// decodeBody parses one record body that has already passed its CRC.
func decodeBody(body []byte) ([]crdt.MapEntry[uint64, string], error) {
	if len(body) < recordBodyFixed {
		return nil, errors.New("the body is shorter than its fixed header")
	}
	if body[0] != recordVersion {
		return nil, fmt.Errorf("record version %d is not one this silod reads; it was written by a newer release, so upgrade this node", body[0])
	}
	count := binary.LittleEndian.Uint32(body[1:])
	r := body[recordBodyFixed:]
	if int64(count) > int64(len(r)/entryFixed) {
		return nil, fmt.Errorf("the body claims %d entries but holds %d bytes", count, len(r))
	}
	out := make([]crdt.MapEntry[uint64, string], count)
	for i := range out {
		e, rest, ok := decodeEntry(r)
		if !ok {
			return nil, fmt.Errorf("entry %d runs past the end of the body", i)
		}
		out[i], r = e, rest
	}
	if len(r) != 0 {
		return nil, fmt.Errorf("%d bytes follow the last entry", len(r))
	}
	return out, nil
}

func decodeEntry(r []byte) (crdt.MapEntry[uint64, string], []byte, bool) {
	var e crdt.MapEntry[uint64, string]
	if len(r) < entryFixed-2 {
		return e, nil, false
	}
	e.Key = binary.LittleEndian.Uint64(r)
	e.TS = hlc.Timestamp{
		Wall:    int64(binary.LittleEndian.Uint64(r[8:])), // #nosec G115 -- bit pattern round-trips through uint64
		Logical: binary.LittleEndian.Uint32(r[16:]),
	}
	var ok bool
	if e.TS.Node, r, ok = readString(r[20:]); !ok {
		return e, nil, false
	}
	if e.Value, r, ok = readString(r); !ok {
		return e, nil, false
	}
	return e, r, true
}

func readString(r []byte) (string, []byte, bool) {
	if len(r) < 2 {
		return "", nil, false
	}
	n := int(binary.LittleEndian.Uint16(r))
	r = r[2:]
	if len(r) < n {
		return "", nil, false
	}
	return string(r[:n]), r[n:], true
}
