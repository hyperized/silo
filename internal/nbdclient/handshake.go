// Package nbdclient is the client half of silo's NBD stack: it negotiates an
// export with silod (the handshake internal/nbd serves), hands the connected
// socket to the kernel through internal/nbdnl, and supervises the attachment —
// when the connection dies (a silod restart), it dials silod again and splices
// the fresh socket into the live /dev/nbdX so queued I/O resumes instead of
// failing. This is what makes a silod rollout invisible to a mounted volume.
package nbdclient

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// Protocol magics and flags (see the NBD protocol spec; the server half lives
// in internal/nbd and these must stay in lockstep with it).
const (
	magicNBD   uint64 = 0x4e42444d41474943 // "NBDMAGIC"
	magicIHAVE uint64 = 0x49484156454f5054 // "IHAVEOPT"
	magicRep   uint64 = 0x0003e889045565a9 // option reply

	flagFixedNewstyle uint16 = 1 << 0
	flagNoZeroes      uint16 = 1 << 1

	cFlagFixedNewstyle uint32 = 1 << 0
	cFlagNoZeroes      uint32 = 1 << 1

	optAbort uint32 = 2
	optGo    uint32 = 7

	repAck  uint32 = 1
	repInfo uint32 = 3
	repErr  uint32 = 1 << 31

	infoExport uint16 = 0

	// maxReplyBytes caps an option reply's payload so a confused peer cannot
	// make the client allocate unbounded memory mid-handshake.
	maxReplyBytes uint32 = 1 << 20
)

// Export is what the handshake negotiates: the volume's size and the
// transmission flags the kernel needs to configure the block queue.
type Export struct {
	Size              uint64
	TransmissionFlags uint16
}

// Negotiate runs the client side of the fixed-newstyle handshake on conn and
// requests export by name. On success the connection has entered the
// transmission phase and is ready to hand to the kernel. A ctx deadline is
// applied to the whole exchange.
func Negotiate(ctx context.Context, conn net.Conn, export string) (Export, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return Export{}, fmt.Errorf("nbdclient: could not bound the handshake (%w)", err)
		}
		defer func() { _ = conn.SetDeadline(time.Time{}) }()
	}

	serverFlags, err := readGreeting(conn)
	if err != nil {
		return Export{}, err
	}
	if serverFlags&flagFixedNewstyle == 0 {
		return Export{}, errors.New("nbdclient: the server does not speak the fixed-newstyle NBD handshake; is this really silod's NBD port?")
	}
	clientFlags := cFlagFixedNewstyle
	if serverFlags&flagNoZeroes != 0 {
		clientFlags |= cFlagNoZeroes
	}
	if err := binary.Write(conn, binary.BigEndian, clientFlags); err != nil {
		return Export{}, fmt.Errorf("nbdclient: sending client flags failed (%w)", err)
	}
	if err := writeGo(conn, export); err != nil {
		return Export{}, err
	}
	return readGoReplies(conn, export)
}

func readGreeting(conn net.Conn) (uint16, error) {
	greeting := make([]byte, 18)
	if _, err := io.ReadFull(conn, greeting); err != nil {
		return 0, fmt.Errorf("nbdclient: reading the server greeting failed (%w)", err)
	}
	if binary.BigEndian.Uint64(greeting[0:8]) != magicNBD || binary.BigEndian.Uint64(greeting[8:16]) != magicIHAVE {
		return 0, errors.New("nbdclient: the server greeting is not NBD; is this really silod's NBD port?")
	}
	return binary.BigEndian.Uint16(greeting[16:18]), nil
}

// writeGo sends NBD_OPT_GO: the export name plus zero information requests.
func writeGo(conn net.Conn, export string) error {
	payload := binary.BigEndian.AppendUint32(nil, uint32(len(export))) // #nosec G115 -- export names are short volume paths
	payload = append(payload, export...)
	payload = binary.BigEndian.AppendUint16(payload, 0)

	msg := binary.BigEndian.AppendUint64(nil, magicIHAVE)
	msg = binary.BigEndian.AppendUint32(msg, optGo)
	msg = binary.BigEndian.AppendUint32(msg, uint32(len(payload))) // #nosec G115 -- bounded by the export name length
	msg = append(msg, payload...)
	if _, err := conn.Write(msg); err != nil {
		return fmt.Errorf("nbdclient: requesting export %q failed (%w)", export, err)
	}
	return nil
}

// readGoReplies consumes option replies until the final ACK, extracting the
// export's size and flags from the NBD_INFO_EXPORT info reply.
func readGoReplies(conn net.Conn, export string) (Export, error) {
	var (
		result Export
		seen   bool
	)
	for {
		opt, repType, payload, err := readReply(conn)
		if err != nil {
			return Export{}, err
		}
		if opt != optGo {
			return Export{}, fmt.Errorf("nbdclient: the server answered option %d while %d (GO) was pending", opt, optGo)
		}
		switch {
		case repType&repErr != 0:
			detail := ""
			if len(payload) > 0 {
				detail = ": " + string(payload)
			}
			return Export{}, fmt.Errorf("nbdclient: silod refused to serve export %q (reply %#x%s); check that the volume exists and has a size — a just-created volume can take a moment to reach this node, and the attach is retried automatically", export, repType, detail)
		case repType == repInfo:
			if len(payload) >= 12 && binary.BigEndian.Uint16(payload[0:2]) == infoExport {
				result.Size = binary.BigEndian.Uint64(payload[2:10])
				result.TransmissionFlags = binary.BigEndian.Uint16(payload[10:12])
				seen = true
			}
			// Other info kinds are advisory; ignore them.
		case repType == repAck:
			if !seen {
				return Export{}, fmt.Errorf("nbdclient: the server acknowledged export %q without describing it", export)
			}
			return result, nil
		default:
			return Export{}, fmt.Errorf("nbdclient: unexpected option reply %#x during the handshake", repType)
		}
	}
}

func readReply(conn net.Conn) (opt, repType uint32, payload []byte, err error) {
	header := make([]byte, 20)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, 0, nil, fmt.Errorf("nbdclient: reading an option reply failed (%w)", err)
	}
	if binary.BigEndian.Uint64(header[0:8]) != magicRep {
		return 0, 0, nil, errors.New("nbdclient: the server sent a malformed option reply")
	}
	opt = binary.BigEndian.Uint32(header[8:12])
	repType = binary.BigEndian.Uint32(header[12:16])
	length := binary.BigEndian.Uint32(header[16:20])
	if length > maxReplyBytes {
		return 0, 0, nil, fmt.Errorf("nbdclient: option reply of %d bytes exceeds the cap", length)
	}
	payload = make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, 0, nil, fmt.Errorf("nbdclient: reading an option reply payload failed (%w)", err)
	}
	return opt, repType, payload, nil
}
