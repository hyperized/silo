// Package nbdnl drives the Linux kernel's NBD driver over generic netlink —
// the kernel-facing half of attaching a silo volume as /dev/nbdX. It connects
// a handshaken TCP socket to a device, splices a fresh socket into a live
// device after silod restarts (reconfigure), disconnects, and watches the
// kernel's dead-link notifications so a supervisor knows when to reconnect.
// The wire codec is implemented from the uapi headers over the standard
// library; only the socket layer (conn_linux.go) touches the OS.
package nbdnl

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Generic-netlink protocol constants (include/uapi/linux/{netlink,genetlink}.h).
// Netlink is a native-endian protocol, so all integers below are marshalled
// with binary.NativeEndian, not big-endian.
const (
	nlmsgError uint16 = 0x2
	nlmsgDone  uint16 = 0x3

	nlmFRequest uint16 = 0x1
	nlmFAck     uint16 = 0x4

	// genlIDCtrl is the fixed family id of the generic-netlink controller,
	// used to resolve the "nbd" family's dynamic id and multicast groups.
	genlIDCtrl uint16 = 0x10

	ctrlCmdGetFamily uint8 = 3

	ctrlAttrFamilyID    uint16 = 1
	ctrlAttrFamilyName  uint16 = 2
	ctrlAttrMcastGroups uint16 = 7

	ctrlAttrMcastGrpName uint16 = 1
	ctrlAttrMcastGrpID   uint16 = 2

	nlaFNested  uint16 = 0x8000
	nlaTypeMask uint16 = 0x3fff

	nlmsgHdrLen = 16
	genlHdrLen  = 4
	nlaHdrLen   = 4
)

// NBD generic-netlink family constants (include/uapi/linux/nbd-netlink.h).
const (
	nbdFamilyName  = "nbd"
	nbdMcastGroup  = "nbd_mc_group"
	nbdGenlVersion = 1

	nbdCmdConnect     uint8 = 1
	nbdCmdDisconnect  uint8 = 2
	nbdCmdReconfigure uint8 = 3
	nbdCmdLinkDead    uint8 = 4
	nbdCmdStatus      uint8 = 5

	nbdAttrIndex           uint16 = 1
	nbdAttrSizeBytes       uint16 = 2
	nbdAttrBlockSizeBytes  uint16 = 3
	nbdAttrTimeout         uint16 = 4
	nbdAttrServerFlags     uint16 = 5
	nbdAttrSockets         uint16 = 7
	nbdAttrDeadConnTimeout uint16 = 8
	nbdAttrDeviceList      uint16 = 9

	nbdSockItem uint16 = 1
	nbdSockFD   uint16 = 1

	nbdDeviceItem      uint16 = 1
	nbdDeviceIndex     uint16 = 1
	nbdDeviceConnected uint16 = 2
)

// attr is one netlink attribute: either a leaf carrying data or a nested
// container carrying children (marshalled with NLA_F_NESTED set).
type attr struct {
	typ  uint16
	data []byte
	kids []attr
}

func u32Attr(typ uint16, v uint32) attr {
	return attr{typ: typ, data: binary.NativeEndian.AppendUint32(nil, v)}
}

func u64Attr(typ uint16, v uint64) attr {
	return attr{typ: typ, data: binary.NativeEndian.AppendUint64(nil, v)}
}

// stringAttr marshals a NUL-terminated netlink string (NLA_NUL_STRING).
func stringAttr(typ uint16, s string) attr {
	return attr{typ: typ, data: append([]byte(s), 0)}
}

func nest(typ uint16, kids ...attr) attr {
	return attr{typ: typ, kids: kids}
}

// nlaAlign pads netlink lengths to the 4-byte attribute alignment.
func nlaAlign(n int) int { return (n + 3) &^ 3 }

func marshalAttr(dst []byte, a attr) []byte {
	var payload []byte
	typ := a.typ
	if a.kids != nil {
		typ |= nlaFNested
		payload = marshalAttrs(nil, a.kids)
	} else {
		payload = a.data
	}
	length := nlaHdrLen + len(payload)
	dst = binary.NativeEndian.AppendUint16(dst, uint16(length)) // #nosec G115 -- attribute payloads are small and bounded
	dst = binary.NativeEndian.AppendUint16(dst, typ)
	dst = append(dst, payload...)
	dst = append(dst, make([]byte, nlaAlign(len(payload))-len(payload))...)
	return dst
}

func marshalAttrs(dst []byte, attrs []attr) []byte {
	for _, a := range attrs {
		dst = marshalAttr(dst, a)
	}
	return dst
}

// marshalMessage frames one generic-netlink request: nlmsghdr + genlmsghdr +
// attributes, with the total length back-filled once the payload is known.
func marshalMessage(family uint16, flags uint16, seq, pid uint32, cmd, version uint8, attrs []attr) []byte {
	buf := make([]byte, nlmsgHdrLen, 256)
	buf = append(buf, cmd, version, 0, 0)
	buf = marshalAttrs(buf, attrs)
	binary.NativeEndian.PutUint32(buf[0:4], uint32(len(buf))) // #nosec G115 -- request messages are small and bounded
	binary.NativeEndian.PutUint16(buf[4:6], family)
	binary.NativeEndian.PutUint16(buf[6:8], flags)
	binary.NativeEndian.PutUint32(buf[8:12], seq)
	binary.NativeEndian.PutUint32(buf[12:16], pid)
	return buf
}

// rawAttr is one parsed attribute; Data aliases the receive buffer.
type rawAttr struct {
	Typ  uint16
	Data []byte
}

func (a rawAttr) U16() (uint16, error) {
	if len(a.Data) < 2 {
		return 0, fmt.Errorf("nbdnl: attribute %d is %d bytes, want at least 2", a.Typ, len(a.Data))
	}
	return binary.NativeEndian.Uint16(a.Data), nil
}

func (a rawAttr) U32() (uint32, error) {
	if len(a.Data) < 4 {
		return 0, fmt.Errorf("nbdnl: attribute %d is %d bytes, want at least 4", a.Typ, len(a.Data))
	}
	return binary.NativeEndian.Uint32(a.Data), nil
}

func (a rawAttr) U8() (uint8, error) {
	if len(a.Data) < 1 {
		return 0, fmt.Errorf("nbdnl: attribute %d is empty, want at least 1 byte", a.Typ)
	}
	return a.Data[0], nil
}

// String trims the NUL terminator netlink strings carry.
func (a rawAttr) String() string {
	d := a.Data
	for i, b := range d {
		if b == 0 {
			return string(d[:i])
		}
	}
	return string(d)
}

func (a rawAttr) Nested() ([]rawAttr, error) { return parseAttrs(a.Data) }

func parseAttrs(buf []byte) ([]rawAttr, error) {
	var out []rawAttr
	for len(buf) >= nlaHdrLen {
		length := int(binary.NativeEndian.Uint16(buf[0:2]))
		typ := binary.NativeEndian.Uint16(buf[2:4])
		if length < nlaHdrLen || length > len(buf) {
			return nil, fmt.Errorf("nbdnl: attribute length %d does not fit the %d remaining bytes", length, len(buf))
		}
		out = append(out, rawAttr{Typ: typ & nlaTypeMask, Data: buf[nlaHdrLen:length]})
		// The last attribute's length need not be padded out to the 4-byte
		// alignment, so cap the stride at what remains.
		buf = buf[min(nlaAlign(length), len(buf)):]
	}
	if len(buf) != 0 {
		return nil, fmt.Errorf("nbdnl: %d trailing bytes after the last attribute", len(buf))
	}
	return out, nil
}

func attrByType(attrs []rawAttr, typ uint16) (rawAttr, bool) {
	for _, a := range attrs {
		if a.Typ == typ {
			return a, true
		}
	}
	return rawAttr{}, false
}

// message is one parsed netlink message. For NLMSG_ERROR frames Err carries
// the kernel's errno (nil for an ACK) and the genl fields are zero.
type message struct {
	Type  uint16
	Flags uint16
	Seq   uint32

	Cmd     uint8
	Version uint8
	Attrs   []rawAttr

	Err   error
	IsAck bool
}

// errnoToError is swapped in by the linux socket layer to turn a kernel errno
// into a syscall error; the pure codec has no syscall dependency so tests run
// on any OS.
var errnoToError = func(code int32) error {
	return fmt.Errorf("nbdnl: kernel returned errno %d", -code)
}

// parseMessages splits one received datagram into its netlink messages.
func parseMessages(buf []byte) ([]message, error) {
	var out []message
	for len(buf) >= nlmsgHdrLen {
		length := int(binary.NativeEndian.Uint32(buf[0:4]))
		if length < nlmsgHdrLen || length > len(buf) {
			return nil, fmt.Errorf("nbdnl: message length %d does not fit the %d remaining bytes", length, len(buf))
		}
		m := message{
			Type:  binary.NativeEndian.Uint16(buf[4:6]),
			Flags: binary.NativeEndian.Uint16(buf[6:8]),
			Seq:   binary.NativeEndian.Uint32(buf[8:12]),
		}
		payload := buf[nlmsgHdrLen:length]
		switch m.Type {
		case nlmsgError:
			if len(payload) < 4 {
				return nil, errors.New("nbdnl: error message too short for an error code")
			}
			code := int32(binary.NativeEndian.Uint32(payload[0:4])) // #nosec G115 -- errno is a signed 32-bit value by ABI
			if code == 0 {
				m.IsAck = true
			} else {
				m.Err = errnoToError(code)
			}
		case nlmsgDone:
			// End of a multipart reply; no payload we care about.
		default:
			if len(payload) < genlHdrLen {
				return nil, errors.New("nbdnl: message too short for a generic-netlink header")
			}
			m.Cmd = payload[0]
			m.Version = payload[1]
			attrs, err := parseAttrs(payload[genlHdrLen:])
			if err != nil {
				return nil, err
			}
			m.Attrs = attrs
		}
		out = append(out, m)
		// The last message's length need not be padded out to the 4-byte
		// alignment, so cap the stride at what remains.
		buf = buf[min(nlaAlign(length), len(buf)):]
	}
	if len(buf) != 0 {
		return nil, fmt.Errorf("nbdnl: %d trailing bytes after the last message", len(buf))
	}
	return out, nil
}
