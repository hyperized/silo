package nbdnl

import (
	"errors"
	"fmt"
	"time"
)

// ConnectConfig describes one device attachment for Connect.
type ConnectConfig struct {
	// SocketFD is a connected, handshaken NBD socket. The kernel takes its own
	// reference; the caller may close its copy after Connect returns.
	SocketFD int
	// SizeBytes is the export size negotiated during the handshake.
	SizeBytes uint64
	// BlockSizeBytes is the device's logical block size (0 = kernel default).
	BlockSizeBytes uint64
	// ServerFlags are the transmission flags from the handshake; the kernel
	// uses them to enable flush/trim support on the block queue.
	ServerFlags uint16
	// IOTimeout bounds a single request (0 = kernel default).
	IOTimeout time.Duration
	// DeadConnTimeout is how long the kernel queues I/O while the connection
	// is down, waiting for a Reconfigure with a fresh socket, before failing
	// the queued requests. This window is what makes a silod restart survivable.
	DeadConnTimeout time.Duration
}

// seconds converts a duration to the whole seconds the kernel expects,
// rounding a small non-zero duration up so it never silently becomes "unset".
func seconds(d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	s := (d + time.Second - 1) / time.Second
	return uint64(s)
}

// getFamilyRequest asks the genetlink controller for a family's id and
// multicast groups by name.
func getFamilyRequest(seq, pid uint32, family string) []byte {
	return marshalMessage(genlIDCtrl, nlmFRequest|nlmFAck, seq, pid, ctrlCmdGetFamily, 1,
		[]attr{stringAttr(ctrlAttrFamilyName, family)})
}

// familyInfo is the controller's answer: the family's dynamic id and its
// multicast groups by name.
type familyInfo struct {
	id     uint16
	groups map[string]uint32
}

func parseFamilyReply(m message) (familyInfo, error) {
	info := familyInfo{groups: map[string]uint32{}}
	idAttr, ok := attrByType(m.Attrs, ctrlAttrFamilyID)
	if !ok {
		return info, errors.New("nbdnl: family reply carries no family id")
	}
	id, err := idAttr.U16()
	if err != nil {
		return info, err
	}
	info.id = id
	if groups, ok := attrByType(m.Attrs, ctrlAttrMcastGroups); ok {
		items, err := groups.Nested()
		if err != nil {
			return info, err
		}
		for _, item := range items {
			fields, err := item.Nested()
			if err != nil {
				return info, err
			}
			nameAttr, okName := attrByType(fields, ctrlAttrMcastGrpName)
			idAttr, okID := attrByType(fields, ctrlAttrMcastGrpID)
			if !okName || !okID {
				continue
			}
			gid, err := idAttr.U32()
			if err != nil {
				return info, err
			}
			info.groups[nameAttr.String()] = gid
		}
	}
	return info, nil
}

// socketsAttr wraps a socket fd in the nested NBD_ATTR_SOCKETS list the
// connect and reconfigure commands expect.
func socketsAttr(fd int) attr {
	return nest(nbdAttrSockets,
		nest(nbdSockItem, u32Attr(nbdSockFD, uint32(fd)))) // #nosec G115 -- fds are small non-negative integers
}

func connectRequest(family uint16, seq, pid uint32, cfg ConnectConfig) []byte {
	// No NBD_ATTR_INDEX: the kernel picks (or creates) a free device and
	// replies with its index, which removes any free-device scanning race.
	attrs := []attr{
		u64Attr(nbdAttrSizeBytes, cfg.SizeBytes),
		u64Attr(nbdAttrServerFlags, uint64(cfg.ServerFlags)),
		socketsAttr(cfg.SocketFD),
	}
	if cfg.BlockSizeBytes != 0 {
		attrs = append(attrs, u64Attr(nbdAttrBlockSizeBytes, cfg.BlockSizeBytes))
	}
	if cfg.IOTimeout != 0 {
		attrs = append(attrs, u64Attr(nbdAttrTimeout, seconds(cfg.IOTimeout)))
	}
	if cfg.DeadConnTimeout != 0 {
		attrs = append(attrs, u64Attr(nbdAttrDeadConnTimeout, seconds(cfg.DeadConnTimeout)))
	}
	return marshalMessage(family, nlmFRequest|nlmFAck, seq, pid, nbdCmdConnect, nbdGenlVersion, attrs)
}

func parseConnectReply(m message) (uint32, error) {
	idx, ok := attrByType(m.Attrs, nbdAttrIndex)
	if !ok {
		return 0, errors.New("nbdnl: connect reply carries no device index")
	}
	return idx.U32()
}

func reconfigureRequest(family uint16, seq, pid uint32, index uint32, fd int, ioTimeout, deadConnTimeout time.Duration) []byte {
	attrs := []attr{
		u32Attr(nbdAttrIndex, index),
		socketsAttr(fd),
	}
	if ioTimeout != 0 {
		attrs = append(attrs, u64Attr(nbdAttrTimeout, seconds(ioTimeout)))
	}
	if deadConnTimeout != 0 {
		attrs = append(attrs, u64Attr(nbdAttrDeadConnTimeout, seconds(deadConnTimeout)))
	}
	return marshalMessage(family, nlmFRequest|nlmFAck, seq, pid, nbdCmdReconfigure, nbdGenlVersion, attrs)
}

func disconnectRequest(family uint16, seq, pid uint32, index uint32) []byte {
	return marshalMessage(family, nlmFRequest|nlmFAck, seq, pid, nbdCmdDisconnect, nbdGenlVersion,
		[]attr{u32Attr(nbdAttrIndex, index)})
}

func statusRequest(family uint16, seq, pid uint32, index uint32) []byte {
	return marshalMessage(family, nlmFRequest|nlmFAck, seq, pid, nbdCmdStatus, nbdGenlVersion,
		[]attr{u32Attr(nbdAttrIndex, index)})
}

// parseStatusReply extracts the connected flag for index from a status reply's
// device list.
func parseStatusReply(m message, index uint32) (bool, error) {
	list, ok := attrByType(m.Attrs, nbdAttrDeviceList)
	if !ok {
		return false, errors.New("nbdnl: status reply carries no device list")
	}
	items, err := list.Nested()
	if err != nil {
		return false, err
	}
	for _, item := range items {
		fields, err := item.Nested()
		if err != nil {
			return false, err
		}
		idxAttr, okIdx := attrByType(fields, nbdDeviceIndex)
		connAttr, okConn := attrByType(fields, nbdDeviceConnected)
		if !okIdx || !okConn {
			continue
		}
		idx, err := idxAttr.U32()
		if err != nil {
			return false, err
		}
		if idx != index {
			continue
		}
		conn, err := connAttr.U8()
		if err != nil {
			return false, err
		}
		return conn != 0, nil
	}
	return false, fmt.Errorf("nbdnl: status reply does not mention device %d", index)
}

// parseLinkDead extracts the device index from an NBD_CMD_LINK_DEAD multicast
// notification, reporting ok=false for other notification kinds.
func parseLinkDead(m message) (uint32, bool) {
	if m.Cmd != nbdCmdLinkDead {
		return 0, false
	}
	idx, ok := attrByType(m.Attrs, nbdAttrIndex)
	if !ok {
		return 0, false
	}
	v, err := idx.U32()
	if err != nil {
		return 0, false
	}
	return v, true
}
