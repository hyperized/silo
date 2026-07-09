package nbdclient

import (
	"net"
	"strings"
	"testing"
)

func TestSocketFDRejectsConnWithoutDescriptor(t *testing.T) {
	// net.Pipe connections are pure in-memory and expose no syscall.Conn, so the
	// default extractor has no fd to hand the kernel — it must say so rather than
	// return a bogus descriptor.
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()
	if _, err := socketFD(c1); err == nil || !strings.Contains(err.Error(), "file descriptor") {
		t.Fatalf("socketFD(pipe) err = %v, want a no-file-descriptor error", err)
	}
}
