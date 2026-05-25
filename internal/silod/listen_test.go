package silod

import "net"

// listenTCP opens an ephemeral local TCP listener. Kept in a separate
// file so the test helper isn't pulled into the production binary.
func listenTCP() (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}
