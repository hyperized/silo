package nbd

// SetMaxRequestBytesForTest lowers the per-request cap so tests can exercise
// the oversize-request path without sending huge payloads. It returns a
// function that restores the previous value.
func SetMaxRequestBytesForTest(n uint32) func() {
	prev := maxRequestBytes
	maxRequestBytes = n
	return func() { maxRequestBytes = prev }
}
