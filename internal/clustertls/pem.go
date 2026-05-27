package clustertls

import "encoding/pem"

// pemEncode wraps encoding/pem so the whole package can stay free of
// the import; PEM is an implementation detail of how we serialise
// certs and keys.
func pemEncode(blockType string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
}
