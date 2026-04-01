// Package replica provides the distributed orchestration layer for CRDT
// types. Each replica type wraps a CRDT storage type with clocks and merge
// logic, providing mutation (with dot stamping), delta application (with
// dot comparison), and anti-entropy (via received clock diffing).
package replica

import "github.com/3clabs/crdt"

// Op codes for delta wire format.
const (
	OpPut    byte = 0x01
	OpRemove byte = 0x02
)

// appendVarintBytes appends a varint-length-prefixed byte slice.
func appendVarintBytes(dst []byte, data []byte) []byte {
	return crdt.AppendVarintBytes(dst, data)
}

// readVarintBytes reads a varint-length-prefixed byte slice.
func readVarintBytes(b []byte, offset int) ([]byte, int, error) {
	return crdt.ReadVarintBytes(b, offset)
}
