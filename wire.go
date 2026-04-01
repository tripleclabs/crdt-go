package crdt

import (
	"encoding/binary"
	"errors"
)

// Wire encoding for CRDT metadata types. Uses fixed-width big-endian encoding
// with no preamble or version byte. These formats are used for both on-disk
// storage (bbolt entry metadata) and sync protocol messages.
//
// Dot: 16 bytes (8 replica + 8 counter)
// DotMap/VClock: 4-byte count + 16 bytes per entry

// Op codes for delta wire format.
const (
	OpPut    byte = 0x01
	OpRemove byte = 0x02
)

var (
	// ErrShortBuffer indicates the byte slice is too short for the expected data.
	ErrShortBuffer = errors.New("crdt: short buffer")
	// ErrInvalidData indicates the byte slice contains invalid encoded data.
	ErrInvalidData = errors.New("crdt: invalid data")
	// ErrUnknownOp indicates an unknown operation code in a delta.
	ErrUnknownOp = errors.New("crdt: unknown delta op")
)

// EncodeDot encodes a Dot as 16 bytes: 8 bytes replica (big-endian) + 8 bytes
// counter (big-endian).
func EncodeDot(d Dot) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], d.Replica)
	binary.BigEndian.PutUint64(buf[8:16], d.Counter)
	return buf
}

// DecodeDot decodes a Dot from 16 bytes. Returns an error if the buffer is
// too short.
func DecodeDot(b []byte) (Dot, error) {
	if len(b) < 16 {
		return Dot{}, ErrShortBuffer
	}
	return Dot{
		Replica: binary.BigEndian.Uint64(b[0:8]),
		Counter: binary.BigEndian.Uint64(b[8:16]),
	}, nil
}

// EncodeDotMap encodes a DotMap as a 4-byte entry count followed by 16 bytes
// per entry (8 bytes replica + 8 bytes counter). The encoding is deterministic:
// entries are sorted by replica ID.
func EncodeDotMap(dm DotMap) []byte {
	n := len(dm)
	buf := make([]byte, 4+n*16)
	binary.BigEndian.PutUint32(buf[0:4], uint32(n))

	// Sort replica IDs for deterministic encoding.
	keys := sortedReplicaIDs(dm)
	off := 4
	for _, r := range keys {
		binary.BigEndian.PutUint64(buf[off:off+8], r)
		binary.BigEndian.PutUint64(buf[off+8:off+16], dm[r])
		off += 16
	}
	return buf
}

// DecodeDotMap decodes a DotMap from bytes. Returns an error if the buffer
// is malformed.
func DecodeDotMap(b []byte) (DotMap, error) {
	if len(b) < 4 {
		return nil, ErrShortBuffer
	}
	n := int(binary.BigEndian.Uint32(b[0:4]))
	if len(b) < 4+n*16 {
		return nil, ErrShortBuffer
	}
	dm := make(DotMap, n)
	off := 4
	for i := 0; i < n; i++ {
		r := binary.BigEndian.Uint64(b[off : off+8])
		c := binary.BigEndian.Uint64(b[off+8 : off+16])
		dm[r] = c
		off += 16
	}
	return dm, nil
}

// EncodeVClock encodes a VClock. The format is identical to [EncodeDotMap]
// since both are map[uint64]uint64.
func EncodeVClock(vc VClock) []byte {
	return EncodeDotMap(DotMap(vc))
}

// DecodeVClock decodes a VClock from bytes.
func DecodeVClock(b []byte) (VClock, error) {
	dm, err := DecodeDotMap(b)
	if err != nil {
		return nil, err
	}
	return VClock(dm), nil
}

// AppendVarintBytes appends a varint-length-prefixed byte slice to dst.
func AppendVarintBytes(dst []byte, data []byte) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(data)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, data...)
	return dst
}

// ReadVarintBytes reads a varint-length-prefixed byte slice from b at offset.
func ReadVarintBytes(b []byte, offset int) ([]byte, int, error) {
	if offset >= len(b) {
		return nil, offset, ErrShortBuffer
	}
	length, n := binary.Uvarint(b[offset:])
	if n <= 0 {
		return nil, offset, ErrShortBuffer
	}
	offset += n
	end := offset + int(length)
	if end > len(b) {
		return nil, offset, ErrShortBuffer
	}
	return b[offset:end], end, nil
}

// sortedReplicaIDs returns the replica IDs from a DotMap in sorted order.
func sortedReplicaIDs(dm DotMap) []uint64 {
	keys := make([]uint64, 0, len(dm))
	for r := range dm {
		keys = append(keys, r)
	}
	// Simple insertion sort — DotMaps are typically small (<100 entries).
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
