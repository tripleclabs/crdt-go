package crdt

import "encoding/binary"

// Codec defines how a value type is encoded/decoded to bytes for storage
// and wire transport. Implement this for custom types to use them as CRDT
// values. The library provides built-in codecs: [StringCodec], [Int64Codec],
// [Uint64Codec], [BytesCodec].
type Codec[V any] interface {
	Encode(V) ([]byte, error)
	Decode([]byte) (V, error)
}

// requireCodec panics if codec is nil. Used by constructors to catch
// programming errors early.
func requireCodec[V any](codec Codec[V]) {
	if any(codec) == nil {
		panic("crdt: nil codec")
	}
}

// StringCodec encodes/decodes string values.
type StringCodec struct{}

func (StringCodec) Encode(s string) ([]byte, error) { return []byte(s), nil }
func (StringCodec) Decode(b []byte) (string, error) { return string(b), nil }

// Int64Codec encodes/decodes int64 values as 8 big-endian bytes.
type Int64Codec struct{}

func (Int64Codec) Encode(i int64) ([]byte, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b, nil
}
func (Int64Codec) Decode(b []byte) (int64, error) {
	if len(b) < 8 {
		return 0, ErrShortBuffer
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

// Uint64Codec encodes/decodes uint64 values as 8 big-endian bytes.
type Uint64Codec struct{}

func (Uint64Codec) Encode(u uint64) ([]byte, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b, nil
}
func (Uint64Codec) Decode(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, ErrShortBuffer
	}
	return binary.BigEndian.Uint64(b), nil
}

// BytesCodec encodes/decodes raw byte slices (passthrough).
type BytesCodec struct{}

func (BytesCodec) Encode(b []byte) ([]byte, error) { return b, nil }
func (BytesCodec) Decode(b []byte) ([]byte, error) {
	c := make([]byte, len(b))
	copy(c, b)
	return c, nil
}
