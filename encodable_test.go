package crdt

import "testing"

func TestStringCodec(t *testing.T) {
	c := StringCodec{}
	b, err := c.Encode("hello")
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if v != "hello" {
		t.Fatalf("expected hello, got %s", v)
	}
}

func TestStringCodec_Empty(t *testing.T) {
	c := StringCodec{}
	b, _ := c.Encode("")
	v, _ := c.Decode(b)
	if v != "" {
		t.Fatal("expected empty")
	}
}

func TestInt64Codec(t *testing.T) {
	c := Int64Codec{}
	b, err := c.Encode(-42)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 8 {
		t.Fatalf("expected 8 bytes, got %d", len(b))
	}
	v, err := c.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if v != -42 {
		t.Fatalf("expected -42, got %d", v)
	}
}

func TestInt64Codec_ShortBuffer(t *testing.T) {
	c := Int64Codec{}
	_, err := c.Decode([]byte{1, 2})
	if err != ErrShortBuffer {
		t.Fatalf("expected ErrShortBuffer, got %v", err)
	}
}

func TestUint64Codec(t *testing.T) {
	c := Uint64Codec{}
	b, _ := c.Encode(12345678)
	v, _ := c.Decode(b)
	if v != 12345678 {
		t.Fatalf("expected 12345678, got %d", v)
	}
}

func TestUint64Codec_Max(t *testing.T) {
	c := Uint64Codec{}
	b, _ := c.Encode(^uint64(0))
	v, _ := c.Decode(b)
	if v != ^uint64(0) {
		t.Fatal("max uint64 roundtrip failed")
	}
}

func TestUint64Codec_ShortBuffer(t *testing.T) {
	c := Uint64Codec{}
	_, err := c.Decode([]byte{1})
	if err != ErrShortBuffer {
		t.Fatalf("expected ErrShortBuffer, got %v", err)
	}
}

func TestBytesCodec(t *testing.T) {
	c := BytesCodec{}
	b, _ := c.Encode([]byte{1, 2, 3})
	v, _ := c.Decode(b)
	if len(v) != 3 || v[0] != 1 || v[2] != 3 {
		t.Fatalf("expected [1 2 3], got %v", v)
	}
	// Verify it's a copy.
	b[0] = 99
	if v[0] != 1 {
		t.Fatal("Decode should copy")
	}
}

func TestBytesCodec_Nil(t *testing.T) {
	c := BytesCodec{}
	b, _ := c.Encode(nil)
	v, _ := c.Decode(b)
	if len(v) != 0 {
		t.Fatal("expected empty")
	}
}

// Compile-time checks.
var (
	_ Codec[string] = StringCodec{}
	_ Codec[int64]  = Int64Codec{}
	_ Codec[uint64] = Uint64Codec{}
	_ Codec[[]byte] = BytesCodec{}
)
