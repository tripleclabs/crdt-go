package crdt

import "testing"

func TestEncodeDot(t *testing.T) {
	d := Dot{Replica: 42, Counter: 7}
	b := EncodeDot(d)
	if len(b) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(b))
	}

	d2, err := DecodeDot(b)
	if err != nil {
		t.Fatal(err)
	}
	if d2 != d {
		t.Fatalf("roundtrip failed: got %v, want %v", d2, d)
	}
}

func TestDecodeDot_ShortBuffer(t *testing.T) {
	_, err := DecodeDot([]byte{1, 2, 3})
	if err != ErrShortBuffer {
		t.Fatalf("expected ErrShortBuffer, got %v", err)
	}
}

func TestEncodeDotMap(t *testing.T) {
	dm := DotMap{3: 10, 1: 20, 2: 30}
	b := EncodeDotMap(dm)

	dm2, err := DecodeDotMap(b)
	if err != nil {
		t.Fatal(err)
	}
	if !DotMapEqual(dm, dm2) {
		t.Fatalf("roundtrip failed: got %v, want %v", dm2, dm)
	}
}

func TestEncodeDotMap_Empty(t *testing.T) {
	dm := DotMap{}
	b := EncodeDotMap(dm)
	if len(b) != 4 {
		t.Fatalf("expected 4 bytes for empty, got %d", len(b))
	}

	dm2, err := DecodeDotMap(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(dm2) != 0 {
		t.Fatalf("expected empty, got %v", dm2)
	}
}

func TestDecodeDotMap_ShortBuffer(t *testing.T) {
	_, err := DecodeDotMap([]byte{0, 0})
	if err != ErrShortBuffer {
		t.Fatalf("expected ErrShortBuffer, got %v", err)
	}

	// Count says 1 entry but no entry data.
	_, err = DecodeDotMap([]byte{0, 0, 0, 1})
	if err != ErrShortBuffer {
		t.Fatalf("expected ErrShortBuffer, got %v", err)
	}
}

func TestEncodeDotMap_Deterministic(t *testing.T) {
	dm := DotMap{3: 10, 1: 20, 2: 30}
	b1 := EncodeDotMap(dm)
	b2 := EncodeDotMap(dm)
	if string(b1) != string(b2) {
		t.Fatal("encoding should be deterministic")
	}
}

func TestEncodeVClock(t *testing.T) {
	vc := VClock{1: 5, 2: 3}
	b := EncodeVClock(vc)

	vc2, err := DecodeVClock(b)
	if err != nil {
		t.Fatal(err)
	}
	if !vc.Equal(vc2) {
		t.Fatalf("roundtrip failed: got %v, want %v", vc2, vc)
	}
}

func TestEncodeVClock_Empty(t *testing.T) {
	vc := VClock{}
	b := EncodeVClock(vc)
	vc2, err := DecodeVClock(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(vc2) != 0 {
		t.Fatalf("expected empty, got %v", vc2)
	}
}

func TestDecodeVClock_ShortBuffer(t *testing.T) {
	_, err := DecodeVClock([]byte{})
	if err != ErrShortBuffer {
		t.Fatalf("expected ErrShortBuffer, got %v", err)
	}
}

func TestSortedReplicaIDs(t *testing.T) {
	dm := DotMap{5: 1, 1: 1, 3: 1, 2: 1, 4: 1}
	keys := sortedReplicaIDs(dm)
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Fatalf("not sorted: %v", keys)
		}
	}
}

func TestEncodeDot_ZeroValues(t *testing.T) {
	d := Dot{}
	b := EncodeDot(d)
	d2, err := DecodeDot(b)
	if err != nil {
		t.Fatal(err)
	}
	if d2 != d {
		t.Fatalf("zero dot roundtrip failed")
	}
}

func TestEncodeDotMap_LargeValues(t *testing.T) {
	dm := DotMap{^uint64(0): ^uint64(0)} // max uint64
	b := EncodeDotMap(dm)
	dm2, err := DecodeDotMap(b)
	if err != nil {
		t.Fatal(err)
	}
	if !DotMapEqual(dm, dm2) {
		t.Fatal("max value roundtrip failed")
	}
}
