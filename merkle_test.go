package crdt

import "testing"

func TestNewMerkleMap(t *testing.T) {
	mm := NewMerkleMap()
	if mm.Len() != 0 {
		t.Fatal("new merkle map should be empty")
	}
}

func TestMerkleMap_PutGet(t *testing.T) {
	mm := NewMerkleMap()
	mm.Put("a", []byte("hello"))

	v, ok := mm.Get("a")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(v) != "hello" {
		t.Fatalf("expected hello, got %s", v)
	}

	_, ok = mm.Get("missing")
	if ok {
		t.Fatal("expected false for missing key")
	}
}

func TestMerkleMap_Delete(t *testing.T) {
	mm := NewMerkleMap()
	mm.Put("a", []byte("hello"))
	mm.Delete("a")
	if mm.Len() != 0 {
		t.Fatal("expected empty after delete")
	}

	// Delete non-existent is a no-op.
	mm.Delete("nonexistent")
}

func TestMerkleMap_Len(t *testing.T) {
	mm := NewMerkleMap()
	mm.Put("a", []byte("1"))
	mm.Put("b", []byte("2"))
	if mm.Len() != 2 {
		t.Fatalf("expected 2, got %d", mm.Len())
	}
}

func TestMerkleMap_Hash(t *testing.T) {
	a := NewMerkleMap()
	a.Put("x", []byte("1"))
	a.Put("y", []byte("2"))

	b := NewMerkleMap()
	b.seed = a.seed // use same seed for comparison
	b.Put("y", []byte("2"))
	b.Put("x", []byte("1"))

	if a.Hash() != b.Hash() {
		t.Fatal("same entries should have same hash regardless of insertion order")
	}

	b.Put("z", []byte("3"))
	if a.Hash() == b.Hash() {
		t.Fatal("different entries should (almost certainly) have different hashes")
	}
}

func TestMerkleMap_HashDirty(t *testing.T) {
	mm := NewMerkleMap()
	mm.Put("a", []byte("1"))
	h1 := mm.Hash()

	mm.Put("b", []byte("2"))
	h2 := mm.Hash()

	if h1 == h2 {
		t.Fatal("hash should change after modification")
	}
}

func TestMerkleMap_Equal(t *testing.T) {
	a := NewMerkleMap()
	a.Put("x", []byte("1"))

	b := NewMerkleMap()
	b.seed = a.seed
	b.Put("x", []byte("1"))

	if !a.Equal(b) {
		t.Fatal("expected equal")
	}

	b.Put("y", []byte("2"))
	if a.Equal(b) {
		t.Fatal("expected not equal")
	}
}

func TestMerkleMap_EqualEmpty(t *testing.T) {
	a := NewMerkleMap()
	b := NewMerkleMap()
	b.seed = a.seed
	if !a.Equal(b) {
		t.Fatal("two empty maps should be equal")
	}
}

func TestMerkleMap_DivergentKeys(t *testing.T) {
	a := NewMerkleMap()
	a.Put("x", []byte("1"))
	a.Put("y", []byte("2"))
	a.Put("z", []byte("3"))

	b := NewMerkleMap()
	b.Put("x", []byte("1"))       // same
	b.Put("y", []byte("changed")) // different value
	b.Put("w", []byte("new"))     // only in b

	divergent := a.DivergentKeys(b)
	// Should include: y (different value), z (only in a), w (only in b)
	if len(divergent) != 3 {
		t.Fatalf("expected 3 divergent keys, got %d: %v", len(divergent), divergent)
	}
	// Should be sorted.
	if divergent[0] != "w" || divergent[1] != "y" || divergent[2] != "z" {
		t.Fatalf("expected [w, y, z], got %v", divergent)
	}
}

func TestMerkleMap_DivergentKeysIdentical(t *testing.T) {
	a := NewMerkleMap()
	a.Put("x", []byte("1"))

	b := NewMerkleMap()
	b.Put("x", []byte("1"))

	divergent := a.DivergentKeys(b)
	if len(divergent) != 0 {
		t.Fatalf("expected 0 divergent keys, got %v", divergent)
	}
}

func TestMerkleMap_ZeroValue(t *testing.T) {
	var mm MerkleMap
	_, ok := mm.Get("x")
	if ok {
		t.Fatal("zero value should return false")
	}
	mm.Delete("x") // no-op
	if mm.Len() != 0 {
		t.Fatal("expected 0")
	}

	mm.Put("x", []byte("y"))
	v, ok := mm.Get("x")
	if !ok || string(v) != "y" {
		t.Fatal("put on zero value should work")
	}
}
