package crdt

import "testing"

func TestNewDeltaMap(t *testing.T) {
	m := NewDeltaMap(1)
	if m.Len() != 0 {
		t.Fatal("new deltamap should be empty")
	}
}

func TestDeltaMap_Put(t *testing.T) {
	m := NewDeltaMap(1)
	m2, d := m.Put("name", TypeLWWRegister, "alice")

	s, ok := m2.Get("name")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if s.Value() != "alice" {
		t.Fatalf("expected alice, got %v", s.Value())
	}
	if m.Len() != 0 {
		t.Fatal("original should not be modified")
	}
	if d.Type != TypeDeltaMap {
		t.Fatal("wrong delta type")
	}
}

func TestDeltaMap_PutCounter(t *testing.T) {
	m := NewDeltaMap(1)
	m, _ = m.Put("views", TypeGCounter, uint64(10))

	s, ok := m.Get("views")
	if !ok {
		t.Fatal("expected views key")
	}
	gc := s.(*GCounter)
	if gc.Int64() != 10 {
		t.Fatalf("expected 10, got %d", gc.Int64())
	}
}

func TestDeltaMap_Remove(t *testing.T) {
	m := NewDeltaMap(1)
	m, _ = m.Put("a", TypeLWWRegister, "val")
	m, _ = m.Put("b", TypeLWWRegister, "val2")
	m2, _ := m.Remove("a")

	if _, ok := m2.Get("a"); ok {
		t.Fatal("a should be removed")
	}
	if _, ok := m2.Get("b"); !ok {
		t.Fatal("b should remain")
	}
	if m2.Len() != 1 {
		t.Fatalf("expected 1, got %d", m2.Len())
	}
}

func TestDeltaMap_Get(t *testing.T) {
	m := NewDeltaMap(1)
	_, ok := m.Get("missing")
	if ok {
		t.Fatal("expected false for missing key")
	}
}

func TestDeltaMap_Value(t *testing.T) {
	m := NewDeltaMap(1)
	m, _ = m.Put("name", TypeLWWRegister, "alice")
	v := m.Value().(map[string]any)
	if v["name"] != "alice" {
		t.Fatal("Value() mismatch")
	}
}

func TestDeltaMap_MergeNestedCRDTs(t *testing.T) {
	a := NewDeltaMap(1)
	a, _ = a.Put("name", TypeLWWRegister, "alice")

	b := NewDeltaMap(2)
	b, _ = b.Put("age", TypeLWWRegister, 30)

	merged := a.Merge(b).(*DeltaMap)
	if merged.Len() != 2 {
		t.Fatalf("expected 2, got %d", merged.Len())
	}

	// Commutative.
	merged2 := b.Merge(a).(*DeltaMap)
	if merged2.Len() != 2 {
		t.Fatalf("commutative: expected 2, got %d", merged2.Len())
	}
}

func TestDeltaMap_MergeSameKeyMergesNested(t *testing.T) {
	// Both replicas put a GCounter for the same key.
	a := NewDeltaMap(1)
	a, _ = a.Put("views", TypeGCounter, uint64(5))

	b := NewDeltaMap(2)
	b, _ = b.Put("views", TypeGCounter, uint64(3))

	merged := a.Merge(b).(*DeltaMap)
	s, ok := merged.Get("views")
	if !ok {
		t.Fatal("views should exist")
	}
	gc := s.(*GCounter)
	// Both counters should be merged: 5 + 3 = 8.
	if gc.Int64() != 8 {
		t.Fatalf("expected 8, got %d", gc.Int64())
	}
}

func TestDeltaMap_MergeIdempotent(t *testing.T) {
	m := NewDeltaMap(1)
	m, _ = m.Put("k", TypeLWWRegister, "v")
	merged := m.Merge(m).(*DeltaMap)
	if merged.Len() != 1 {
		t.Fatal("merge with self should be idempotent")
	}
}

func TestDeltaMap_MergeDelta(t *testing.T) {
	a := NewDeltaMap(1)
	a, _ = a.Put("a", TypeLWWRegister, "val-a")

	b := NewDeltaMap(2)
	_, d := b.Put("b", TypeLWWRegister, "val-b")

	merged := a.Merge(d.State).(*DeltaMap)
	if merged.Len() != 2 {
		t.Fatalf("expected 2, got %d", merged.Len())
	}
}

func TestDeltaMap_CRDTType(t *testing.T) {
	m := NewDeltaMap(1)
	if m.CRDTType() != TypeDeltaMap {
		t.Fatal("wrong type")
	}
}

func TestDeltaMap_MarshalUnmarshal(t *testing.T) {
	m := NewDeltaMap(1)
	m, _ = m.Put("name", TypeLWWRegister, "alice")
	m, _ = m.Put("views", TypeGCounter, uint64(42))

	data, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	m2 := &DeltaMap{}
	if err := m2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if m2.Len() != 2 {
		t.Fatalf("expected 2, got %d", m2.Len())
	}
	s, ok := m2.Get("name")
	if !ok {
		t.Fatal("name should exist")
	}
	if s.Value() != "alice" {
		t.Fatalf("expected alice, got %v", s.Value())
	}
}

func TestDeltaMap_OriginalUnchangedAfterMerge(t *testing.T) {
	a := NewDeltaMap(1)
	a, _ = a.Put("a", TypeLWWRegister, 1)
	b := NewDeltaMap(2)
	b, _ = b.Put("b", TypeLWWRegister, 2)
	a.Merge(b)
	if a.Len() != 1 {
		t.Fatal("merge should not modify receiver")
	}
}

func TestUnmarshalState(t *testing.T) {
	c := NewGCounter(1)
	c, _ = c.Increment(42)
	data, err := c.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	s, err := UnmarshalState(TypeGCounter, data)
	if err != nil {
		t.Fatal(err)
	}
	gc := s.(*GCounter)
	if gc.Int64() != 42 {
		t.Fatalf("expected 42, got %d", gc.Int64())
	}
}

func TestUnmarshalState_UnknownType(t *testing.T) {
	_, err := UnmarshalState(TypeID(255), []byte{})
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}
