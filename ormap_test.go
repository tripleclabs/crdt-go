package crdt

import "testing"

func TestNewORMap(t *testing.T) {
	m := NewORMap(1)
	if m.Len() != 0 {
		t.Fatal("new map should be empty")
	}
}

func TestORMap_Put(t *testing.T) {
	m := NewORMap(1)
	m2, d := m.Put("name", "alice")

	v, ok := m2.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
	if m.Len() != 0 {
		t.Fatal("original should not be modified")
	}
	if d.Type != TypeORMap {
		t.Fatal("wrong delta type")
	}
}

func TestORMap_PutOverwrite(t *testing.T) {
	m := NewORMap(1)
	m, _ = m.Put("k", "v1")
	m, _ = m.Put("k", "v2")
	v, ok := m.Get("k")
	if !ok || v != "v2" {
		t.Fatalf("expected v2, got %v", v)
	}
	if m.Len() != 1 {
		t.Fatal("overwrite should not create duplicate keys")
	}
}

func TestORMap_Remove(t *testing.T) {
	m := NewORMap(1)
	m, _ = m.Put("a", 1)
	m, _ = m.Put("b", 2)
	m2, _ := m.Remove("a")

	_, ok := m2.Get("a")
	if ok {
		t.Fatal("a should be removed")
	}
	v, ok := m2.Get("b")
	if !ok || v != 2 {
		t.Fatal("b should remain")
	}
	// Original unchanged.
	if _, ok := m.Get("a"); !ok {
		t.Fatal("original should not be modified")
	}
}

func TestORMap_Get(t *testing.T) {
	m := NewORMap(1)
	_, ok := m.Get("missing")
	if ok {
		t.Fatal("expected false for missing key")
	}
}

func TestORMap_Value(t *testing.T) {
	m := NewORMap(1)
	m, _ = m.Put("x", 42)
	v := m.Value().(map[string]any)
	if v["x"] != 42 {
		t.Fatal("Value() mismatch")
	}
}

func TestORMap_Map(t *testing.T) {
	m := NewORMap(1)
	m, _ = m.Put("a", 1)
	m, _ = m.Put("b", 2)
	mp := m.Map()
	if len(mp) != 2 || mp["a"] != 1 || mp["b"] != 2 {
		t.Fatalf("unexpected map: %v", mp)
	}
}

func TestORMap_MergeAddAdd(t *testing.T) {
	a := NewORMap(1)
	a, _ = a.Put("from-a", "a-val")

	b := NewORMap(2)
	b, _ = b.Put("from-b", "b-val")

	merged := a.Merge(b).(*ORMap)
	if merged.Len() != 2 {
		t.Fatalf("expected 2, got %d", merged.Len())
	}

	// Commutative.
	merged2 := b.Merge(a).(*ORMap)
	if merged2.Len() != 2 {
		t.Fatalf("commutative: expected 2, got %d", merged2.Len())
	}
}

func TestORMap_MergeAddWins(t *testing.T) {
	a := NewORMap(1)
	a, _ = a.Put("key", "from-a")

	b := NewORMap(2)
	b, _ = b.Put("key", "from-b")
	b, _ = b.Remove("key")

	// a's add should survive b's remove (concurrent, add-wins).
	merged := a.Merge(b).(*ORMap)
	v, ok := merged.Get("key")
	if !ok {
		t.Fatal("add-wins: key should survive concurrent add/remove")
	}
	if v != "from-a" {
		t.Fatalf("expected from-a, got %v", v)
	}
}

func TestORMap_MergeObservedRemove(t *testing.T) {
	a := NewORMap(1)
	a, _ = a.Put("key", "val")

	// b sees a's state then removes.
	b := a.Merge(NewORMap(2)).(*ORMap)
	b.replica = 2
	b, _ = b.Remove("key")

	merged := a.Merge(b).(*ORMap)
	if _, ok := merged.Get("key"); ok {
		t.Fatal("key should be removed after observed remove")
	}
}

func TestORMap_MergeIdempotent(t *testing.T) {
	m := NewORMap(1)
	m, _ = m.Put("k", "v")
	merged := m.Merge(m).(*ORMap)
	if merged.Len() != 1 {
		t.Fatal("merge with self should be idempotent")
	}
}

func TestORMap_MergeDeltaAdd(t *testing.T) {
	a := NewORMap(1)
	a, _ = a.Put("a", 1)

	b := NewORMap(2)
	_, d := b.Put("b", 2)

	merged := a.Merge(d.State).(*ORMap)
	if merged.Len() != 2 {
		t.Fatalf("expected 2, got %d", merged.Len())
	}
}

func TestORMap_MergeDeltaRemove(t *testing.T) {
	a := NewORMap(1)
	a, _ = a.Put("key", "val")

	// b has seen a and removes key.
	b := a.Merge(NewORMap(2)).(*ORMap)
	b.replica = 2
	_, d := b.Remove("key")

	merged := a.Merge(d.State).(*ORMap)
	if _, ok := merged.Get("key"); ok {
		t.Fatal("delta remove should remove observed key")
	}
}

func TestORMap_CRDTType(t *testing.T) {
	m := NewORMap(1)
	if m.CRDTType() != TypeORMap {
		t.Fatal("wrong type")
	}
}

func TestORMap_MarshalUnmarshal(t *testing.T) {
	m := NewORMap(1)
	m, _ = m.Put("a", "hello")
	m, _ = m.Put("b", "world")

	data, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	m2 := &ORMap{}
	if err := m2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if m2.Len() != 2 {
		t.Fatalf("expected 2, got %d", m2.Len())
	}
}

func TestORMap_OriginalUnchangedAfterMerge(t *testing.T) {
	a := NewORMap(1)
	a, _ = a.Put("a", 1)
	b := NewORMap(2)
	b, _ = b.Put("b", 2)
	a.Merge(b)
	if a.Len() != 1 {
		t.Fatal("merge should not modify receiver")
	}
}
