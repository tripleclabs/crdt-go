package crdt

import "testing"

func TestNewAWLWWMap(t *testing.T) {
	m := NewAWLWWMap(1)
	if m.Len() != 0 {
		t.Fatal("new map should be empty")
	}
}

func TestAWLWWMap_Put(t *testing.T) {
	m := NewAWLWWMap(1)
	m2, d := m.Put("name", "alice")

	v, ok := m2.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
	if m.Len() != 0 {
		t.Fatal("original should not be modified")
	}
	if d.Type != TypeAWLWWMap {
		t.Fatal("wrong delta type")
	}
}

func TestAWLWWMap_PutOverwrite(t *testing.T) {
	m := NewAWLWWMap(1)
	m, _ = m.Put("k", "v1")
	m, _ = m.Put("k", "v2")
	v, _ := m.Get("k")
	if v != "v2" {
		t.Fatalf("expected v2, got %v", v)
	}
}

func TestAWLWWMap_Remove(t *testing.T) {
	m := NewAWLWWMap(1)
	m, _ = m.Put("a", 1)
	m, _ = m.Put("b", 2)
	m2, _ := m.Remove("a")

	if _, ok := m2.Get("a"); ok {
		t.Fatal("a should be removed")
	}
	if m2.Len() != 1 {
		t.Fatalf("expected 1, got %d", m2.Len())
	}
}

func TestAWLWWMap_Get(t *testing.T) {
	m := NewAWLWWMap(1)
	_, ok := m.Get("missing")
	if ok {
		t.Fatal("expected false for missing key")
	}
}

func TestAWLWWMap_Value(t *testing.T) {
	m := NewAWLWWMap(1)
	m, _ = m.Put("x", 42)
	v := m.Value().(map[string]any)
	if v["x"] != 42 {
		t.Fatal("Value() mismatch")
	}
}

func TestAWLWWMap_MergeAddWins(t *testing.T) {
	// Concurrent put and remove — put should win (add-wins).
	a := NewAWLWWMap(1)
	a, _ = a.Put("k", "from-a") // dot {1, 1}

	b := NewAWLWWMap(2)
	b, _ = b.Put("k", "temp")
	b, _ = b.Remove("k") // tombstone with context that doesn't include a's dot

	merged := a.Merge(b).(*AWLWWMap)
	v, ok := merged.Get("k")
	if !ok {
		t.Fatal("add-wins: key should survive concurrent remove")
	}
	if v != "from-a" {
		t.Fatalf("expected from-a, got %v", v)
	}

	// Commutative.
	merged2 := b.Merge(a).(*AWLWWMap)
	v2, ok := merged2.Get("k")
	if !ok || v2 != "from-a" {
		t.Fatalf("commutative: expected from-a, got %v, ok=%v", v2, ok)
	}
}

func TestAWLWWMap_MergeObservedRemove(t *testing.T) {
	// a puts key, syncs to b, b removes. After merge, key should be gone
	// because b's tombstone context covers a's dot.
	a := NewAWLWWMap(1)
	a, _ = a.Put("k", "val") // dot {1, 1}

	// b receives a's full state.
	b := a.Merge(NewAWLWWMap(2)).(*AWLWWMap)
	b.replica = 2
	b, _ = b.Remove("k") // tombstone context includes {1: 1}

	merged := a.Merge(b).(*AWLWWMap)
	if _, ok := merged.Get("k"); ok {
		t.Fatal("observed remove: key should be removed")
	}
}

func TestAWLWWMap_MergeReAddAfterRemove(t *testing.T) {
	// a puts, syncs to b, b removes, a re-adds. The re-add has a new dot
	// NOT covered by b's tombstone context — it should survive.
	a := NewAWLWWMap(1)
	a, _ = a.Put("k", "v1") // dot {1, 1}

	b := a.Merge(NewAWLWWMap(2)).(*AWLWWMap)
	b.replica = 2
	b, _ = b.Remove("k") // context {1: 1}

	a, _ = a.Put("k", "v2") // dot {1, 2}, not covered by context {1: 1}

	merged := a.Merge(b).(*AWLWWMap)
	v, ok := merged.Get("k")
	if !ok || v != "v2" {
		t.Fatalf("re-add should survive: got %v, ok=%v", v, ok)
	}
}

func TestAWLWWMap_MergeLWWOnSameKey(t *testing.T) {
	a := NewAWLWWMap(1)
	a, _ = a.Put("k", "a-val") // counter=1

	b := NewAWLWWMap(2)
	b, _ = b.Put("k", "b-first")
	b, _ = b.Put("k", "b-second") // counter=2, higher

	merged := a.Merge(b).(*AWLWWMap)
	v, _ := merged.Get("k")
	if v != "b-second" {
		t.Fatalf("expected b-second (higher counter), got %v", v)
	}
}

func TestAWLWWMap_MergeIdempotent(t *testing.T) {
	m := NewAWLWWMap(1)
	m, _ = m.Put("k", "v")
	merged := m.Merge(m).(*AWLWWMap)
	if merged.Len() != 1 {
		t.Fatal("merge with self should be idempotent")
	}
}

func TestAWLWWMap_MergeDelta(t *testing.T) {
	a := NewAWLWWMap(1)
	a, _ = a.Put("a", 1)

	b := NewAWLWWMap(2)
	_, d := b.Put("b", 2)

	merged := a.Merge(d.State).(*AWLWWMap)
	if merged.Len() != 2 {
		t.Fatalf("expected 2, got %d", merged.Len())
	}
}

func TestAWLWWMap_CRDTType(t *testing.T) {
	m := NewAWLWWMap(1)
	if m.CRDTType() != TypeAWLWWMap {
		t.Fatal("wrong type")
	}
}

func TestAWLWWMap_MarshalUnmarshal(t *testing.T) {
	m := NewAWLWWMap(1)
	m, _ = m.Put("a", "hello")
	m, _ = m.Put("b", "world")

	data, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	m2 := &AWLWWMap{}
	if err := m2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if m2.Len() != 2 {
		t.Fatalf("expected 2, got %d", m2.Len())
	}
}

func TestAWLWWMap_OriginalUnchangedAfterMerge(t *testing.T) {
	a := NewAWLWWMap(1)
	a, _ = a.Put("a", 1)
	b := NewAWLWWMap(2)
	b, _ = b.Put("b", 2)
	a.Merge(b)
	if a.Len() != 1 {
		t.Fatal("merge should not modify receiver")
	}
}
