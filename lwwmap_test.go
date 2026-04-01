package crdt

import "testing"

func TestNewLWWMap(t *testing.T) {
	m := NewLWWMap(1)
	if m.Len() != 0 {
		t.Fatal("new map should be empty")
	}
}

func TestLWWMap_Put(t *testing.T) {
	m := NewLWWMap(1)
	m2, d := m.Put("name", "alice")

	v, ok := m2.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
	if m.Len() != 0 {
		t.Fatal("original should not be modified")
	}
	if d.Type != TypeLWWMap {
		t.Fatal("wrong delta type")
	}
}

func TestLWWMap_PutOverwrite(t *testing.T) {
	m := NewLWWMap(1)
	m, _ = m.Put("k", "v1")
	m, _ = m.Put("k", "v2")
	v, _ := m.Get("k")
	if v != "v2" {
		t.Fatalf("expected v2, got %v", v)
	}
}

func TestLWWMap_Remove(t *testing.T) {
	m := NewLWWMap(1)
	m, _ = m.Put("a", 1)
	m, _ = m.Put("b", 2)
	m2, _ := m.Remove("a")

	if _, ok := m2.Get("a"); ok {
		t.Fatal("a should be removed")
	}
	if m2.Len() != 1 {
		t.Fatalf("expected 1, got %d", m2.Len())
	}
	// Original unchanged.
	if _, ok := m.Get("a"); !ok {
		t.Fatal("original should not be modified")
	}
}

func TestLWWMap_Get(t *testing.T) {
	m := NewLWWMap(1)
	_, ok := m.Get("missing")
	if ok {
		t.Fatal("expected false for missing key")
	}
}

func TestLWWMap_Value(t *testing.T) {
	m := NewLWWMap(1)
	m, _ = m.Put("x", 42)
	v := m.Value().(map[string]any)
	if v["x"] != 42 {
		t.Fatal("Value() mismatch")
	}
}

func TestLWWMap_MergeLWW(t *testing.T) {
	a := NewLWWMap(1)
	a, _ = a.Put("k", "a-val") // counter=1

	b := NewLWWMap(2)
	b, _ = b.Put("k", "b-first")
	b, _ = b.Put("k", "b-second") // counter=2, higher wins

	merged := a.Merge(b).(*LWWMap)
	v, _ := merged.Get("k")
	if v != "b-second" {
		t.Fatalf("expected b-second (higher counter), got %v", v)
	}

	// Commutative.
	merged2 := b.Merge(a).(*LWWMap)
	v2, _ := merged2.Get("k")
	if v2 != "b-second" {
		t.Fatalf("commutative: expected b-second, got %v", v2)
	}
}

func TestLWWMap_MergeTieBreak(t *testing.T) {
	a := NewLWWMap(1)
	a, _ = a.Put("k", "from-1") // counter=1, replica=1

	b := NewLWWMap(2)
	b, _ = b.Put("k", "from-2") // counter=1, replica=2

	// Same counter, lower replica wins.
	merged := a.Merge(b).(*LWWMap)
	v, _ := merged.Get("k")
	if v != "from-1" {
		t.Fatalf("expected from-1 (lower replica), got %v", v)
	}
}

func TestLWWMap_MergePutBeatsRemove(t *testing.T) {
	// Put at counter=2 should beat remove at counter=1.
	a := NewLWWMap(1)
	a, _ = a.Put("k", "val")
	a, _ = a.Put("k", "val2") // counter=2

	b := NewLWWMap(2)
	b, _ = b.Put("k", "temp")
	b, _ = b.Remove("k") // counter=2, but replica 2 > replica 1 for dot comparison
	// Actually b's remove dot is {2, 2} and a's entry dot is {1, 2}.
	// DotGT({1,2}, {2,2}) = false (same counter, 1 < 2 → true). So entry wins.

	merged := a.Merge(b).(*LWWMap)
	v, ok := merged.Get("k")
	if !ok || v != "val2" {
		t.Fatalf("put should beat concurrent remove: got %v, ok=%v", v, ok)
	}
}

func TestLWWMap_MergeRemoveAfterPut(t *testing.T) {
	m := NewLWWMap(1)
	m, _ = m.Put("k", "val") // counter=1
	m, _ = m.Remove("k")     // counter=2, tombstone beats entry

	// Create a stale replica that only saw the put.
	stale := NewLWWMap(2)
	stale = stale.Merge(NewLWWMap(1)).(*LWWMap) // empty
	stale.replica = 2

	merged := stale.Merge(m).(*LWWMap)
	if _, ok := merged.Get("k"); ok {
		t.Fatal("remove with higher counter should win")
	}
}

func TestLWWMap_MergeIdempotent(t *testing.T) {
	m := NewLWWMap(1)
	m, _ = m.Put("k", "v")
	merged := m.Merge(m).(*LWWMap)
	if merged.Len() != 1 {
		t.Fatal("merge with self should be idempotent")
	}
}

func TestLWWMap_MergeDelta(t *testing.T) {
	a := NewLWWMap(1)
	a, _ = a.Put("a", 1)

	b := NewLWWMap(2)
	_, d := b.Put("b", 2)

	merged := a.Merge(d.State).(*LWWMap)
	if merged.Len() != 2 {
		t.Fatalf("expected 2, got %d", merged.Len())
	}
}

func TestLWWMap_CRDTType(t *testing.T) {
	m := NewLWWMap(1)
	if m.CRDTType() != TypeLWWMap {
		t.Fatal("wrong type")
	}
}

func TestLWWMap_MarshalUnmarshal(t *testing.T) {
	m := NewLWWMap(1)
	m, _ = m.Put("a", "hello")
	m, _ = m.Put("b", "world")

	data, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	m2 := &LWWMap{}
	if err := m2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if m2.Len() != 2 {
		t.Fatalf("expected 2, got %d", m2.Len())
	}
}

func TestLWWMap_OriginalUnchangedAfterMerge(t *testing.T) {
	a := NewLWWMap(1)
	a, _ = a.Put("a", 1)
	b := NewLWWMap(2)
	b, _ = b.Put("b", 2)
	a.Merge(b)
	if a.Len() != 1 {
		t.Fatal("merge should not modify receiver")
	}
}
