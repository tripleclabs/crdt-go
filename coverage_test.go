package crdt

import "testing"

// Additional tests to ensure full coverage of exported and internal APIs.

// --- TypeID.String ---

func TestTypeID_String(t *testing.T) {
	tests := []struct {
		id   TypeID
		want string
	}{
		{TypeGCounter, "GCounter"},
		{TypePNCounter, "PNCounter"},
		{TypeORSet, "ORSet"},
		{TypeLWWRegister, "LWWRegister"},
		{TypeMVRegister, "MVRegister"},
		{TypeORMap, "ORMap"},
		{TypeLWWMap, "LWWMap"},
		{TypeAWLWWMap, "AWLWWMap"},
		{TypeGList, "GList"},
		{TypeDeltaMap, "DeltaMap"},
		{TypeID(255), "Unknown"},
	}
	for _, tt := range tests {
		if got := tt.id.String(); got != tt.want {
			t.Errorf("TypeID(%d).String() = %q, want %q", tt.id, got, tt.want)
		}
	}
}

// --- VClock methods on types that weren't called ---

func TestLWWMap_VClock(t *testing.T) {
	m := NewLWWMap(1)
	m, _ = m.Put("k", "v")
	vc := m.VClock()
	if vc.Get(1) != 1 {
		t.Fatalf("expected 1, got %d", vc.Get(1))
	}
}

func TestAWLWWMap_VClock(t *testing.T) {
	m := NewAWLWWMap(1)
	m, _ = m.Put("k", "v")
	vc := m.VClock()
	if vc.Get(1) != 1 {
		t.Fatalf("expected 1, got %d", vc.Get(1))
	}
}

func TestORMap_VClock(t *testing.T) {
	m := NewORMap(1)
	m, _ = m.Put("k", "v")
	vc := m.VClock()
	if vc.Get(1) != 1 {
		t.Fatalf("expected 1, got %d", vc.Get(1))
	}
}

func TestDeltaMap_VClock(t *testing.T) {
	m := NewDeltaMap(1)
	m, _ = m.Put("k", TypeLWWRegister, "v")
	vc := m.VClock()
	if vc.Get(1) != 1 {
		t.Fatalf("expected 1, got %d", vc.Get(1))
	}
}

// --- wrapValue coverage for all types ---

func TestWrapValue_AllTypes(t *testing.T) {
	types := []struct {
		typeID TypeID
		value  any
	}{
		{TypeGCounter, uint64(0)},
		{TypeGCounter, uint64(5)},
		{TypePNCounter, nil},
		{TypeLWWRegister, "hello"},
		{TypeLWWRegister, nil},
		{TypeMVRegister, "hello"},
		{TypeMVRegister, nil},
		{TypeORSet, nil},
		{TypeGList, nil},
		{TypeORMap, nil},
		{TypeLWWMap, nil},
		{TypeAWLWWMap, nil},
		{TypeDeltaMap, nil},
		{TypeID(255), nil}, // unknown type falls back to LWWRegister
	}
	for _, tt := range types {
		s := wrapValue(1, tt.typeID, tt.value)
		if s == nil {
			t.Errorf("wrapValue(%d, %v) returned nil", tt.typeID, tt.value)
		}
	}
}

// --- unmarshalByType coverage for all types ---

func TestUnmarshalByType_AllTypes(t *testing.T) {
	types := []struct {
		typeID TypeID
		create func() State
	}{
		{TypeGCounter, func() State { return NewGCounter(1) }},
		{TypePNCounter, func() State { return NewPNCounter(1) }},
		{TypeLWWRegister, func() State { r := NewLWWRegister(1); r, _ = r.Set("x"); return r }},
		{TypeMVRegister, func() State { r := NewMVRegister(1); r, _ = r.Write("x"); return r }},
		{TypeORSet, func() State { s := NewORSet(1); s, _ = s.Add("x"); return s }},
		{TypeGList, func() State { l := NewGList(1); l, _ = l.Append("x"); return l }},
		{TypeORMap, func() State { m := NewORMap(1); m, _ = m.Put("k", "v"); return m }},
		{TypeLWWMap, func() State { m := NewLWWMap(1); m, _ = m.Put("k", "v"); return m }},
		{TypeAWLWWMap, func() State { m := NewAWLWWMap(1); m, _ = m.Put("k", "v"); return m }},
		{TypeDeltaMap, func() State {
			m := NewDeltaMap(1)
			m, _ = m.Put("k", TypeLWWRegister, "v")
			return m
		}},
	}
	for _, tt := range types {
		original := tt.create()
		data, err := original.MarshalBinary()
		if err != nil {
			t.Fatalf("type %d: marshal failed: %v", tt.typeID, err)
		}
		restored, err := UnmarshalState(tt.typeID, data)
		if err != nil {
			t.Fatalf("type %d: unmarshal failed: %v", tt.typeID, err)
		}
		if restored.CRDTType() != tt.typeID {
			t.Fatalf("type %d: type mismatch after unmarshal", tt.typeID)
		}
	}
}

// --- DeltaMap merge: tombstone reconciliation ---

func TestDeltaMap_MergeRemoveWins(t *testing.T) {
	a := NewDeltaMap(1)
	a, _ = a.Put("k", TypeLWWRegister, "val")
	a, _ = a.Remove("k")

	b := NewDeltaMap(2) // empty

	merged := a.Merge(b).(*DeltaMap)
	if _, ok := merged.Get("k"); ok {
		t.Fatal("removed key should not exist after merge")
	}
}

func TestDeltaMap_MergeRemoveVsAddWins(t *testing.T) {
	// a removes key, b adds with higher counter — add wins.
	a := NewDeltaMap(1)
	a, _ = a.Put("k", TypeLWWRegister, "v1") // counter=1
	a, _ = a.Remove("k")                     // counter=2

	b := NewDeltaMap(2)
	b, _ = b.Put("k", TypeLWWRegister, "b1")
	b, _ = b.Put("k", TypeLWWRegister, "b2")
	b, _ = b.Put("k", TypeLWWRegister, "b3") // counter=3, > tombstone counter=2

	merged := a.Merge(b).(*DeltaMap)
	if _, ok := merged.Get("k"); !ok {
		t.Fatal("add with higher counter should survive tombstone")
	}
}

// --- ORSet/ORMap remove delta marshal/unmarshal ---

func TestORSetRemoveDelta_Methods(t *testing.T) {
	s := NewORSet(1)
	s, _ = s.Add("alice")
	_, d := s.Remove("alice")

	rd := d.State.(*orSetRemoveDelta)
	if rd.Value() != nil {
		t.Fatal("remove delta Value should be nil")
	}
	if rd.CRDTType() != TypeORSet {
		t.Fatal("wrong type")
	}
	if rd.VClock() == nil {
		t.Fatal("vclock should not be nil")
	}
	// Merge is a no-op.
	if rd.Merge(nil) != rd {
		t.Fatal("merge should return self")
	}

	// MarshalBinary.
	data, err := rd.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	rd2 := &orSetRemoveDelta{}
	if err := rd2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
}

func TestORMapRemoveDelta_Methods(t *testing.T) {
	m := NewORMap(1)
	m, _ = m.Put("k", "v")
	_, d := m.Remove("k")

	rd := d.State.(*orMapRemoveDelta)
	if rd.Value() != nil {
		t.Fatal("remove delta Value should be nil")
	}
	if rd.CRDTType() != TypeORMap {
		t.Fatal("wrong type")
	}
	if rd.Merge(nil) != rd {
		t.Fatal("merge should return self")
	}

	data, err := rd.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	rd2 := &orMapRemoveDelta{}
	if err := rd2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
}

// --- MVRegister merge: fallback when all dominated ---

func TestMVRegister_MergeAllDominated(t *testing.T) {
	// Create a scenario where no values survive the concurrent check
	// but the fallback picks the highest dot.
	a := NewMVRegister(1)
	a, _ = a.Write("first")

	// b has seen a (vclock includes a).
	b := a.Merge(NewMVRegister(2)).(*MVRegister)
	b.replica = 2
	b, _ = b.Write("second") // clears "first", writes "second"

	// Now merge a with b. a's "first" is dominated by b's vclock.
	// b's "second" is NOT dominated by a's vclock → should survive.
	merged := a.Merge(b).(*MVRegister)
	vals := merged.Values()
	if len(vals) != 1 || vals[0] != "second" {
		t.Fatalf("expected [second], got %v", vals)
	}
}

// --- DeltaMap merge: remote entry with existing key merges nested ---

func TestDeltaMap_MergeExistingKeyDiffReplica(t *testing.T) {
	a := NewDeltaMap(1)
	a, _ = a.Put("counter", TypeGCounter, uint64(5))

	b := NewDeltaMap(2)
	b, _ = b.Put("counter", TypeGCounter, uint64(3))

	// Merge should combine the GCounters.
	m1 := a.Merge(b).(*DeltaMap)
	m2 := b.Merge(a).(*DeltaMap)

	v1 := m1.Value().(map[string]any)
	v2 := m2.Value().(map[string]any)
	if v1["counter"] != v2["counter"] {
		t.Fatalf("not commutative: %v vs %v", v1, v2)
	}
}

// --- MerkleMap: hash collision path in Equal ---

func TestMerkleMap_EqualDifferentValues(t *testing.T) {
	a := NewMerkleMap()
	a.Put("k", []byte("val1"))

	b := NewMerkleMap()
	b.seed = a.seed
	b.Put("k", []byte("val2"))

	if a.Equal(b) {
		t.Fatal("different values should not be equal")
	}
}

// --- DeltaMap: Put with existing key ---

// --- orMapRemoveDelta VClock ---

func TestORMapRemoveDelta_VClock(t *testing.T) {
	m := NewORMap(1)
	m, _ = m.Put("k", "v")
	_, d := m.Remove("k")
	rd := d.State.(*orMapRemoveDelta)
	vc := rd.VClock()
	if vc == nil {
		t.Fatal("vclock should not be nil")
	}
}

// --- orSetRemoveDelta/orMapRemoveDelta already tested above ---

// --- DeltaMap marshal error paths via bad data ---

func TestGobDecode_ErrorOnBadData(t *testing.T) {
	c := &GCounter{}
	err := c.UnmarshalBinary([]byte{0xFF, 0xFE})
	if err == nil {
		t.Fatal("expected error on bad data")
	}
}

// --- MerkleMap Equal: hash match but different key ---

func TestMerkleMap_EqualMissingKey(t *testing.T) {
	a := NewMerkleMap()
	a.Put("x", []byte("1"))

	b := NewMerkleMap()
	b.seed = a.seed
	b.Put("y", []byte("1")) // different key

	if a.Equal(b) {
		t.Fatal("different keys should not be equal")
	}
}

// --- ORSet/ORMap mergeRemoveDelta on nonexistent key ---

func TestORSet_MergeRemoveDeltaNonexistent(t *testing.T) {
	s := NewORSet(1)
	s, _ = s.Add("alice")
	// Create remove delta for "bob" which doesn't exist in s.
	rd := &orSetRemoveDelta{replica: 2, value: "bob", vclock: VClock{2: 1}}
	merged := s.Merge(rd).(*ORSet)
	if !merged.Contains("alice") {
		t.Fatal("alice should still be present")
	}
}

func TestORMap_MergeRemoveDeltaNonexistent(t *testing.T) {
	m := NewORMap(1)
	m, _ = m.Put("a", 1)
	rd := &orMapRemoveDelta{replica: 2, key: "b", vclock: VClock{2: 1}}
	merged := m.Merge(rd).(*ORMap)
	if _, ok := merged.Get("a"); !ok {
		t.Fatal("a should still be present")
	}
}

// --- MVRegister merge: both empty ---

func TestMVRegister_MergeBothEmpty(t *testing.T) {
	a := NewMVRegister(1)
	b := NewMVRegister(2)
	merged := a.Merge(b).(*MVRegister)
	if len(merged.Values()) != 0 {
		t.Fatal("merge of two empty registers should be empty")
	}
}

// --- MVRegister merge: fallback when all values mutually dominated ---

func TestMVRegister_MergeFallbackBothDominated(t *testing.T) {
	// Construct states where all values are dominated by the other's vclock.
	// This happens when both replicas wrote after seeing each other.
	a := NewMVRegister(1)
	a, _ = a.Write("a-first") // dot {1,1}
	b := NewMVRegister(2)
	b, _ = b.Write("b-first") // dot {2,1}

	// Sync so both have seen each other.
	a = a.Merge(b).(*MVRegister)
	b = b.Merge(a).(*MVRegister)

	// Both write again. Now each has the other's vclock.
	a, _ = a.Write("a-second") // dot {1,2}, vclock {1:2, 2:1}
	b, _ = b.Write("b-second") // dot {2,2}, vclock {1:1, 2:2}

	// Merge: a's "a-second" dot {1,2} vs b's vclock {1:1} → 2 > 1 → survives
	// b's "b-second" dot {2,2} vs a's vclock {2:1} → 2 > 1 → survives
	// Both should survive as concurrent values.
	merged := a.Merge(b).(*MVRegister)
	vals := merged.Values()
	if len(vals) != 2 {
		t.Fatalf("expected 2 concurrent values, got %d: %v", len(vals), vals)
	}
}

// --- MVRegister merge: same value in both with different dots ---

func TestMVRegister_MergeSameValueBothSides(t *testing.T) {
	a := NewMVRegister(1)
	a, _ = a.Write("same") // dot {1,1}
	b := NewMVRegister(2)
	b, _ = b.Write("same") // dot {2,1}

	// Both have "same" but with different dots. After sync they should
	// keep "same" (deduped).
	merged := a.Merge(b).(*MVRegister)
	vals := merged.Values()
	if len(vals) != 1 || vals[0] != "same" {
		t.Fatalf("expected [same], got %v", vals)
	}
}

// --- DeltaMap: tombstone merging from both sides ---

func TestDeltaMap_MergeTombstonesFromBothSides(t *testing.T) {
	a := NewDeltaMap(1)
	a, _ = a.Put("k", TypeLWWRegister, "v")
	a, _ = a.Remove("k") // tombstone from replica 1

	b := NewDeltaMap(2)
	b, _ = b.Put("k", TypeLWWRegister, "v")
	b, _ = b.Remove("k") // tombstone from replica 2

	merged := a.Merge(b).(*DeltaMap)
	if _, ok := merged.Get("k"); ok {
		t.Fatal("doubly tombstoned key should not exist")
	}
}

func TestDeltaMap_PutExistingKey(t *testing.T) {
	m := NewDeltaMap(1)
	m, _ = m.Put("k", TypeLWWRegister, "first")
	m, _ = m.Put("k", TypeLWWRegister, "second")

	s, ok := m.Get("k")
	if !ok {
		t.Fatal("key should exist")
	}
	if s.Value() != "second" {
		t.Fatalf("expected second, got %v", s.Value())
	}
}
