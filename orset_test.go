package crdt

import (
	"sort"
	"testing"
)

func sortedStrings(v any) []string {
	elems := v.([]any)
	out := make([]string, len(elems))
	for i, e := range elems {
		out[i] = e.(string)
	}
	sort.Strings(out)
	return out
}

func TestNewORSet(t *testing.T) {
	s := NewORSet(1)
	if s.Len() != 0 {
		t.Fatal("new set should be empty")
	}
}

func TestORSet_Add(t *testing.T) {
	s := NewORSet(1)
	s2, d := s.Add("alice")

	if !s2.Contains("alice") {
		t.Fatal("expected set to contain alice")
	}
	if s2.Len() != 1 {
		t.Fatalf("expected len 1, got %d", s2.Len())
	}
	if s.Len() != 0 {
		t.Fatal("original should not be modified")
	}
	if d.Type != TypeORSet {
		t.Fatal("wrong delta type")
	}
}

func TestORSet_AddDuplicate(t *testing.T) {
	s := NewORSet(1)
	s, _ = s.Add("alice")
	s, _ = s.Add("alice")
	if s.Len() != 1 {
		t.Fatalf("expected 1 element, got %d", s.Len())
	}
}

func TestORSet_Remove(t *testing.T) {
	s := NewORSet(1)
	s, _ = s.Add("alice")
	s, _ = s.Add("bob")
	s2, _ := s.Remove("alice")

	if s2.Contains("alice") {
		t.Fatal("alice should be removed")
	}
	if !s2.Contains("bob") {
		t.Fatal("bob should still be present")
	}
	if s2.Len() != 1 {
		t.Fatalf("expected 1, got %d", s2.Len())
	}
	// Original unchanged.
	if !s.Contains("alice") {
		t.Fatal("original should not be modified")
	}
}

func TestORSet_RemoveNonexistent(t *testing.T) {
	s := NewORSet(1)
	s2, _ := s.Remove("ghost")
	if s2.Len() != 0 {
		t.Fatal("remove of nonexistent should not add elements")
	}
}

func TestORSet_Contains(t *testing.T) {
	s := NewORSet(1)
	if s.Contains("x") {
		t.Fatal("empty set should not contain anything")
	}
	s, _ = s.Add("x")
	if !s.Contains("x") {
		t.Fatal("should contain x")
	}
}

func TestORSet_Elements(t *testing.T) {
	s := NewORSet(1)
	s, _ = s.Add("a")
	s, _ = s.Add("b")
	s, _ = s.Add("c")

	elems := sortedStrings(s.Value())
	if len(elems) != 3 || elems[0] != "a" || elems[1] != "b" || elems[2] != "c" {
		t.Fatalf("unexpected elements: %v", elems)
	}
}

func TestORSet_VClock(t *testing.T) {
	s := NewORSet(1)
	s, _ = s.Add("a")
	s, _ = s.Add("b")
	vc := s.VClock()
	if vc.Get(1) != 2 {
		t.Fatalf("expected vclock[1]=2, got %d", vc.Get(1))
	}
}

func TestORSet_MergeAddAdd(t *testing.T) {
	a := NewORSet(1)
	a, _ = a.Add("alice")

	b := NewORSet(2)
	b, _ = b.Add("bob")

	merged := a.Merge(b).(*ORSet)
	elems := sortedStrings(merged.Value())
	if len(elems) != 2 || elems[0] != "alice" || elems[1] != "bob" {
		t.Fatalf("expected [alice, bob], got %v", elems)
	}

	// Commutative.
	merged2 := b.Merge(a).(*ORSet)
	elems2 := sortedStrings(merged2.Value())
	if len(elems2) != 2 || elems2[0] != "alice" || elems2[1] != "bob" {
		t.Fatalf("commutative: expected [alice, bob], got %v", elems2)
	}
}

func TestORSet_MergeAddWins(t *testing.T) {
	// a adds "alice", b adds "alice" then removes it.
	// Meanwhile a doesn't know about the remove — add-wins.
	a := NewORSet(1)
	a, _ = a.Add("alice")

	b := NewORSet(2)
	b, _ = b.Add("alice")
	b, _ = b.Remove("alice")

	merged := a.Merge(b).(*ORSet)
	if !merged.Contains("alice") {
		t.Fatal("add-wins: alice should survive concurrent add/remove")
	}
}

func TestORSet_MergeRemoveObserved(t *testing.T) {
	// a adds "alice", syncs to b, then b removes "alice".
	// After full sync, alice should be gone from both.
	a := NewORSet(1)
	a, _ = a.Add("alice")

	// b receives a's full state.
	b := NewORSet(2)
	b = b.Merge(a).(*ORSet)

	// b removes alice (has seen the add dot).
	b, _ = b.Remove("alice")

	// a merges b's state — alice should be gone because a's dot was observed.
	merged := a.Merge(b).(*ORSet)
	if merged.Contains("alice") {
		t.Fatal("alice should be removed after observed remove")
	}
}

func TestORSet_MergeIdempotent(t *testing.T) {
	s := NewORSet(1)
	s, _ = s.Add("a")
	s, _ = s.Add("b")
	merged := s.Merge(s).(*ORSet)
	if merged.Len() != s.Len() {
		t.Fatal("merge with self should be idempotent")
	}
}

func TestORSet_MergeDeltaAdd(t *testing.T) {
	a := NewORSet(1)
	a, _ = a.Add("alice")

	b := NewORSet(2)
	_, d := b.Add("bob")

	merged := a.Merge(d.State).(*ORSet)
	if !merged.Contains("alice") || !merged.Contains("bob") {
		t.Fatal("delta add merge failed")
	}
}

func TestORSet_MergeDeltaRemove(t *testing.T) {
	a := NewORSet(1)
	a, _ = a.Add("alice")

	// b has seen a's state and removes alice.
	b := a.Merge(NewORSet(2)).(*ORSet)
	b.replica = 2
	_, d := b.Remove("alice")

	merged := a.Merge(d.State).(*ORSet)
	if merged.Contains("alice") {
		t.Fatal("delta remove should remove observed element")
	}
}

func TestORSet_CRDTType(t *testing.T) {
	s := NewORSet(1)
	if s.CRDTType() != TypeORSet {
		t.Fatal("wrong type")
	}
}

func TestORSet_MarshalUnmarshal(t *testing.T) {
	s := NewORSet(1)
	s, _ = s.Add("alice")
	s, _ = s.Add("bob")

	data, err := s.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	s2 := &ORSet{}
	if err := s2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if s2.Len() != 2 {
		t.Fatalf("expected 2 elements, got %d", s2.Len())
	}
	if !s2.Contains("alice") || !s2.Contains("bob") {
		t.Fatal("deserialized set missing elements")
	}
}

func TestORSet_OriginalUnchangedAfterMerge(t *testing.T) {
	a := NewORSet(1)
	a, _ = a.Add("a")
	b := NewORSet(2)
	b, _ = b.Add("b")

	a.Merge(b)
	if a.Len() != 1 || !a.Contains("a") {
		t.Fatal("merge should not modify receiver")
	}
}

func TestORSet_ConcurrentAddRemoveAddWins(t *testing.T) {
	// Replica 1 and 2 both have "x". Replica 1 removes it, replica 2
	// concurrently adds it again. The re-add should win.
	base := NewORSet(1)
	base, _ = base.Add("x")

	// Both start from same state.
	a := base.Merge(NewORSet(1)).(*ORSet)
	a.replica = 1
	b := base.Merge(NewORSet(2)).(*ORSet)
	b.replica = 2

	a, _ = a.Remove("x")
	b, _ = b.Add("x") // concurrent re-add

	merged := a.Merge(b).(*ORSet)
	if !merged.Contains("x") {
		t.Fatal("concurrent re-add should win")
	}

	merged2 := b.Merge(a).(*ORSet)
	if !merged2.Contains("x") {
		t.Fatal("commutative: concurrent re-add should win")
	}
}
