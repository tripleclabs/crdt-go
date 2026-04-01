package crdt

import "testing"

func TestNewLWWRegister(t *testing.T) {
	r := NewLWWRegister(1)
	if r.Value() != nil {
		t.Fatal("new register should have nil value")
	}
	_, ok := r.Get()
	if ok {
		t.Fatal("new register Get should return false")
	}
}

func TestLWWRegister_Set(t *testing.T) {
	r := NewLWWRegister(1)
	r2, d := r.Set("hello")

	v, ok := r2.Get()
	if !ok || v != "hello" {
		t.Fatalf("expected 'hello', got %v", v)
	}
	// Original unchanged.
	if r.Value() != nil {
		t.Fatal("original should not be modified")
	}
	if d.Type != TypeLWWRegister {
		t.Fatal("wrong delta type")
	}
}

func TestLWWRegister_SetOverwrite(t *testing.T) {
	r := NewLWWRegister(1)
	r, _ = r.Set("first")
	r, _ = r.Set("second")

	v, ok := r.Get()
	if !ok || v != "second" {
		t.Fatalf("expected 'second', got %v", v)
	}
}

func TestLWWRegister_Value(t *testing.T) {
	r := NewLWWRegister(1)
	r, _ = r.Set(42)
	if r.Value() != 42 {
		t.Fatal("Value() mismatch")
	}
}

func TestLWWRegister_VClock(t *testing.T) {
	r := NewLWWRegister(1)
	r, _ = r.Set("a")
	r, _ = r.Set("b")
	vc := r.VClock()
	if vc.Get(1) != 2 {
		t.Fatalf("expected vclock[1]=2, got %d", vc.Get(1))
	}
}

func TestLWWRegister_MergeHigherCounterWins(t *testing.T) {
	a := NewLWWRegister(1)
	a, _ = a.Set("a1")

	b := NewLWWRegister(2)
	b, _ = b.Set("b1")
	b, _ = b.Set("b2") // b has higher counter

	merged := a.Merge(b).(*LWWRegister)
	if merged.Value() != "b2" {
		t.Fatalf("expected 'b2', got %v", merged.Value())
	}

	// Commutative.
	merged2 := b.Merge(a).(*LWWRegister)
	if merged2.Value() != "b2" {
		t.Fatalf("commutative: expected 'b2', got %v", merged2.Value())
	}
}

func TestLWWRegister_MergeTieBreakOnReplica(t *testing.T) {
	a := NewLWWRegister(1)
	a, _ = a.Set("from-1") // counter=1, replica=1

	b := NewLWWRegister(2)
	b, _ = b.Set("from-2") // counter=1, replica=2

	// Same counter, lower replica wins → replica 1 wins.
	merged := a.Merge(b).(*LWWRegister)
	if merged.Value() != "from-1" {
		t.Fatalf("expected 'from-1' (lower replica wins), got %v", merged.Value())
	}

	merged2 := b.Merge(a).(*LWWRegister)
	if merged2.Value() != "from-1" {
		t.Fatalf("commutative: expected 'from-1', got %v", merged2.Value())
	}
}

func TestLWWRegister_MergeWithEmpty(t *testing.T) {
	a := NewLWWRegister(1)
	a, _ = a.Set("hello")
	b := NewLWWRegister(2) // empty, never set

	merged := a.Merge(b).(*LWWRegister)
	if merged.Value() != "hello" {
		t.Fatalf("expected 'hello', got %v", merged.Value())
	}

	merged2 := b.Merge(a).(*LWWRegister)
	if merged2.Value() != "hello" {
		t.Fatalf("expected 'hello', got %v", merged2.Value())
	}
}

func TestLWWRegister_MergeIdempotent(t *testing.T) {
	r := NewLWWRegister(1)
	r, _ = r.Set("val")
	merged := r.Merge(r).(*LWWRegister)
	if merged.Value() != "val" {
		t.Fatal("merge with self should be idempotent")
	}
}

func TestLWWRegister_MergeDelta(t *testing.T) {
	a := NewLWWRegister(1)
	a, _ = a.Set("old")

	b := NewLWWRegister(2)
	b, _ = b.Set("b1")
	_, d := b.Set("b2")

	merged := a.Merge(d.State).(*LWWRegister)
	if merged.Value() != "b2" {
		t.Fatalf("expected 'b2', got %v", merged.Value())
	}
}

func TestLWWRegister_CRDTType(t *testing.T) {
	r := NewLWWRegister(1)
	if r.CRDTType() != TypeLWWRegister {
		t.Fatal("wrong type")
	}
}

func TestLWWRegister_MarshalUnmarshal(t *testing.T) {
	r := NewLWWRegister(1)
	r, _ = r.Set("hello")

	data, err := r.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	r2 := &LWWRegister{}
	if err := r2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if r2.Value() != "hello" {
		t.Fatalf("expected 'hello', got %v", r2.Value())
	}
	if r2.replica != 1 {
		t.Fatalf("expected replica 1, got %d", r2.replica)
	}
}

func TestLWWRegister_OriginalUnchangedAfterMerge(t *testing.T) {
	a := NewLWWRegister(1)
	a, _ = a.Set("a-val")
	b := NewLWWRegister(2)
	b, _ = b.Set("b-val")
	b, _ = b.Set("b-val2")

	a.Merge(b)
	if a.Value() != "a-val" {
		t.Fatal("merge should not modify receiver")
	}
}
