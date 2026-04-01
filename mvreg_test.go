package crdt

import (
	"sort"
	"testing"
)

func TestNewMVRegister(t *testing.T) {
	r := NewMVRegister(1)
	if len(r.Values()) != 0 {
		t.Fatal("new register should have no values")
	}
}

func TestMVRegister_Write(t *testing.T) {
	r := NewMVRegister(1)
	r2, d := r.Write("hello")

	vals := r2.Values()
	if len(vals) != 1 || vals[0] != "hello" {
		t.Fatalf("expected [hello], got %v", vals)
	}
	if len(r.Values()) != 0 {
		t.Fatal("original should not be modified")
	}
	if d.Type != TypeMVRegister {
		t.Fatal("wrong delta type")
	}
}

func TestMVRegister_WriteOverwrite(t *testing.T) {
	r := NewMVRegister(1)
	r, _ = r.Write("first")
	r, _ = r.Write("second")

	vals := r.Values()
	if len(vals) != 1 || vals[0] != "second" {
		t.Fatalf("expected [second], got %v", vals)
	}
}

func TestMVRegister_Value(t *testing.T) {
	r := NewMVRegister(1)
	r, _ = r.Write("test")
	v := r.Value().([]any)
	if len(v) != 1 || v[0] != "test" {
		t.Fatal("Value() mismatch")
	}
}

func TestMVRegister_VClock(t *testing.T) {
	r := NewMVRegister(1)
	r, _ = r.Write("a")
	r, _ = r.Write("b")
	vc := r.VClock()
	if vc.Get(1) != 2 {
		t.Fatalf("expected vclock[1]=2, got %d", vc.Get(1))
	}
}

func TestMVRegister_MergeConcurrentPreserved(t *testing.T) {
	a := NewMVRegister(1)
	a, _ = a.Write("from-a")

	b := NewMVRegister(2)
	b, _ = b.Write("from-b")

	merged := a.Merge(b).(*MVRegister)
	vals := merged.Values()
	strs := make([]string, len(vals))
	for i, v := range vals {
		strs[i] = v.(string)
	}
	sort.Strings(strs)
	if len(strs) != 2 || strs[0] != "from-a" || strs[1] != "from-b" {
		t.Fatalf("concurrent values should be preserved: %v", strs)
	}

	// Commutative.
	merged2 := b.Merge(a).(*MVRegister)
	vals2 := merged2.Values()
	strs2 := make([]string, len(vals2))
	for i, v := range vals2 {
		strs2[i] = v.(string)
	}
	sort.Strings(strs2)
	if len(strs2) != 2 || strs2[0] != "from-a" || strs2[1] != "from-b" {
		t.Fatalf("commutative: concurrent values should be preserved: %v", strs2)
	}
}

func TestMVRegister_MergeSubsequentWriteResolvesConflict(t *testing.T) {
	a := NewMVRegister(1)
	a, _ = a.Write("from-a")

	b := NewMVRegister(2)
	b, _ = b.Write("from-b")

	// Merge so both see each other.
	a = a.Merge(b).(*MVRegister)

	// Now a writes again — should resolve the conflict.
	a, _ = a.Write("resolved")
	merged := b.Merge(a).(*MVRegister)
	vals := merged.Values()
	if len(vals) != 1 || vals[0] != "resolved" {
		t.Fatalf("expected [resolved], got %v", vals)
	}
}

func TestMVRegister_MergeIdempotent(t *testing.T) {
	r := NewMVRegister(1)
	r, _ = r.Write("val")
	merged := r.Merge(r).(*MVRegister)
	vals := merged.Values()
	if len(vals) != 1 || vals[0] != "val" {
		t.Fatalf("merge with self should be idempotent: %v", vals)
	}
}

func TestMVRegister_MergeDelta(t *testing.T) {
	a := NewMVRegister(1)
	a, _ = a.Write("a-val")

	b := NewMVRegister(2)
	_, d := b.Write("b-val")

	merged := a.Merge(d.State).(*MVRegister)
	vals := merged.Values()
	if len(vals) != 2 {
		t.Fatalf("expected 2 concurrent values, got %d", len(vals))
	}
}

func TestMVRegister_CRDTType(t *testing.T) {
	r := NewMVRegister(1)
	if r.CRDTType() != TypeMVRegister {
		t.Fatal("wrong type")
	}
}

func TestMVRegister_MarshalUnmarshal(t *testing.T) {
	r := NewMVRegister(1)
	r, _ = r.Write("hello")

	data, err := r.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	r2 := &MVRegister{}
	if err := r2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	vals := r2.Values()
	if len(vals) != 1 || vals[0] != "hello" {
		t.Fatalf("expected [hello], got %v", vals)
	}
}

func TestMVRegister_OriginalUnchangedAfterMerge(t *testing.T) {
	a := NewMVRegister(1)
	a, _ = a.Write("a")
	b := NewMVRegister(2)
	b, _ = b.Write("b")

	a.Merge(b)
	vals := a.Values()
	if len(vals) != 1 || vals[0] != "a" {
		t.Fatal("merge should not modify receiver")
	}
}
