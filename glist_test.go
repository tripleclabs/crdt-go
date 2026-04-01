package crdt

import "testing"

func TestNewGList(t *testing.T) {
	l := NewGList(1)
	if l.Len() != 0 {
		t.Fatal("new list should be empty")
	}
}

func TestGList_Append(t *testing.T) {
	l := NewGList(1)
	l2, d := l.Append("hello")
	if l2.Len() != 1 {
		t.Fatalf("expected 1, got %d", l2.Len())
	}
	if l.Len() != 0 {
		t.Fatal("original should not be modified")
	}
	if d.Type != TypeGList {
		t.Fatal("wrong delta type")
	}
}

func TestGList_AppendMultiple(t *testing.T) {
	l := NewGList(1)
	l, _ = l.Append("a")
	l, _ = l.Append("b")
	l, _ = l.Append("c")

	items := l.Items()
	if len(items) != 3 {
		t.Fatalf("expected 3, got %d", len(items))
	}
	if items[0] != "a" || items[1] != "b" || items[2] != "c" {
		t.Fatalf("unexpected order: %v", items)
	}
}

func TestGList_Value(t *testing.T) {
	l := NewGList(1)
	l, _ = l.Append("x")
	v := l.Value().([]any)
	if len(v) != 1 || v[0] != "x" {
		t.Fatal("Value() mismatch")
	}
}

func TestGList_ItemsCausalOrder(t *testing.T) {
	// Two replicas appending concurrently. Items should be ordered by
	// counter then replica.
	a := NewGList(1)
	a, _ = a.Append("a1") // counter=1, replica=1

	b := NewGList(2)
	b, _ = b.Append("b1") // counter=1, replica=2

	merged := a.Merge(b).(*GList)
	items := merged.Items()
	if len(items) != 2 {
		t.Fatalf("expected 2, got %d", len(items))
	}
	// Same counter, lower replica first.
	if items[0] != "a1" || items[1] != "b1" {
		t.Fatalf("expected [a1, b1], got %v", items)
	}
}

func TestGList_VClock(t *testing.T) {
	l := NewGList(1)
	l, _ = l.Append("a")
	l, _ = l.Append("b")
	vc := l.VClock()
	if vc.Get(1) != 2 {
		t.Fatalf("expected vclock[1]=2, got %d", vc.Get(1))
	}
}

func TestGList_Merge(t *testing.T) {
	a := NewGList(1)
	a, _ = a.Append("a1")
	a, _ = a.Append("a2")

	b := NewGList(2)
	b, _ = b.Append("b1")

	merged := a.Merge(b).(*GList)
	if merged.Len() != 3 {
		t.Fatalf("expected 3, got %d", merged.Len())
	}

	// Commutative.
	merged2 := b.Merge(a).(*GList)
	if merged2.Len() != 3 {
		t.Fatalf("commutative: expected 3, got %d", merged2.Len())
	}
}

func TestGList_MergeDedup(t *testing.T) {
	a := NewGList(1)
	a, _ = a.Append("hello")

	// Merge the same state twice.
	merged := a.Merge(a).(*GList)
	if merged.Len() != 1 {
		t.Fatalf("expected dedup to 1, got %d", merged.Len())
	}
}

func TestGList_MergeIdempotent(t *testing.T) {
	l := NewGList(1)
	l, _ = l.Append("a")
	merged := l.Merge(l).(*GList)
	if merged.Len() != l.Len() {
		t.Fatal("merge with self should be idempotent")
	}
}

func TestGList_MergeDelta(t *testing.T) {
	a := NewGList(1)
	a, _ = a.Append("a1")

	b := NewGList(2)
	_, d := b.Append("b1")

	merged := a.Merge(d.State).(*GList)
	if merged.Len() != 2 {
		t.Fatalf("expected 2, got %d", merged.Len())
	}
}

func TestGList_CRDTType(t *testing.T) {
	l := NewGList(1)
	if l.CRDTType() != TypeGList {
		t.Fatal("wrong type")
	}
}

func TestGList_MarshalUnmarshal(t *testing.T) {
	l := NewGList(1)
	l, _ = l.Append("a")
	l, _ = l.Append("b")

	data, err := l.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	l2 := &GList{}
	if err := l2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if l2.Len() != 2 {
		t.Fatalf("expected 2, got %d", l2.Len())
	}
	items := l2.Items()
	if items[0] != "a" || items[1] != "b" {
		t.Fatalf("expected [a, b], got %v", items)
	}
}

func TestGList_OriginalUnchangedAfterMerge(t *testing.T) {
	a := NewGList(1)
	a, _ = a.Append("a")
	b := NewGList(2)
	b, _ = b.Append("b")

	a.Merge(b)
	if a.Len() != 1 {
		t.Fatal("merge should not modify receiver")
	}
}
