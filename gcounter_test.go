package crdt

import "testing"

func TestNewGCounter(t *testing.T) {
	c := NewGCounter(1)
	if c.Int64() != 0 {
		t.Fatal("new counter should be 0")
	}
}

func TestGCounter_Increment(t *testing.T) {
	c := NewGCounter(1)
	c2, d := c.Increment(5)
	if c2.Int64() != 5 {
		t.Fatalf("expected 5, got %d", c2.Int64())
	}
	// Original unchanged.
	if c.Int64() != 0 {
		t.Fatal("original should not be modified")
	}
	// Delta should be valid.
	if d.Type != TypeGCounter {
		t.Fatal("delta type mismatch")
	}
	if d.State.(*GCounter).Int64() != 5 {
		t.Fatal("delta should contain the new count")
	}
}

func TestGCounter_IncrementMultiple(t *testing.T) {
	c := NewGCounter(1)
	c, _ = c.Increment(3)
	c, _ = c.Increment(2)
	if c.Int64() != 5 {
		t.Fatalf("expected 5, got %d", c.Int64())
	}
}

func TestGCounter_Value(t *testing.T) {
	c := NewGCounter(1)
	c, _ = c.Increment(7)
	v := c.Value()
	if v.(int64) != 7 {
		t.Fatal("Value() should return int64")
	}
}

func TestGCounter_VClock(t *testing.T) {
	c := NewGCounter(1)
	c, _ = c.Increment(5)
	vc := c.VClock()
	if vc.Get(1) != 5 {
		t.Fatalf("expected vclock[1]=5, got %d", vc.Get(1))
	}
}

func TestGCounter_Merge(t *testing.T) {
	a := NewGCounter(1)
	a, _ = a.Increment(5)
	a, _ = a.Increment(3) // replica 1 = 8

	b := NewGCounter(2)
	b, _ = b.Increment(4) // replica 2 = 4

	merged := a.Merge(b).(*GCounter)
	if merged.Int64() != 12 {
		t.Fatalf("expected 12, got %d", merged.Int64())
	}

	// Merge is commutative.
	merged2 := b.Merge(a).(*GCounter)
	if merged2.Int64() != 12 {
		t.Fatalf("expected 12, got %d", merged2.Int64())
	}
}

func TestGCounter_MergeOverlap(t *testing.T) {
	// Both replicas know about each other, with different counts.
	a := &GCounter{replica: 1, counts: map[ReplicaID]uint64{1: 5, 2: 3}}
	b := &GCounter{replica: 2, counts: map[ReplicaID]uint64{1: 4, 2: 7}}

	merged := a.Merge(b).(*GCounter)
	// max(5,4)=5 for replica 1, max(3,7)=7 for replica 2 → total 12
	if merged.Int64() != 12 {
		t.Fatalf("expected 12, got %d", merged.Int64())
	}
}

func TestGCounter_MergeIdempotent(t *testing.T) {
	a := NewGCounter(1)
	a, _ = a.Increment(5)
	merged := a.Merge(a).(*GCounter)
	if merged.Int64() != a.Int64() {
		t.Fatal("merge with self should be idempotent")
	}
}

func TestGCounter_MergeDelta(t *testing.T) {
	a := NewGCounter(1)
	a, _ = a.Increment(5)

	b := NewGCounter(2)
	b, d := b.Increment(3)

	// Apply delta from b to a.
	merged := a.Merge(d.State).(*GCounter)
	if merged.Int64() != 8 {
		t.Fatalf("expected 8, got %d", merged.Int64())
	}
}

func TestGCounter_CRDTType(t *testing.T) {
	c := NewGCounter(1)
	if c.CRDTType() != TypeGCounter {
		t.Fatal("wrong type")
	}
}

func TestGCounter_MarshalUnmarshal(t *testing.T) {
	c := NewGCounter(1)
	c, _ = c.Increment(42)
	c, _ = c.Increment(8)

	data, err := c.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	c2 := &GCounter{}
	if err := c2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if c2.Int64() != 50 {
		t.Fatalf("expected 50, got %d", c2.Int64())
	}
	if c2.replica != 1 {
		t.Fatalf("expected replica 1, got %d", c2.replica)
	}
}

func TestGCounter_OriginalUnchangedAfterMerge(t *testing.T) {
	a := NewGCounter(1)
	a, _ = a.Increment(5)
	b := NewGCounter(2)
	b, _ = b.Increment(3)

	a.Merge(b)
	// a should still be 5.
	if a.Int64() != 5 {
		t.Fatal("merge should not modify receiver")
	}
}
