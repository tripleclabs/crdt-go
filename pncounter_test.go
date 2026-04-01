package crdt

import "testing"

func TestNewPNCounter(t *testing.T) {
	c := NewPNCounter(1)
	if c.Int64() != 0 {
		t.Fatal("new counter should be 0")
	}
}

func TestPNCounter_Increment(t *testing.T) {
	c := NewPNCounter(1)
	c2, d := c.Increment(5)
	if c2.Int64() != 5 {
		t.Fatalf("expected 5, got %d", c2.Int64())
	}
	if c.Int64() != 0 {
		t.Fatal("original should not be modified")
	}
	if d.Type != TypePNCounter {
		t.Fatal("wrong delta type")
	}
}

func TestPNCounter_Decrement(t *testing.T) {
	c := NewPNCounter(1)
	c, _ = c.Increment(10)
	c, _ = c.Decrement(3)
	if c.Int64() != 7 {
		t.Fatalf("expected 7, got %d", c.Int64())
	}
}

func TestPNCounter_DecrementBelowZero(t *testing.T) {
	c := NewPNCounter(1)
	c, _ = c.Decrement(5)
	if c.Int64() != -5 {
		t.Fatalf("expected -5, got %d", c.Int64())
	}
}

func TestPNCounter_Value(t *testing.T) {
	c := NewPNCounter(1)
	c, _ = c.Increment(10)
	c, _ = c.Decrement(3)
	if c.Value().(int64) != 7 {
		t.Fatal("Value() should return 7")
	}
}

func TestPNCounter_VClock(t *testing.T) {
	c := NewPNCounter(1)
	c, _ = c.Increment(5)
	c, _ = c.Decrement(3)
	vc := c.VClock()
	// Total ops for replica 1 = 5 + 3 = 8
	if vc.Get(1) != 8 {
		t.Fatalf("expected vclock[1]=8, got %d", vc.Get(1))
	}
}

func TestPNCounter_Merge(t *testing.T) {
	a := NewPNCounter(1)
	a, _ = a.Increment(10)
	a, _ = a.Decrement(2) // value = 8

	b := NewPNCounter(2)
	b, _ = b.Increment(5)
	b, _ = b.Decrement(1) // value = 4

	merged := a.Merge(b).(*PNCounter)
	// pos: max(10,0) + max(0,5) = 15, neg: max(2,0) + max(0,1) = 3 → 12
	if merged.Int64() != 12 {
		t.Fatalf("expected 12, got %d", merged.Int64())
	}

	// Commutative.
	merged2 := b.Merge(a).(*PNCounter)
	if merged2.Int64() != 12 {
		t.Fatalf("commutative: expected 12, got %d", merged2.Int64())
	}
}

func TestPNCounter_MergeOverlap(t *testing.T) {
	a := &PNCounter{
		replica:  1,
		positive: map[ReplicaID]uint64{1: 10, 2: 3},
		negative: map[ReplicaID]uint64{1: 2, 2: 1},
	}
	b := &PNCounter{
		replica:  2,
		positive: map[ReplicaID]uint64{1: 8, 2: 5},
		negative: map[ReplicaID]uint64{1: 4, 2: 0},
	}

	merged := a.Merge(b).(*PNCounter)
	// pos: max(10,8)=10, max(3,5)=5 → 15
	// neg: max(2,4)=4, max(1,0)=1 → 5
	// value = 15 - 5 = 10
	if merged.Int64() != 10 {
		t.Fatalf("expected 10, got %d", merged.Int64())
	}
}

func TestPNCounter_MergeIdempotent(t *testing.T) {
	c := NewPNCounter(1)
	c, _ = c.Increment(5)
	c, _ = c.Decrement(2)
	merged := c.Merge(c).(*PNCounter)
	if merged.Int64() != c.Int64() {
		t.Fatal("merge with self should be idempotent")
	}
}

func TestPNCounter_MergeDelta(t *testing.T) {
	a := NewPNCounter(1)
	a, _ = a.Increment(5)

	b := NewPNCounter(2)
	_, d := b.Increment(3)

	merged := a.Merge(d.State).(*PNCounter)
	if merged.Int64() != 8 {
		t.Fatalf("expected 8, got %d", merged.Int64())
	}
}

func TestPNCounter_CRDTType(t *testing.T) {
	c := NewPNCounter(1)
	if c.CRDTType() != TypePNCounter {
		t.Fatal("wrong type")
	}
}

func TestPNCounter_MarshalUnmarshal(t *testing.T) {
	c := NewPNCounter(1)
	c, _ = c.Increment(10)
	c, _ = c.Decrement(3)

	data, err := c.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	c2 := &PNCounter{}
	if err := c2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if c2.Int64() != 7 {
		t.Fatalf("expected 7, got %d", c2.Int64())
	}
	if c2.replica != 1 {
		t.Fatalf("expected replica 1, got %d", c2.replica)
	}
}

func TestPNCounter_OriginalUnchangedAfterMerge(t *testing.T) {
	a := NewPNCounter(1)
	a, _ = a.Increment(5)
	b := NewPNCounter(2)
	b, _ = b.Increment(3)

	a.Merge(b)
	if a.Int64() != 5 {
		t.Fatal("merge should not modify receiver")
	}
}
