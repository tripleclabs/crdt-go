package crdt

import "testing"

func TestLWWMapReplica_Put(t *testing.T) {
	r := NewLWWMapReplica[string](1, StringCodec{})
	delta, err := r.Data.Put("name", "alice", r.NextDot())
	if err != nil {
		t.Fatal(err)
	}
	if len(delta) == 0 {
		t.Fatal("expected non-empty delta")
	}
	v, _, ok := r.Data.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
}

func TestLWWMapReplica_Remove(t *testing.T) {
	r := NewLWWMapReplica[string](1, StringCodec{})
	r.Data.Put("key", "val", r.NextDot())
	delta := r.Data.Remove("key", r.NextDot())
	if len(delta) == 0 {
		t.Fatal("expected non-empty delta")
	}
	_, _, ok := r.Data.Get("key")
	if ok {
		t.Fatal("key should be removed")
	}
}

func TestLWWMapReplica_ApplyDelta_Put(t *testing.T) {
	a := NewLWWMapReplica[string](1, StringCodec{})
	b := NewLWWMapReplica[string](2, StringCodec{})

	delta, _ := a.Data.Put("x", "from-a", a.NextDot())
	b.ApplyDelta(delta)

	v, _, ok := b.Data.Get("x")
	if !ok || v != "from-a" {
		t.Fatalf("expected from-a, got %v", v)
	}
}

func TestLWWMapReplica_ApplyDelta_HigherDotWins(t *testing.T) {
	a := NewLWWMapReplica[string](1, StringCodec{})
	a.Data.Put("k", "a1", a.NextDot())                    // dot {1,1}
	deltaA, _ := a.Data.Put("k", "a2", a.NextDot()) // dot {1,2}

	b := NewLWWMapReplica[string](2, StringCodec{})
	deltaB, _ := b.Data.Put("k", "b1", b.NextDot()) // dot {2,1}

	// c applies both — a2 at {1,2} beats b1 at {2,1} (higher counter).
	c := NewLWWMapReplica[string](3, StringCodec{})
	c.ApplyDelta(deltaB)
	c.ApplyDelta(deltaA)

	v, _, _ := c.Data.Get("k")
	if v != "a2" {
		t.Fatalf("expected a2 (higher counter), got %v", v)
	}
}

func TestLWWMapReplica_ApplyDelta_Idempotent(t *testing.T) {
	a := NewLWWMapReplica[string](1, StringCodec{})
	b := NewLWWMapReplica[string](2, StringCodec{})

	delta, _ := a.Data.Put("k", "val", a.NextDot())
	b.ApplyDelta(delta)
	b.ApplyDelta(delta)

	if b.Data.Len() != 1 {
		t.Fatalf("expected 1, got %d", b.Data.Len())
	}
}

func TestLWWMapReplica_ApplyDelta_Remove(t *testing.T) {
	a := NewLWWMapReplica[string](1, StringCodec{})
	a.Data.Put("k", "val", a.NextDot())             // dot {1,1}
	removeDelta := a.Data.Remove("k", a.NextDot()) // dot {1,2}

	b := NewLWWMapReplica[string](2, StringCodec{})
	putDelta, _ := b.Data.Put("k", "b-val", b.NextDot()) // dot {2,1}

	// c applies put then remove. Remove at {1,2} beats put at {2,1}.
	c := NewLWWMapReplica[string](3, StringCodec{})
	c.ApplyDelta(putDelta)
	c.ApplyDelta(removeDelta)

	_, _, ok := c.Data.Get("k")
	if ok {
		t.Fatal("remove with higher counter should win")
	}
}

func TestLWWMapReplica_DeltasSince(t *testing.T) {
	r := NewLWWMapReplica[string](1, StringCodec{})
	r.Data.Put("a", "1", r.NextDot())
	r.Data.Put("b", "2", r.NextDot())

	deltas := r.DeltasSince(VClock{})
	if len(deltas) != 2 {
		t.Fatalf("expected 2 deltas, got %d", len(deltas))
	}

	deltas = r.DeltasSince(VClock{1: 2})
	if len(deltas) != 0 {
		t.Fatalf("expected 0 deltas, got %d", len(deltas))
	}

	deltas = r.DeltasSince(VClock{1: 1})
	if len(deltas) != 1 {
		t.Fatalf("expected 1 delta, got %d", len(deltas))
	}
}

func TestLWWMapReplica_Convergence(t *testing.T) {
	a := NewLWWMapReplica[string](1, StringCodec{})
	b := NewLWWMapReplica[string](2, StringCodec{})
	c := NewLWWMapReplica[string](3, StringCodec{})

	da, _ := a.Data.Put("x", "from-a", a.NextDot())
	db, _ := b.Data.Put("y", "from-b", b.NextDot())
	dc, _ := c.Data.Put("z", "from-c", c.NextDot())

	allDeltas := [][]byte{da, db, dc}
	for _, r := range []*Replica[*LWWMap[string]]{a, b, c} {
		for _, d := range allDeltas {
			r.ApplyDelta(d)
		}
	}

	if a.Data.Len() != 3 || b.Data.Len() != 3 || c.Data.Len() != 3 {
		t.Fatalf("expected all 3, got a=%d b=%d c=%d",
			a.Data.Len(), b.Data.Len(), c.Data.Len())
	}
}

func TestLWWMapReplica_AntiEntropy(t *testing.T) {
	a := NewLWWMapReplica[string](1, StringCodec{})
	b := NewLWWMapReplica[string](2, StringCodec{})

	a.Data.Put("x", "from-a", a.NextDot())
	a.Data.Put("y", "from-a2", a.NextDot())
	b.Data.Put("z", "from-b", b.NextDot())

	for _, d := range a.DeltasSince(b.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.HWM()) {
		a.ApplyDelta(d)
	}

	if a.Data.Len() != 3 || b.Data.Len() != 3 {
		t.Fatalf("expected both 3, got a=%d b=%d", a.Data.Len(), b.Data.Len())
	}
}

func TestLWWMapReplica_ReceivedClock(t *testing.T) {
	a := NewLWWMapReplica[string](1, StringCodec{})
	a.Data.Put("x", "1", a.NextDot())
	a.Data.Put("y", "2", a.NextDot())
	a.Data.Put("z", "3", a.NextDot())

	if a.Received.Get(1) != 3 {
		t.Fatalf("expected received hwm 3, got %d", a.Received.Get(1))
	}

	// b receives out of order.
	b := NewLWWMapReplica[string](2, StringCodec{})
	deltas := a.DeltasSince(b.HWM())
	for i := len(deltas) - 1; i >= 0; i-- {
		b.ApplyDelta(deltas[i])
	}

	if b.Received.Get(1) != 3 {
		t.Fatalf("expected received hwm 3 after out-of-order, got %d", b.Received.Get(1))
	}
}

func TestLWWMapReplica_LocalClock_NotAdvancedByDelta(t *testing.T) {
	a := NewLWWMapReplica[string](1, StringCodec{})
	b := NewLWWMapReplica[string](2, StringCodec{})

	delta, _ := a.Data.Put("x", "val", a.NextDot())
	b.ApplyDelta(delta)

	if b.Local.Counter() != 0 {
		t.Fatalf("local clock should not advance from delta, got %d", b.Local.Counter())
	}
	if b.Received.Get(1) != 1 {
		t.Fatalf("received should track it, got %d", b.Received.Get(1))
	}
}

func TestLWWMapReplica_GapFill(t *testing.T) {
	a := NewLWWMapReplica[string](1, StringCodec{})
	d1, _ := a.Data.Put("a", "1", a.NextDot())
	a.Data.Put("b", "2", a.NextDot()) // d2 is "lost"
	d3, _ := a.Data.Put("c", "3", a.NextDot())

	b := NewLWWMapReplica[string](2, StringCodec{})
	b.ApplyDelta(d1)
	b.ApplyDelta(d3)

	// hwm should be 1 (gap at 2).
	if b.Received.Get(1) != 1 {
		t.Fatalf("expected hwm 1, got %d", b.Received.Get(1))
	}

	// Anti-entropy fills the gap.
	for _, d := range a.DeltasSince(b.HWM()) {
		b.ApplyDelta(d)
	}

	if b.Received.Get(1) != 3 {
		t.Fatalf("expected hwm 3 after gap fill, got %d", b.Received.Get(1))
	}
	if b.Data.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", b.Data.Len())
	}
}

func TestLWWMapReplica_FiveNodes(t *testing.T) {
	replicas := make([]*Replica[*LWWMap[string]], 5)
	for i := range replicas {
		replicas[i] = NewLWWMapReplica[string](ReplicaID(i+1), StringCodec{})
	}

	// Each node does 3 ops.
	var allDeltas [][]byte
	for _, r := range replicas {
		for j := 0; j < 3; j++ {
			d, _ := r.Data.Put(
				string(rune('a'+int(r.Local.Replica())*3+j)),
				string(rune('A'+int(r.Local.Replica())*3+j)),
				r.NextDot(),
			)
			allDeltas = append(allDeltas, d)
		}
	}

	// Apply all deltas to all replicas.
	for _, r := range replicas {
		for _, d := range allDeltas {
			r.ApplyDelta(d)
		}
	}

	// All should have 15 entries.
	for i, r := range replicas {
		if r.Data.Len() != 15 {
			t.Fatalf("replica %d: expected 15, got %d", i+1, r.Data.Len())
		}
	}

	// All received clocks should have hwm 3 for each replica.
	for i, r := range replicas {
		for j := 1; j <= 5; j++ {
			if r.Received.Get(ReplicaID(j)) != 3 {
				t.Fatalf("replica %d: expected received[%d]=3, got %d",
					i+1, j, r.Received.Get(ReplicaID(j)))
			}
		}
	}
}
