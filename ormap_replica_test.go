package crdt

import "testing"

func TestORMapReplica_PutGet(t *testing.T) {
	r := NewORMapReplica[string](1, StringCodec{})
	r.Data.Put("name", "alice", r.NextDot())
	v, _, ok := r.Data.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
}

func TestORMapReplica_ApplyDelta(t *testing.T) {
	a := NewORMapReplica[string](1, StringCodec{})
	b := NewORMapReplica[string](2, StringCodec{})
	da, _ := a.Data.Put("x", "from-a", a.NextDot())
	db, _ := b.Data.Put("y", "from-b", b.NextDot())
	a.ApplyDelta(db)
	b.ApplyDelta(da)
	if a.Data.Len() != 2 || b.Data.Len() != 2 {
		t.Fatalf("expected both 2, got a=%d b=%d", a.Data.Len(), b.Data.Len())
	}
}

func TestORMapReplica_AddWins(t *testing.T) {
	a := NewORMapReplica[string](1, StringCodec{})
	putDelta, _ := a.Data.Put("k", "val", a.NextDot())

	b := NewORMapReplica[string](2, StringCodec{})
	b.Data.Put("k", "b-val", b.NextDot())
	removeDelta := b.Data.Remove("k", b.HWM())

	c := NewORMapReplica[string](3, StringCodec{})
	c.ApplyDelta(removeDelta)
	c.ApplyDelta(putDelta) // a's add should survive — dot not in remove context
	if c.Data.Len() != 1 {
		t.Fatal("add-wins: key should survive")
	}
}

func TestORMapReplica_AntiEntropy(t *testing.T) {
	a := NewORMapReplica[string](1, StringCodec{})
	b := NewORMapReplica[string](2, StringCodec{})
	a.Data.Put("x", "from-a", a.NextDot())
	b.Data.Put("y", "from-b", b.NextDot())

	for _, d := range a.DeltasSince(b.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.HWM()) {
		a.ApplyDelta(d)
	}
	if a.Data.Len() != 2 || b.Data.Len() != 2 {
		t.Fatalf("expected both 2, got a=%d b=%d", a.Data.Len(), b.Data.Len())
	}
}

func TestORMapReplica_DeltasSince(t *testing.T) {
	r := NewORMapReplica[string](1, StringCodec{})
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

func TestORMapReplica_ObservedRemove(t *testing.T) {
	a := NewORMapReplica[string](1, StringCodec{})
	putDelta, _ := a.Data.Put("k", "val", a.NextDot())

	b := NewORMapReplica[string](2, StringCodec{})
	b.ApplyDelta(putDelta) // b has seen a's dot
	removeDelta := b.Data.Remove("k", b.HWM())

	a.ApplyDelta(removeDelta) // remove context covers a's dot
	_, _, ok := a.Data.Get("k")
	if ok {
		t.Fatal("observed remove: key should be gone")
	}
}

func TestORMapReplica_Convergence(t *testing.T) {
	a := NewORMapReplica[string](1, StringCodec{})
	b := NewORMapReplica[string](2, StringCodec{})
	c := NewORMapReplica[string](3, StringCodec{})

	da, _ := a.Data.Put("x", "from-a", a.NextDot())
	db, _ := b.Data.Put("y", "from-b", b.NextDot())
	dc, _ := c.Data.Put("z", "from-c", c.NextDot())

	allDeltas := [][]byte{da, db, dc}
	for _, r := range []*Replica[*ORMap[string]]{a, b, c} {
		for _, d := range allDeltas {
			r.ApplyDelta(d)
		}
	}

	if a.Data.Len() != 3 || b.Data.Len() != 3 || c.Data.Len() != 3 {
		t.Fatalf("expected all 3, got a=%d b=%d c=%d",
			a.Data.Len(), b.Data.Len(), c.Data.Len())
	}
}

func TestORMapReplica_ReceivedClock(t *testing.T) {
	a := NewORMapReplica[string](1, StringCodec{})
	a.Data.Put("x", "1", a.NextDot())
	a.Data.Put("y", "2", a.NextDot())
	a.Data.Put("z", "3", a.NextDot())

	if a.Received.Get(1) != 3 {
		t.Fatalf("expected received hwm 3, got %d", a.Received.Get(1))
	}

	// b receives out of order.
	b := NewORMapReplica[string](2, StringCodec{})
	deltas := a.DeltasSince(b.HWM())
	for i := len(deltas) - 1; i >= 0; i-- {
		b.ApplyDelta(deltas[i])
	}

	if b.Received.Get(1) != 3 {
		t.Fatalf("expected received hwm 3 after out-of-order, got %d", b.Received.Get(1))
	}
}
