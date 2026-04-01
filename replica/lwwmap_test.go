package replica

import (
	"testing"

	"github.com/3clabs/crdt"
)

func TestLWWMapReplica_Put(t *testing.T) {
	r := NewLWWMap[string](1, crdt.StringCodec{})
	delta, err := r.Put("name", "alice")
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
	r := NewLWWMap[string](1, crdt.StringCodec{})
	r.Put("key", "val")
	delta := r.Remove("key")
	if len(delta) == 0 {
		t.Fatal("expected non-empty delta")
	}
	_, _, ok := r.Data.Get("key")
	if ok {
		t.Fatal("key should be removed")
	}
}

func TestLWWMapReplica_ApplyDelta_Put(t *testing.T) {
	a := NewLWWMap[string](1, crdt.StringCodec{})
	b := NewLWWMap[string](2, crdt.StringCodec{})

	delta, _ := a.Put("x", "from-a")
	b.ApplyDelta(delta)

	v, _, ok := b.Data.Get("x")
	if !ok || v != "from-a" {
		t.Fatalf("expected from-a, got %v", v)
	}
}

func TestLWWMapReplica_ApplyDelta_HigherDotWins(t *testing.T) {
	a := NewLWWMap[string](1, crdt.StringCodec{})
	a.Put("k", "a1")         // dot {1,1}
	deltaA, _ := a.Put("k", "a2") // dot {1,2}

	b := NewLWWMap[string](2, crdt.StringCodec{})
	deltaB, _ := b.Put("k", "b1") // dot {2,1}

	// c applies both — a2 at {1,2} beats b1 at {2,1} (higher counter).
	c := NewLWWMap[string](3, crdt.StringCodec{})
	c.ApplyDelta(deltaB)
	c.ApplyDelta(deltaA)

	v, _, _ := c.Data.Get("k")
	if v != "a2" {
		t.Fatalf("expected a2 (higher counter), got %v", v)
	}
}

func TestLWWMapReplica_ApplyDelta_Idempotent(t *testing.T) {
	a := NewLWWMap[string](1, crdt.StringCodec{})
	b := NewLWWMap[string](2, crdt.StringCodec{})

	delta, _ := a.Put("k", "val")
	b.ApplyDelta(delta)
	b.ApplyDelta(delta)

	if b.Data.Len() != 1 {
		t.Fatalf("expected 1, got %d", b.Data.Len())
	}
}

func TestLWWMapReplica_ApplyDelta_Remove(t *testing.T) {
	a := NewLWWMap[string](1, crdt.StringCodec{})
	a.Put("k", "val")          // dot {1,1}
	removeDelta := a.Remove("k") // dot {1,2}

	b := NewLWWMap[string](2, crdt.StringCodec{})
	putDelta, _ := b.Put("k", "b-val") // dot {2,1}

	// c applies put then remove. Remove at {1,2} beats put at {2,1}.
	c := NewLWWMap[string](3, crdt.StringCodec{})
	c.ApplyDelta(putDelta)
	c.ApplyDelta(removeDelta)

	_, _, ok := c.Data.Get("k")
	if ok {
		t.Fatal("remove with higher counter should win")
	}
}

func TestLWWMapReplica_DeltasSince(t *testing.T) {
	r := NewLWWMap[string](1, crdt.StringCodec{})
	r.Put("a", "1")
	r.Put("b", "2")

	deltas := r.DeltasSince(crdt.VClock{})
	if len(deltas) != 2 {
		t.Fatalf("expected 2 deltas, got %d", len(deltas))
	}

	deltas = r.DeltasSince(crdt.VClock{1: 2})
	if len(deltas) != 0 {
		t.Fatalf("expected 0 deltas, got %d", len(deltas))
	}

	deltas = r.DeltasSince(crdt.VClock{1: 1})
	if len(deltas) != 1 {
		t.Fatalf("expected 1 delta, got %d", len(deltas))
	}
}

func TestLWWMapReplica_Convergence(t *testing.T) {
	a := NewLWWMap[string](1, crdt.StringCodec{})
	b := NewLWWMap[string](2, crdt.StringCodec{})
	c := NewLWWMap[string](3, crdt.StringCodec{})

	da, _ := a.Put("x", "from-a")
	db, _ := b.Put("y", "from-b")
	dc, _ := c.Put("z", "from-c")

	allDeltas := [][]byte{da, db, dc}
	for _, r := range []*LWWMapReplica[string]{a, b, c} {
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
	a := NewLWWMap[string](1, crdt.StringCodec{})
	b := NewLWWMap[string](2, crdt.StringCodec{})

	a.Put("x", "from-a")
	a.Put("y", "from-a2")
	b.Put("z", "from-b")

	for _, d := range a.DeltasSince(b.Received.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.Received.HWM()) {
		a.ApplyDelta(d)
	}

	if a.Data.Len() != 3 || b.Data.Len() != 3 {
		t.Fatalf("expected both 3, got a=%d b=%d", a.Data.Len(), b.Data.Len())
	}
}

func TestLWWMapReplica_ReceivedClock(t *testing.T) {
	a := NewLWWMap[string](1, crdt.StringCodec{})
	a.Put("x", "1")
	a.Put("y", "2")
	a.Put("z", "3")

	if a.Received.Get(1) != 3 {
		t.Fatalf("expected received hwm 3, got %d", a.Received.Get(1))
	}

	// b receives out of order.
	b := NewLWWMap[string](2, crdt.StringCodec{})
	deltas := a.DeltasSince(b.Received.HWM())
	for i := len(deltas) - 1; i >= 0; i-- {
		b.ApplyDelta(deltas[i])
	}

	if b.Received.Get(1) != 3 {
		t.Fatalf("expected received hwm 3 after out-of-order, got %d", b.Received.Get(1))
	}
}

func TestLWWMapReplica_LocalClock_NotAdvancedByDelta(t *testing.T) {
	a := NewLWWMap[string](1, crdt.StringCodec{})
	b := NewLWWMap[string](2, crdt.StringCodec{})

	delta, _ := a.Put("x", "val")
	b.ApplyDelta(delta)

	if b.Clock.Counter() != 0 {
		t.Fatalf("local clock should not advance from delta, got %d", b.Clock.Counter())
	}
	if b.Received.Get(1) != 1 {
		t.Fatalf("received should track it, got %d", b.Received.Get(1))
	}
}

func TestLWWMapReplica_GapFill(t *testing.T) {
	a := NewLWWMap[string](1, crdt.StringCodec{})
	d1, _ := a.Put("a", "1")
	_, _ = a.Put("b", "2") // d2 is "lost"
	d3, _ := a.Put("c", "3")

	b := NewLWWMap[string](2, crdt.StringCodec{})
	b.ApplyDelta(d1)
	b.ApplyDelta(d3)

	// hwm should be 1 (gap at 2).
	if b.Received.Get(1) != 1 {
		t.Fatalf("expected hwm 1, got %d", b.Received.Get(1))
	}

	// Anti-entropy fills the gap.
	for _, d := range a.DeltasSince(b.Received.HWM()) {
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
	replicas := make([]*LWWMapReplica[string], 5)
	for i := range replicas {
		replicas[i] = NewLWWMap[string](crdt.ReplicaID(i+1), crdt.StringCodec{})
	}

	// Each node does 3 ops.
	var allDeltas [][]byte
	for i, r := range replicas {
		for j := 0; j < 3; j++ {
			d, _ := r.Put(
				string(rune('a'+i*3+j)),
				string(rune('A'+i*3+j)),
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
			if r.Received.Get(crdt.ReplicaID(j)) != 3 {
				t.Fatalf("replica %d: expected received[%d]=3, got %d",
					i+1, j, r.Received.Get(crdt.ReplicaID(j)))
			}
		}
	}
}
