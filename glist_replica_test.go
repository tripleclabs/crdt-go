package crdt

import "testing"

func TestGListReplica_Append(t *testing.T) {
	r := NewGListReplica[string](1, StringCodec{})
	r.Data.Append("first", r.NextDot())
	r.Data.Append("second", r.NextDot())
	items, _ := r.Data.Items()
	if len(items) != 2 || items[0] != "first" || items[1] != "second" {
		t.Fatalf("expected [first second], got %v", items)
	}
}

func TestGListReplica_ApplyDelta(t *testing.T) {
	a := NewGListReplica[string](1, StringCodec{})
	b := NewGListReplica[string](2, StringCodec{})
	da, _ := a.Data.Append("from-a", a.NextDot())
	db, _ := b.Data.Append("from-b", b.NextDot())
	a.ApplyDelta(db)
	b.ApplyDelta(da)

	ia, _ := a.Data.Items()
	ib, _ := b.Data.Items()
	if len(ia) != 2 || len(ib) != 2 {
		t.Fatalf("expected both 2, got a=%d b=%d", len(ia), len(ib))
	}
}

func TestGListReplica_Idempotent(t *testing.T) {
	a := NewGListReplica[string](1, StringCodec{})
	b := NewGListReplica[string](2, StringCodec{})
	d, _ := a.Data.Append("x", a.NextDot())
	b.ApplyDelta(d)
	b.ApplyDelta(d)
	if b.Data.Len() != 1 {
		t.Fatalf("expected 1, got %d", b.Data.Len())
	}
}

func TestGListReplica_AntiEntropy(t *testing.T) {
	a := NewGListReplica[string](1, StringCodec{})
	b := NewGListReplica[string](2, StringCodec{})
	a.Data.Append("x", a.NextDot())
	a.Data.Append("y", a.NextDot())
	b.Data.Append("z", b.NextDot())

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

func TestGListReplica_CausalOrder(t *testing.T) {
	a := NewGListReplica[string](1, StringCodec{})
	b := NewGListReplica[string](2, StringCodec{})
	da, _ := a.Data.Append("a1", a.NextDot()) // dot {1,1}
	db, _ := b.Data.Append("b1", b.NextDot()) // dot {2,1}

	c := NewGListReplica[string](3, StringCodec{})
	c.ApplyDelta(db)
	c.ApplyDelta(da)
	items, _ := c.Data.Items()
	// Same counter, lower replica first.
	if items[0] != "a1" || items[1] != "b1" {
		t.Fatalf("expected [a1 b1], got %v", items)
	}
}
