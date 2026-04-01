package crdt

import "testing"

func TestAWLWWMapReplica_PutGet(t *testing.T) {
	r := NewAWLWWMapReplica[string](1, StringCodec{})
	_, err := r.Data.Put("name", "alice", r.NextDot())
	if err != nil {
		t.Fatal(err)
	}
	v, _, ok := r.Data.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
}

func TestAWLWWMapReplica_AddWins(t *testing.T) {
	a := NewAWLWWMapReplica[string](1, StringCodec{})
	putDelta, _ := a.Data.Put("k", "from-a", a.NextDot())

	b := NewAWLWWMapReplica[string](2, StringCodec{})
	b.Data.Put("k", "temp", b.NextDot())
	removeDelta := b.Data.Remove("k", b.NextDot(), b.HWM())

	// c: put from a should survive b's remove (add-wins).
	c := NewAWLWWMapReplica[string](3, StringCodec{})
	c.ApplyDelta(removeDelta)
	c.ApplyDelta(putDelta)
	v, _, ok := c.Data.Get("k")
	if !ok || v != "from-a" {
		t.Fatalf("add-wins: expected from-a, got %v ok=%v", v, ok)
	}
}

func TestAWLWWMapReplica_ObservedRemove(t *testing.T) {
	a := NewAWLWWMapReplica[string](1, StringCodec{})
	putDelta, _ := a.Data.Put("k", "val", a.NextDot())

	b := NewAWLWWMapReplica[string](2, StringCodec{})
	b.ApplyDelta(putDelta) // b has seen a's dot
	removeDelta := b.Data.Remove("k", b.NextDot(), b.HWM())

	a.ApplyDelta(removeDelta) // remove context covers a's dot
	_, _, ok := a.Data.Get("k")
	if ok {
		t.Fatal("observed remove: key should be gone")
	}
}

func TestAWLWWMapReplica_AntiEntropy(t *testing.T) {
	a := NewAWLWWMapReplica[string](1, StringCodec{})
	b := NewAWLWWMapReplica[string](2, StringCodec{})
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
