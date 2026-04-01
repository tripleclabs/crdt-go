package crdt

import (
	"sort"
	"testing"
)

func TestORSetReplica_Add(t *testing.T) {
	r := NewORSetReplica[string](1, StringCodec{})
	delta, err := r.Data.Add("alice", r.NextDot())
	if err != nil {
		t.Fatal(err)
	}
	if len(delta) == 0 {
		t.Fatal("expected non-empty delta")
	}
	if !r.Data.Contains("alice") || r.Data.Len() != 1 {
		t.Fatal("expected alice in set")
	}
}

func TestORSetReplica_Remove(t *testing.T) {
	r := NewORSetReplica[string](1, StringCodec{})
	r.Data.Add("alice", r.NextDot())
	r.Data.Add("bob", r.NextDot())
	r.Data.Remove("alice", r.HWM())
	if r.Data.Contains("alice") {
		t.Fatal("alice should be removed")
	}
	if !r.Data.Contains("bob") {
		t.Fatal("bob should remain")
	}
}

func TestORSetReplica_ApplyDelta_Add(t *testing.T) {
	a := NewORSetReplica[string](1, StringCodec{})
	b := NewORSetReplica[string](2, StringCodec{})

	d, _ := a.Data.Add("x", a.NextDot())
	b.ApplyDelta(d)
	if !b.Data.Contains("x") {
		t.Fatal("b should contain x")
	}
}

func TestORSetReplica_ApplyDelta_AddWins(t *testing.T) {
	// a adds "x". b adds "x" then removes. a's add should survive
	// because its dot is not covered by b's remove context.
	a := NewORSetReplica[string](1, StringCodec{})
	addA, _ := a.Data.Add("x", a.NextDot())

	b := NewORSetReplica[string](2, StringCodec{})
	b.Data.Add("x", b.NextDot())
	removeB, _ := b.Data.Remove("x", b.HWM())

	// c applies remove first, then add.
	c := NewORSetReplica[string](3, StringCodec{})
	c.ApplyDelta(removeB)
	c.ApplyDelta(addA)
	if !c.Data.Contains("x") {
		t.Fatal("add-wins: x should survive concurrent remove")
	}
}

func TestORSetReplica_ApplyDelta_ObservedRemove(t *testing.T) {
	// a adds "x", sends to b. b removes "x" (has seen a's dot).
	a := NewORSetReplica[string](1, StringCodec{})
	addA, _ := a.Data.Add("x", a.NextDot())

	b := NewORSetReplica[string](2, StringCodec{})
	b.ApplyDelta(addA) // b now has a's dot in received
	removeB, _ := b.Data.Remove("x", b.HWM())

	// a applies b's remove. Since b's context covers a's dot, x is gone.
	a.ApplyDelta(removeB)
	if a.Data.Contains("x") {
		t.Fatal("observed remove: x should be gone")
	}
}

func TestORSetReplica_Convergence(t *testing.T) {
	a := NewORSetReplica[string](1, StringCodec{})
	b := NewORSetReplica[string](2, StringCodec{})

	da, _ := a.Data.Add("from-a", a.NextDot())
	db, _ := b.Data.Add("from-b", b.NextDot())

	a.ApplyDelta(db)
	b.ApplyDelta(da)

	ea, _ := a.Data.Elements()
	eb, _ := b.Data.Elements()
	sort.Strings(ea)
	sort.Strings(eb)

	if len(ea) != 2 || ea[0] != "from-a" || ea[1] != "from-b" {
		t.Fatalf("a: expected [from-a from-b], got %v", ea)
	}
	if len(eb) != 2 || eb[0] != "from-a" || eb[1] != "from-b" {
		t.Fatalf("b: expected [from-a from-b], got %v", eb)
	}
}

func TestORSetReplica_AntiEntropy(t *testing.T) {
	a := NewORSetReplica[string](1, StringCodec{})
	b := NewORSetReplica[string](2, StringCodec{})

	a.Data.Add("x", a.NextDot())
	a.Data.Add("y", a.NextDot())
	b.Data.Add("z", b.NextDot())

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

func TestORSetReplica_Idempotent(t *testing.T) {
	a := NewORSetReplica[string](1, StringCodec{})
	b := NewORSetReplica[string](2, StringCodec{})
	d, _ := a.Data.Add("x", a.NextDot())
	b.ApplyDelta(d)
	b.ApplyDelta(d)
	if b.Data.Len() != 1 {
		t.Fatalf("expected 1, got %d", b.Data.Len())
	}
}
