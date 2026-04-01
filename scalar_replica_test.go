package crdt

import (
	"sort"
	"testing"
)

// --- GCounter ---

func TestGCounterReplica_Increment(t *testing.T) {
	r := NewGCounterReplica(1)
	delta := r.Data.Increment(r.Local.Replica(), 5)
	r.Local.SetCounter(r.Data.Get(r.Local.Replica()))
	r.Received.Record(r.Local.Replica(), r.Data.Get(r.Local.Replica()))
	if r.Data.Int64() != 5 {
		t.Fatalf("expected 5, got %d", r.Data.Int64())
	}
	if len(delta) != 16 {
		t.Fatalf("expected 16 byte delta, got %d", len(delta))
	}
}

func TestGCounterReplica_ApplyDelta(t *testing.T) {
	a := NewGCounterReplica(1)
	b := NewGCounterReplica(2)
	da := a.Data.Increment(a.Local.Replica(), 5)
	a.Received.Record(1, 5)
	db := b.Data.Increment(b.Local.Replica(), 3)
	b.Received.Record(2, 3)

	a.ApplyDelta(db)
	b.ApplyDelta(da)

	if a.Data.Int64() != 8 || b.Data.Int64() != 8 {
		t.Fatalf("expected both 8, got a=%d b=%d", a.Data.Int64(), b.Data.Int64())
	}
}

func TestGCounterReplica_Idempotent(t *testing.T) {
	a := NewGCounterReplica(1)
	b := NewGCounterReplica(2)
	d := a.Data.Increment(a.Local.Replica(), 5)
	a.Received.Record(1, 5)
	b.ApplyDelta(d)
	b.ApplyDelta(d)
	if b.Data.Int64() != 5 {
		t.Fatalf("expected 5, got %d", b.Data.Int64())
	}
}

func TestGCounterReplica_AntiEntropy(t *testing.T) {
	a := NewGCounterReplica(1)
	b := NewGCounterReplica(2)
	a.Data.Increment(a.Local.Replica(), 10)
	a.Received.Record(1, 10)
	b.Data.Increment(b.Local.Replica(), 7)
	b.Received.Record(2, 7)

	for _, d := range a.DeltasSince(b.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.HWM()) {
		a.ApplyDelta(d)
	}

	if a.Data.Int64() != 17 || b.Data.Int64() != 17 {
		t.Fatalf("expected both 17, got a=%d b=%d", a.Data.Int64(), b.Data.Int64())
	}
}

func TestGCounterReplica_FiveNodes(t *testing.T) {
	replicas := make([]*Replica[*GCounter], 5)
	var allDeltas [][]byte
	for i := range replicas {
		rid := ReplicaID(i + 1)
		replicas[i] = NewGCounterReplica(rid)
		d := replicas[i].Data.Increment(rid, uint64(10*(i+1)))
		replicas[i].Received.Record(rid, uint64(10*(i+1)))
		allDeltas = append(allDeltas, d)
	}

	for _, r := range replicas {
		for _, d := range allDeltas {
			r.ApplyDelta(d)
		}
	}

	for i, r := range replicas {
		if r.Data.Int64() != 150 {
			t.Fatalf("replica %d: expected 150, got %d", i+1, r.Data.Int64())
		}
	}
}

// --- PNCounter ---

func TestPNCounterReplica_IncrementDecrement(t *testing.T) {
	r := NewPNCounterReplica(1)
	r.Data.Increment(r.Local.Replica(), 10)
	r.Data.Decrement(r.Local.Replica(), 3)
	if r.Data.Int64() != 7 {
		t.Fatalf("expected 7, got %d", r.Data.Int64())
	}
}

func TestPNCounterReplica_ApplyDelta(t *testing.T) {
	a := NewPNCounterReplica(1)
	b := NewPNCounterReplica(2)
	dInc := a.Data.Increment(a.Local.Replica(), 10)
	dDec := a.Data.Decrement(a.Local.Replica(), 2)
	db := b.Data.Increment(b.Local.Replica(), 5)

	a.ApplyDelta(db)
	b.ApplyDelta(dInc)
	b.ApplyDelta(dDec)

	if a.Data.Int64() != 13 || b.Data.Int64() != 13 {
		t.Fatalf("expected both 13, got a=%d b=%d", a.Data.Int64(), b.Data.Int64())
	}
}

func TestPNCounterReplica_Idempotent(t *testing.T) {
	a := NewPNCounterReplica(1)
	b := NewPNCounterReplica(2)
	d := a.Data.Increment(a.Local.Replica(), 5)
	b.ApplyDelta(d)
	b.ApplyDelta(d)
	if b.Data.Int64() != 5 {
		t.Fatalf("expected 5, got %d", b.Data.Int64())
	}
}

func TestPNCounterReplica_AntiEntropy(t *testing.T) {
	a := NewPNCounterReplica(1)
	b := NewPNCounterReplica(2)
	a.Data.Increment(a.Local.Replica(), 10)
	a.Received.Record(1, 10)
	a.Data.Decrement(a.Local.Replica(), 3)
	a.Received.Record(1, 13) // combined pos+neg
	b.Data.Increment(b.Local.Replica(), 5)
	b.Received.Record(2, 5)

	for _, d := range a.DeltasSince(b.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.HWM()) {
		a.ApplyDelta(d)
	}
	if a.Data.Int64() != b.Data.Int64() {
		t.Fatalf("expected convergence, got a=%d b=%d", a.Data.Int64(), b.Data.Int64())
	}
}

// --- LWWRegister ---

func TestLWWRegisterReplica_Set(t *testing.T) {
	r := NewLWWRegisterReplica[string](1, StringCodec{})
	delta, err := r.Data.Set("hello", r.NextDot())
	if err != nil {
		t.Fatal(err)
	}
	if len(delta) == 0 {
		t.Fatal("expected delta")
	}
	v, _, ok := r.Data.Get()
	if !ok || v != "hello" {
		t.Fatalf("expected hello, got %v", v)
	}
}

func TestLWWRegisterReplica_ApplyDelta(t *testing.T) {
	a := NewLWWRegisterReplica[string](1, StringCodec{})
	b := NewLWWRegisterReplica[string](2, StringCodec{})

	a.Data.Set("a1", a.NextDot())
	da, _ := a.Data.Set("a2", a.NextDot()) // counter 2
	db, _ := b.Data.Set("b1", b.NextDot()) // counter 1

	// a2 at {1,2} beats b1 at {2,1}.
	c := NewLWWRegisterReplica[string](3, StringCodec{})
	c.ApplyDelta(db)
	c.ApplyDelta(da)
	v, _, _ := c.Data.Get()
	if v != "a2" {
		t.Fatalf("expected a2, got %v", v)
	}
}

func TestLWWRegisterReplica_Convergence(t *testing.T) {
	a := NewLWWRegisterReplica[string](1, StringCodec{})
	b := NewLWWRegisterReplica[string](2, StringCodec{})
	da, _ := a.Data.Set("from-a", a.NextDot())
	db, _ := b.Data.Set("from-b", b.NextDot())
	a.ApplyDelta(db)
	b.ApplyDelta(da)
	va, _, _ := a.Data.Get()
	vb, _, _ := b.Data.Get()
	if va != vb {
		t.Fatalf("expected convergence, got a=%v b=%v", va, vb)
	}
}

func TestLWWRegisterReplica_AntiEntropy(t *testing.T) {
	a := NewLWWRegisterReplica[string](1, StringCodec{})
	b := NewLWWRegisterReplica[string](2, StringCodec{})
	a.Data.Set("val", a.NextDot())
	for _, d := range a.DeltasSince(b.HWM()) {
		b.ApplyDelta(d)
	}
	v, _, ok := b.Data.Get()
	if !ok || v != "val" {
		t.Fatalf("expected val, got %v", v)
	}
}

// --- MVRegister ---

func TestMVRegisterReplica_Write(t *testing.T) {
	r := NewMVRegisterReplica[string](1, StringCodec{})
	r.Data.Write("hello", r.NextDot(), r.HWM())
	vals, _ := r.Data.Values()
	if len(vals) != 1 || vals[0] != "hello" {
		t.Fatalf("expected [hello], got %v", vals)
	}
}

func TestMVRegisterReplica_ConcurrentPreserved(t *testing.T) {
	a := NewMVRegisterReplica[string](1, StringCodec{})
	b := NewMVRegisterReplica[string](2, StringCodec{})
	da, _ := a.Data.Write("from-a", a.NextDot(), a.HWM())
	db, _ := b.Data.Write("from-b", b.NextDot(), b.HWM())
	a.ApplyDelta(db)
	b.ApplyDelta(da)

	va, _ := a.Data.Values()
	vb, _ := b.Data.Values()
	sort.Strings(va)
	sort.Strings(vb)
	if len(va) != 2 || va[0] != "from-a" || va[1] != "from-b" {
		t.Fatalf("a: expected [from-a from-b], got %v", va)
	}
	if len(vb) != 2 || vb[0] != "from-a" || vb[1] != "from-b" {
		t.Fatalf("b: expected [from-a from-b], got %v", vb)
	}
}

func TestMVRegisterReplica_WriteResolvesConflict(t *testing.T) {
	a := NewMVRegisterReplica[string](1, StringCodec{})
	b := NewMVRegisterReplica[string](2, StringCodec{})
	da, _ := a.Data.Write("from-a", a.NextDot(), a.HWM())
	db, _ := b.Data.Write("from-b", b.NextDot(), b.HWM())
	a.ApplyDelta(db)
	b.ApplyDelta(da)
	// Both have 2 values. a writes again to resolve.
	dResolve, _ := a.Data.Write("resolved", a.NextDot(), a.HWM())
	b.ApplyDelta(dResolve)
	vals, _ := b.Data.Values()
	if len(vals) != 1 || vals[0] != "resolved" {
		t.Fatalf("expected [resolved], got %v", vals)
	}
}

func TestMVRegisterReplica_AntiEntropy(t *testing.T) {
	a := NewMVRegisterReplica[string](1, StringCodec{})
	b := NewMVRegisterReplica[string](2, StringCodec{})
	a.Data.Write("val", a.NextDot(), a.HWM())
	for _, d := range a.DeltasSince(b.HWM()) {
		b.ApplyDelta(d)
	}
	vals, _ := b.Data.Values()
	if len(vals) != 1 || vals[0] != "val" {
		t.Fatalf("expected [val], got %v", vals)
	}
}
