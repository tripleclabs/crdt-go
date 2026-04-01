package replica

import (
	"sort"
	"testing"

	"github.com/3clabs/crdt"
)

// --- PNCounter ---

func TestPNCounterReplica_IncrementDecrement(t *testing.T) {
	r := NewPNCounter(1)
	r.Increment(10)
	r.Decrement(3)
	if r.Data.Int64() != 7 {
		t.Fatalf("expected 7, got %d", r.Data.Int64())
	}
}

func TestPNCounterReplica_ApplyDelta(t *testing.T) {
	a := NewPNCounter(1)
	b := NewPNCounter(2)
	dInc := a.Increment(10)
	dDec := a.Decrement(2)
	db := b.Increment(5)

	a.ApplyDelta(db)
	b.ApplyDelta(dInc)
	b.ApplyDelta(dDec)

	if a.Data.Int64() != 13 || b.Data.Int64() != 13 {
		t.Fatalf("expected both 13, got a=%d b=%d", a.Data.Int64(), b.Data.Int64())
	}
}

func TestPNCounterReplica_Idempotent(t *testing.T) {
	a := NewPNCounter(1)
	b := NewPNCounter(2)
	d := a.Increment(5)
	b.ApplyDelta(d)
	b.ApplyDelta(d)
	if b.Data.Int64() != 5 {
		t.Fatalf("expected 5, got %d", b.Data.Int64())
	}
}

// --- LWWRegister ---

func TestLWWRegisterReplica_Set(t *testing.T) {
	r := NewLWWRegister[string](1, crdt.StringCodec{})
	delta, err := r.Set("hello")
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
	a := NewLWWRegister[string](1, crdt.StringCodec{})
	b := NewLWWRegister[string](2, crdt.StringCodec{})

	a.Set("a1")
	da, _ := a.Set("a2") // counter 2
	db, _ := b.Set("b1") // counter 1

	// a2 at {1,2} beats b1 at {2,1}.
	c := NewLWWRegister[string](3, crdt.StringCodec{})
	c.ApplyDelta(db)
	c.ApplyDelta(da)
	v, _, _ := c.Data.Get()
	if v != "a2" {
		t.Fatalf("expected a2, got %v", v)
	}
}

func TestLWWRegisterReplica_Convergence(t *testing.T) {
	a := NewLWWRegister[string](1, crdt.StringCodec{})
	b := NewLWWRegister[string](2, crdt.StringCodec{})
	da, _ := a.Set("from-a")
	db, _ := b.Set("from-b")
	a.ApplyDelta(db)
	b.ApplyDelta(da)
	va, _, _ := a.Data.Get()
	vb, _, _ := b.Data.Get()
	if va != vb {
		t.Fatalf("expected convergence, got a=%v b=%v", va, vb)
	}
}

func TestLWWRegisterReplica_AntiEntropy(t *testing.T) {
	a := NewLWWRegister[string](1, crdt.StringCodec{})
	b := NewLWWRegister[string](2, crdt.StringCodec{})
	a.Set("val")
	for _, d := range a.DeltasSince(b.Received.HWM()) {
		b.ApplyDelta(d)
	}
	v, _, ok := b.Data.Get()
	if !ok || v != "val" {
		t.Fatalf("expected val, got %v", v)
	}
}

// --- MVRegister ---

func TestMVRegisterReplica_Write(t *testing.T) {
	r := NewMVRegister[string](1, crdt.StringCodec{})
	r.Write("hello")
	vals, _ := r.Data.Values()
	if len(vals) != 1 || vals[0] != "hello" {
		t.Fatalf("expected [hello], got %v", vals)
	}
}

func TestMVRegisterReplica_ConcurrentPreserved(t *testing.T) {
	a := NewMVRegister[string](1, crdt.StringCodec{})
	b := NewMVRegister[string](2, crdt.StringCodec{})
	da, _ := a.Write("from-a")
	db, _ := b.Write("from-b")
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
	a := NewMVRegister[string](1, crdt.StringCodec{})
	b := NewMVRegister[string](2, crdt.StringCodec{})
	da, _ := a.Write("from-a")
	db, _ := b.Write("from-b")
	a.ApplyDelta(db)
	b.ApplyDelta(da)
	// Both have 2 values. a writes again to resolve.
	dResolve, _ := a.Write("resolved")
	b.ApplyDelta(dResolve)
	vals, _ := b.Data.Values()
	if len(vals) != 1 || vals[0] != "resolved" {
		t.Fatalf("expected [resolved], got %v", vals)
	}
}

// --- ORMap ---

func TestORMapReplica_PutGet(t *testing.T) {
	r := NewORMap[string](1, crdt.StringCodec{})
	r.Put("name", "alice")
	v, _, ok := r.Data.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
}

func TestORMapReplica_ApplyDelta(t *testing.T) {
	a := NewORMap[string](1, crdt.StringCodec{})
	b := NewORMap[string](2, crdt.StringCodec{})
	da, _ := a.Put("x", "from-a")
	db, _ := b.Put("y", "from-b")
	a.ApplyDelta(db)
	b.ApplyDelta(da)
	if a.Data.Len() != 2 || b.Data.Len() != 2 {
		t.Fatalf("expected both 2, got a=%d b=%d", a.Data.Len(), b.Data.Len())
	}
}

func TestORMapReplica_AddWins(t *testing.T) {
	a := NewORMap[string](1, crdt.StringCodec{})
	putDelta, _ := a.Put("k", "val")
	b := NewORMap[string](2, crdt.StringCodec{})
	b.Put("k", "b-val")
	removeDelta := b.Remove("k")

	c := NewORMap[string](3, crdt.StringCodec{})
	c.ApplyDelta(removeDelta)
	c.ApplyDelta(putDelta) // a's add should survive — dot not in remove context
	if c.Data.Len() != 1 {
		t.Fatal("add-wins: key should survive")
	}
}

// --- AWLWWMap ---

func TestAWLWWMapReplica_PutGet(t *testing.T) {
	r := NewAWLWWMap[string](1, crdt.StringCodec{})
	r.Put("name", "alice")
	v, _, ok := r.Data.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
}

func TestAWLWWMapReplica_AddWins(t *testing.T) {
	a := NewAWLWWMap[string](1, crdt.StringCodec{})
	putDelta, _ := a.Put("k", "from-a")

	b := NewAWLWWMap[string](2, crdt.StringCodec{})
	b.Put("k", "temp")
	removeDelta := b.Remove("k")

	// c: put from a should survive b's remove (add-wins).
	c := NewAWLWWMap[string](3, crdt.StringCodec{})
	c.ApplyDelta(removeDelta)
	c.ApplyDelta(putDelta)
	v, _, ok := c.Data.Get("k")
	if !ok || v != "from-a" {
		t.Fatalf("add-wins: expected from-a, got %v ok=%v", v, ok)
	}
}

func TestAWLWWMapReplica_ObservedRemove(t *testing.T) {
	a := NewAWLWWMap[string](1, crdt.StringCodec{})
	putDelta, _ := a.Put("k", "val")

	b := NewAWLWWMap[string](2, crdt.StringCodec{})
	b.ApplyDelta(putDelta) // b has seen a's dot
	removeDelta := b.Remove("k")

	a.ApplyDelta(removeDelta) // remove context covers a's dot
	_, _, ok := a.Data.Get("k")
	if ok {
		t.Fatal("observed remove: key should be gone")
	}
}

// --- GList ---

func TestGListReplica_Append(t *testing.T) {
	r := NewGList[string](1, crdt.StringCodec{})
	r.Append("first")
	r.Append("second")
	items, _ := r.Data.Items()
	if len(items) != 2 || items[0] != "first" || items[1] != "second" {
		t.Fatalf("expected [first second], got %v", items)
	}
}

func TestGListReplica_ApplyDelta(t *testing.T) {
	a := NewGList[string](1, crdt.StringCodec{})
	b := NewGList[string](2, crdt.StringCodec{})
	da, _ := a.Append("from-a")
	db, _ := b.Append("from-b")
	a.ApplyDelta(db)
	b.ApplyDelta(da)

	ia, _ := a.Data.Items()
	ib, _ := b.Data.Items()
	if len(ia) != 2 || len(ib) != 2 {
		t.Fatalf("expected both 2, got a=%d b=%d", len(ia), len(ib))
	}
}

func TestGListReplica_Idempotent(t *testing.T) {
	a := NewGList[string](1, crdt.StringCodec{})
	b := NewGList[string](2, crdt.StringCodec{})
	d, _ := a.Append("x")
	b.ApplyDelta(d)
	b.ApplyDelta(d)
	if b.Data.Len() != 1 {
		t.Fatalf("expected 1, got %d", b.Data.Len())
	}
}

func TestGListReplica_AntiEntropy(t *testing.T) {
	a := NewGList[string](1, crdt.StringCodec{})
	b := NewGList[string](2, crdt.StringCodec{})
	a.Append("x")
	a.Append("y")
	b.Append("z")

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

// --- PNCounter anti-entropy ---

func TestPNCounterReplica_AntiEntropy(t *testing.T) {
	a := NewPNCounter(1)
	b := NewPNCounter(2)
	a.Increment(10)
	a.Decrement(3)
	b.Increment(5)

	for _, d := range a.DeltasSince(b.Received.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.Received.HWM()) {
		a.ApplyDelta(d)
	}
	if a.Data.Int64() != b.Data.Int64() {
		t.Fatalf("expected convergence, got a=%d b=%d", a.Data.Int64(), b.Data.Int64())
	}
}

// --- MVRegister anti-entropy ---

func TestMVRegisterReplica_AntiEntropy(t *testing.T) {
	a := NewMVRegister[string](1, crdt.StringCodec{})
	b := NewMVRegister[string](2, crdt.StringCodec{})
	a.Write("val")
	for _, d := range a.DeltasSince(b.Received.HWM()) {
		b.ApplyDelta(d)
	}
	vals, _ := b.Data.Values()
	if len(vals) != 1 || vals[0] != "val" {
		t.Fatalf("expected [val], got %v", vals)
	}
}

// --- ORMap anti-entropy ---

func TestORMapReplica_AntiEntropy(t *testing.T) {
	a := NewORMap[string](1, crdt.StringCodec{})
	b := NewORMap[string](2, crdt.StringCodec{})
	a.Put("x", "from-a")
	b.Put("y", "from-b")

	for _, d := range a.DeltasSince(b.Received.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.Received.HWM()) {
		a.ApplyDelta(d)
	}
	if a.Data.Len() != 2 || b.Data.Len() != 2 {
		t.Fatalf("expected both 2, got a=%d b=%d", a.Data.Len(), b.Data.Len())
	}
}

// --- AWLWWMap anti-entropy ---

func TestAWLWWMapReplica_AntiEntropy(t *testing.T) {
	a := NewAWLWWMap[string](1, crdt.StringCodec{})
	b := NewAWLWWMap[string](2, crdt.StringCodec{})
	a.Put("x", "from-a")
	b.Put("y", "from-b")

	for _, d := range a.DeltasSince(b.Received.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.Received.HWM()) {
		a.ApplyDelta(d)
	}
	if a.Data.Len() != 2 || b.Data.Len() != 2 {
		t.Fatalf("expected both 2, got a=%d b=%d", a.Data.Len(), b.Data.Len())
	}
}

func TestGListReplica_CausalOrder(t *testing.T) {
	a := NewGList[string](1, crdt.StringCodec{})
	b := NewGList[string](2, crdt.StringCodec{})
	da, _ := a.Append("a1") // dot {1,1}
	db, _ := b.Append("b1") // dot {2,1}

	c := NewGList[string](3, crdt.StringCodec{})
	c.ApplyDelta(db)
	c.ApplyDelta(da)
	items, _ := c.Data.Items()
	// Same counter, lower replica first.
	if items[0] != "a1" || items[1] != "b1" {
		t.Fatalf("expected [a1 b1], got %v", items)
	}
}
