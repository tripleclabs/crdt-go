package crdt

import (
	"context"
	"testing"
	"time"
)

func twoNode[T any](t *testing.T, make func(id ReplicaID, opts ...Option) T) (T, T) {
	t.Helper()
	net := newTestNet()
	a := make(1, WithTransport(net.transport(1)), WithTopology(net.topology(1)))
	b := make(2, WithTransport(net.transport(2)), WithTopology(net.topology(2)))
	net.addPeer(1)
	net.addPeer(2)
	return a, b
}

func threeNode[T any](t *testing.T, make func(id ReplicaID, opts ...Option) T) (T, T, T) {
	t.Helper()
	net := newTestNet()
	a := make(1, WithTransport(net.transport(1)), WithTopology(net.topology(1)))
	b := make(2, WithTransport(net.transport(2)), WithTopology(net.topology(2)))
	c := make(3, WithTransport(net.transport(3)), WithTopology(net.topology(3)))
	net.addPeer(1)
	net.addPeer(2)
	net.addPeer(3)
	return a, b, c
}

// ---------------------------------------------------------------------------
// LWWMap
// ---------------------------------------------------------------------------

func TestLWWMap_TwoNodeSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *LWWMap[string] {
		return NewLWWMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	if _, err := a.Put(ctx, "key", "val"); err != nil {
		t.Fatal(err)
	}
	v, ok := b.Get("key")
	if !ok || v != "val" {
		t.Fatalf("expected val, got %q ok=%v", v, ok)
	}
}

func TestLWWMap_BidirectionalSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *LWWMap[string] {
		return NewLWWMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Put(ctx, "from-a", "alice")
	b.Put(ctx, "from-b", "bob")

	v, ok := a.Get("from-b")
	if !ok || v != "bob" {
		t.Fatalf("a: expected bob, got %q ok=%v", v, ok)
	}
	v, ok = b.Get("from-a")
	if !ok || v != "alice" {
		t.Fatalf("b: expected alice, got %q ok=%v", v, ok)
	}
}

func TestLWWMap_RemoveSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *LWWMap[string] {
		return NewLWWMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Put(ctx, "key", "val")
	a.Remove(ctx, "key")

	_, ok := b.Get("key")
	if ok {
		t.Fatal("expected key to be removed on b")
	}
}

func TestLWWMap_ThreeNodeConvergence(t *testing.T) {
	a, b, c := threeNode(t, func(id ReplicaID, opts ...Option) *LWWMap[string] {
		return NewLWWMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Put(ctx, "x", "from-a")
	b.Put(ctx, "y", "from-b")
	c.Put(ctx, "z", "from-c")

	for _, n := range []*LWWMap[string]{a, b, c} {
		if n.Len() != 3 {
			t.Fatalf("expected 3, got %d", n.Len())
		}
	}
}

func TestLWWMap_LocalOnly(t *testing.T) {
	m := NewLWWMap[string](1, StringCodec{})
	ctx := context.Background()

	m.Put(ctx, "key", "val")
	v, ok := m.Get("key")
	if !ok || v != "val" {
		t.Fatalf("expected val, got %q ok=%v", v, ok)
	}
}

// ---------------------------------------------------------------------------
// ORSet
// ---------------------------------------------------------------------------

func TestORSet_TwoNodeSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *ORSet[string] {
		return NewORSet[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Add(ctx, "apple")
	a.Add(ctx, "banana")

	if !b.Contains("apple") || !b.Contains("banana") {
		t.Fatal("expected both elements on b")
	}
}

func TestORSet_RemoveSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *ORSet[string] {
		return NewORSet[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Add(ctx, "x")
	if !b.Contains("x") {
		t.Fatal("expected x on b after add")
	}

	a.Remove(ctx, "x")
	if b.Contains("x") {
		t.Fatal("expected x removed on b")
	}
}

// ---------------------------------------------------------------------------
// ORMap
// ---------------------------------------------------------------------------

func TestORMap_TwoNodeSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *ORMap[string] {
		return NewORMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Put(ctx, "key", "val")
	v, ok := b.Get("key")
	if !ok || v != "val" {
		t.Fatalf("expected val, got %q ok=%v", v, ok)
	}
}

func TestORMap_RemoveSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *ORMap[string] {
		return NewORMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Put(ctx, "key", "val")
	a.Remove(ctx, "key")

	_, ok := b.Get("key")
	if ok {
		t.Fatal("expected key removed on b")
	}
}

// ---------------------------------------------------------------------------
// AWLWWMap
// ---------------------------------------------------------------------------

func TestAWLWWMap_TwoNodeSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *AWLWWMap[string] {
		return NewAWLWWMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Put(ctx, "key", "val")
	v, ok := b.Get("key")
	if !ok || v != "val" {
		t.Fatalf("expected val, got %q ok=%v", v, ok)
	}
}

func TestAWLWWMap_RemoveSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *AWLWWMap[string] {
		return NewAWLWWMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Put(ctx, "key", "val")
	a.Remove(ctx, "key")

	_, ok := b.Get("key")
	if ok {
		t.Fatal("expected key removed on b")
	}
}

// ---------------------------------------------------------------------------
// GList
// ---------------------------------------------------------------------------

func TestGList_TwoNodeSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *GList[string] {
		return NewGList[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Append(ctx, "first")
	a.Append(ctx, "second")

	items, err := b.Items()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0] != "first" || items[1] != "second" {
		t.Fatalf("expected [first second], got %v", items)
	}
}

// ---------------------------------------------------------------------------
// GCounter
// ---------------------------------------------------------------------------

func TestGCounter_TwoNodeSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *GCounter {
		return NewGCounter(id, opts...)
	})
	ctx := context.Background()

	a.Increment(ctx, 5)
	b.Increment(ctx, 3)

	if a.Int64() != 8 {
		t.Fatalf("a: expected 8, got %d", a.Int64())
	}
	if b.Int64() != 8 {
		t.Fatalf("b: expected 8, got %d", b.Int64())
	}
}

// ---------------------------------------------------------------------------
// PNCounter
// ---------------------------------------------------------------------------

func TestPNCounter_TwoNodeSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *PNCounter {
		return NewPNCounter(id, opts...)
	})
	ctx := context.Background()

	a.Increment(ctx, 10)
	b.Decrement(ctx, 3)

	if a.Int64() != 7 {
		t.Fatalf("a: expected 7, got %d", a.Int64())
	}
	if b.Int64() != 7 {
		t.Fatalf("b: expected 7, got %d", b.Int64())
	}
}

// ---------------------------------------------------------------------------
// LWWRegister
// ---------------------------------------------------------------------------

func TestLWWRegister_TwoNodeSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *LWWRegister[string] {
		return NewLWWRegister[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Set(ctx, "hello")
	v, ok := b.Get()
	if !ok || v != "hello" {
		t.Fatalf("expected hello, got %q ok=%v", v, ok)
	}
}

// ---------------------------------------------------------------------------
// MVRegister
// ---------------------------------------------------------------------------

func TestMVRegister_TwoNodeSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *MVRegister[string] {
		return NewMVRegister[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Write(ctx, "hello")
	vals, err := b.Values()
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 || vals[0] != "hello" {
		t.Fatalf("expected [hello], got %v", vals)
	}
}

// ---------------------------------------------------------------------------
// Write concerns
// ---------------------------------------------------------------------------

func TestLWWMap_WriteConcern_WAll(t *testing.T) {
	net := newTestNet()
	mkNode := func(id ReplicaID) *LWWMap[string] {
		return NewLWWMap[string](id, StringCodec{},
			WithTransport(net.transport(id)),
			WithTopology(net.topology(id)),
			WithWriteConcern(WAll),
		)
	}
	a := mkNode(1)
	b := mkNode(2)
	c := mkNode(3)
	net.addPeer(1)
	net.addPeer(2)
	net.addPeer(3)

	ctx := context.Background()

	wr, err := a.Put(ctx, "key", "val")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := wr.Wait(ctx); err != nil {
		t.Fatalf("WAll Wait failed: %v", err)
	}

	for _, n := range []*LWWMap[string]{b, c} {
		v, ok := n.Get("key")
		if !ok || v != "val" {
			t.Fatalf("expected val, got %q ok=%v", v, ok)
		}
	}
}

// ---------------------------------------------------------------------------
// Anti-entropy
// ---------------------------------------------------------------------------

func TestLWWMap_AntiEntropy(t *testing.T) {
	// Node A writes locally (no transport).
	a := NewLWWMap[string](1, StringCodec{})
	ctx := context.Background()
	a.Put(ctx, "key1", "val1")
	a.Put(ctx, "key2", "val2")
	a.Close()

	// Now wire both nodes into a network.
	net := newTestNet()
	aeInterval := 50 * time.Millisecond
	a2 := NewLWWMap[string](1, StringCodec{},
		WithTransport(net.transport(1)),
		WithTopology(net.topology(1)),
		WithAntiEntropyInterval(aeInterval),
	)
	b := NewLWWMap[string](2, StringCodec{},
		WithTransport(net.transport(2)),
		WithTopology(net.topology(2)),
		WithAntiEntropyInterval(aeInterval),
	)
	net.addPeer(1)
	net.addPeer(2)
	defer a2.Close()
	defer b.Close()

	// Re-apply a's state to a2 (simulating recovery from persistence).
	a2.Put(ctx, "key1", "val1")
	a2.Put(ctx, "key2", "val2")

	// Wait for anti-entropy to sync.
	time.Sleep(3 * aeInterval)

	v, ok := b.Get("key1")
	if !ok || v != "val1" {
		t.Fatalf("expected val1, got %q ok=%v", v, ok)
	}
	v, ok = b.Get("key2")
	if !ok || v != "val2" {
		t.Fatalf("expected val2, got %q ok=%v", v, ok)
	}
}

func TestGCounter_AntiEntropy(t *testing.T) {
	net := newTestNet()
	aeInterval := 50 * time.Millisecond

	// Create two counters. Node A increments, but we disable immediate
	// propagation by adding the peer AFTER the mutation.
	a := NewGCounter(1,
		WithTransport(net.transport(1)),
		WithTopology(net.topology(1)),
		WithAntiEntropyInterval(aeInterval),
	)
	b := NewGCounter(2,
		WithTransport(net.transport(2)),
		WithTopology(net.topology(2)),
		WithAntiEntropyInterval(aeInterval),
	)
	// Register peers AFTER creating nodes so OnReceive is wired,
	// but topology is empty during A's increment.
	net.addPeer(1)
	net.addPeer(2)
	defer a.Close()
	defer b.Close()

	ctx := context.Background()
	a.Increment(ctx, 42)

	// Anti-entropy should sync the counter.
	time.Sleep(3 * aeInterval)

	if b.Int64() != 42 {
		t.Fatalf("expected 42, got %d", b.Int64())
	}
}
