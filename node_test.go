package crdt

import (
	"context"
	"testing"
	"time"
)

// eventually polls check until it returns true or timeout expires.
func eventually(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

const pollTimeout = 500 * time.Millisecond

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
	eventually(t, pollTimeout, func() bool {
		v, ok := b.Get("key")
		return ok && v == "val"
	})
}

func TestLWWMap_BidirectionalSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *LWWMap[string] {
		return NewLWWMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Put(ctx, "from-a", "alice")
	b.Put(ctx, "from-b", "bob")

	eventually(t, pollTimeout, func() bool {
		v, ok := a.Get("from-b")
		return ok && v == "bob"
	})
	eventually(t, pollTimeout, func() bool {
		v, ok := b.Get("from-a")
		return ok && v == "alice"
	})
}

func TestLWWMap_RemoveSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *LWWMap[string] {
		return NewLWWMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Put(ctx, "key", "val")
	eventually(t, pollTimeout, func() bool {
		_, ok := b.Get("key")
		return ok
	})
	a.Remove(ctx, "key")
	eventually(t, pollTimeout, func() bool {
		_, ok := b.Get("key")
		return !ok
	})
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
		n := n
		eventually(t, pollTimeout, func() bool {
			return n.Len() == 3
		})
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

	eventually(t, pollTimeout, func() bool {
		return b.Contains("apple") && b.Contains("banana")
	})
}

func TestORSet_RemoveSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *ORSet[string] {
		return NewORSet[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Add(ctx, "x")
	eventually(t, pollTimeout, func() bool {
		return b.Contains("x")
	})

	a.Remove(ctx, "x")
	eventually(t, pollTimeout, func() bool {
		return !b.Contains("x")
	})
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
	eventually(t, pollTimeout, func() bool {
		v, ok := b.Get("key")
		return ok && v == "val"
	})
}

func TestORMap_RemoveSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *ORMap[string] {
		return NewORMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Put(ctx, "key", "val")
	eventually(t, pollTimeout, func() bool {
		_, ok := b.Get("key")
		return ok
	})
	a.Remove(ctx, "key")
	eventually(t, pollTimeout, func() bool {
		_, ok := b.Get("key")
		return !ok
	})
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
	eventually(t, pollTimeout, func() bool {
		v, ok := b.Get("key")
		return ok && v == "val"
	})
}

func TestAWLWWMap_RemoveSync(t *testing.T) {
	a, b := twoNode(t, func(id ReplicaID, opts ...Option) *AWLWWMap[string] {
		return NewAWLWWMap[string](id, StringCodec{}, opts...)
	})
	ctx := context.Background()

	a.Put(ctx, "key", "val")
	eventually(t, pollTimeout, func() bool {
		_, ok := b.Get("key")
		return ok
	})
	a.Remove(ctx, "key")
	eventually(t, pollTimeout, func() bool {
		_, ok := b.Get("key")
		return !ok
	})
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

	eventually(t, pollTimeout, func() bool {
		return b.Len() == 2
	})
	items, err := b.Items()
	if err != nil {
		t.Fatal(err)
	}
	if items[0] != "first" || items[1] != "second" {
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

	eventually(t, pollTimeout, func() bool {
		return a.Int64() == 8 && b.Int64() == 8
	})
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

	eventually(t, pollTimeout, func() bool {
		return a.Int64() == 7 && b.Int64() == 7
	})
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
	eventually(t, pollTimeout, func() bool {
		v, ok := b.Get()
		return ok && v == "hello"
	})
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
	eventually(t, pollTimeout, func() bool {
		vals, err := b.Values()
		return err == nil && len(vals) == 1 && vals[0] == "hello"
	})
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
	a := NewLWWMap[string](1, StringCodec{})
	ctx := context.Background()
	a.Put(ctx, "key1", "val1")
	a.Put(ctx, "key2", "val2")
	a.Close()

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

	a2.Put(ctx, "key1", "val1")
	a2.Put(ctx, "key2", "val2")

	eventually(t, 3*time.Second, func() bool {
		v1, ok1 := b.Get("key1")
		v2, ok2 := b.Get("key2")
		return ok1 && v1 == "val1" && ok2 && v2 == "val2"
	})
}

func TestGCounter_AntiEntropy(t *testing.T) {
	net := newTestNet()
	aeInterval := 50 * time.Millisecond

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
	net.addPeer(1)
	net.addPeer(2)
	defer a.Close()
	defer b.Close()

	ctx := context.Background()
	a.Increment(ctx, 42)

	eventually(t, 3*time.Second, func() bool {
		return b.Int64() == 42
	})
}
