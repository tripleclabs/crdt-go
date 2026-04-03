package crdt

import (
	"context"
	"testing"
	"time"
)

func twoNodeDeltaMap(t *testing.T) (*DeltaMap[ORSetKind[string]], *DeltaMap[ORSetKind[string]]) {
	t.Helper()
	net := newTestNet()
	kind := ORSetKind[string]{Codec: StringCodec{}}
	a := NewDeltaMap(1, kind, WithTransport(net.transport(1)), WithTopology(net.topology(1)))
	b := NewDeltaMap(2, kind, WithTransport(net.transport(2)), WithTopology(net.topology(2)))
	net.addPeer(1)
	net.addPeer(2)
	return a, b
}

// ---------------------------------------------------------------------------
// Basic two-node sync
// ---------------------------------------------------------------------------

func TestDeltaMap_TwoNodeSync(t *testing.T) {
	a, b := twoNodeDeltaMap(t)
	defer a.Close()
	defer b.Close()
	ctx := context.Background()

	a.Mutate(ctx, "node-1", AddSetMember[string]{Value: "chat.general"})
	a.Mutate(ctx, "node-1", AddSetMember[string]{Value: "chat.random"})

	eventually(t, pollTimeout, func() bool {
		r := b.Query("node-1", SetLen[string]{})
		return r != nil && r.(int) == 2
	})

	elems := b.Query("node-1", SetElements[string]{}).([]string)
	if len(elems) != 2 {
		t.Fatalf("expected 2 elements, got %v", elems)
	}
}

func TestDeltaMap_BidirectionalSync(t *testing.T) {
	a, b := twoNodeDeltaMap(t)
	defer a.Close()
	defer b.Close()
	ctx := context.Background()

	a.Mutate(ctx, "node-1", AddSetMember[string]{Value: "topic-a"})
	b.Mutate(ctx, "node-2", AddSetMember[string]{Value: "topic-b"})

	eventually(t, pollTimeout, func() bool {
		return a.HasKey("node-2") && b.HasKey("node-1")
	})
}

// ---------------------------------------------------------------------------
// Cascading remove (DSON-style, no tombstones)
// ---------------------------------------------------------------------------

func TestDeltaMap_CascadingRemove(t *testing.T) {
	a, b := twoNodeDeltaMap(t)
	defer a.Close()
	defer b.Close()
	ctx := context.Background()

	// Add topics under node-1.
	a.Mutate(ctx, "node-1", AddSetMember[string]{Value: "chat.general"})
	a.Mutate(ctx, "node-1", AddSetMember[string]{Value: "chat.random"})
	a.Mutate(ctx, "node-1", AddSetMember[string]{Value: "chat.dev"})

	eventually(t, pollTimeout, func() bool {
		r := b.Query("node-1", SetLen[string]{})
		return r != nil && r.(int) == 3
	})

	// Remove the entire key — cascading remove.
	a.RemoveKey(ctx, "node-1")

	eventually(t, pollTimeout, func() bool {
		return !b.HasKey("node-1")
	})

	// Verify no ghost state — key is truly gone.
	if a.HasKey("node-1") {
		t.Fatal("node-1 should be removed on a")
	}
}

func TestDeltaMap_RemoveAndReAdd(t *testing.T) {
	a, b := twoNodeDeltaMap(t)
	defer a.Close()
	defer b.Close()
	ctx := context.Background()

	// Add then remove.
	a.Mutate(ctx, "node-1", AddSetMember[string]{Value: "topic"})
	eventually(t, pollTimeout, func() bool { return b.HasKey("node-1") })

	a.RemoveKey(ctx, "node-1")
	eventually(t, pollTimeout, func() bool { return !b.HasKey("node-1") })

	// Re-add after remove works cleanly.
	a.Mutate(ctx, "node-1", AddSetMember[string]{Value: "new-topic"})
	eventually(t, pollTimeout, func() bool {
		r := b.Query("node-1", ContainsSetMember[string]{Value: "new-topic"})
		return r != nil && r.(bool)
	})

	// Old topic should NOT be present.
	r := b.Query("node-1", ContainsSetMember[string]{Value: "topic"})
	if r != nil && r.(bool) {
		t.Fatal("old topic should not exist after remove + re-add")
	}
}

// ---------------------------------------------------------------------------
// Concurrent add vs remove (add-wins)
// ---------------------------------------------------------------------------

func TestDeltaMap_ConcurrentAddWins(t *testing.T) {
	// Connected nodes. A adds "existing", syncs to B. Then A removes
	// the key while B concurrently adds "concurrent". The concurrent
	// add should survive (add-wins).
	a, b := twoNodeDeltaMap(t)
	defer a.Close()
	defer b.Close()
	ctx := context.Background()

	// Both start with "existing" under node-1.
	a.Mutate(ctx, "node-1", AddSetMember[string]{Value: "existing"})
	eventually(t, pollTimeout, func() bool { return b.HasKey("node-1") })

	// A removes the key. The remove propagates to B asynchronously.
	a.RemoveKey(ctx, "node-1")

	// B adds concurrently. If B's add dot is NOT dominated by A's
	// remove context, "concurrent" survives on both nodes.
	b.Mutate(ctx, "node-1", AddSetMember[string]{Value: "concurrent"})

	// Both should converge: "concurrent" survives.
	eventually(t, pollTimeout, func() bool {
		r := a.Query("node-1", ContainsSetMember[string]{Value: "concurrent"})
		return r != nil && r.(bool)
	})
	eventually(t, pollTimeout, func() bool {
		r := b.Query("node-1", ContainsSetMember[string]{Value: "concurrent"})
		return r != nil && r.(bool)
	})
}

// ---------------------------------------------------------------------------
// Anti-entropy
// ---------------------------------------------------------------------------

func TestDeltaMap_AntiEntropy(t *testing.T) {
	net := newTestNet()
	kind := ORSetKind[string]{Codec: StringCodec{}}
	aeInterval := 50 * time.Millisecond

	a := NewDeltaMap(1, kind,
		WithTransport(net.transport(1)),
		WithTopology(net.topology(1)),
		WithAntiEntropyInterval(aeInterval),
	)
	b := NewDeltaMap(2, kind,
		WithTransport(net.transport(2)),
		WithTopology(net.topology(2)),
		WithAntiEntropyInterval(aeInterval),
	)
	net.addPeer(1)
	net.addPeer(2)
	defer a.Close()
	defer b.Close()

	ctx := context.Background()
	a.Mutate(ctx, "node-1", AddSetMember[string]{Value: "topic-x"})

	eventually(t, 3*time.Second, func() bool {
		r := b.Query("node-1", ContainsSetMember[string]{Value: "topic-x"})
		return r != nil && r.(bool)
	})
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

func TestDeltaMap_Query(t *testing.T) {
	a := NewDeltaMap(1, ORSetKind[string]{Codec: StringCodec{}})
	ctx := context.Background()

	// Query on nonexistent key returns nil.
	if r := a.Query("missing", SetElements[string]{}); r != nil {
		t.Fatalf("expected nil for missing key, got %v", r)
	}

	a.Mutate(ctx, "k", AddSetMember[string]{Value: "a"})
	a.Mutate(ctx, "k", AddSetMember[string]{Value: "b"})

	// Contains
	if r := a.Query("k", ContainsSetMember[string]{Value: "a"}); r != true {
		t.Fatal("expected contains=true")
	}
	if r := a.Query("k", ContainsSetMember[string]{Value: "z"}); r != false {
		t.Fatal("expected contains=false")
	}

	// Elements
	elems := a.Query("k", SetElements[string]{}).([]string)
	if len(elems) != 2 {
		t.Fatalf("expected 2 elements, got %v", elems)
	}

	// Len
	if r := a.Query("k", SetLen[string]{}); r != 2 {
		t.Fatalf("expected len=2, got %v", r)
	}
}

// ---------------------------------------------------------------------------
// Convenience methods
// ---------------------------------------------------------------------------

func TestDeltaMap_HasKeyKeysLen(t *testing.T) {
	a := NewDeltaMap(1, ORSetKind[string]{Codec: StringCodec{}})
	ctx := context.Background()

	if a.HasKey("x") {
		t.Fatal("expected no keys")
	}
	if a.Len() != 0 {
		t.Fatal("expected len 0")
	}

	a.Mutate(ctx, "x", AddSetMember[string]{Value: "v"})
	a.Mutate(ctx, "y", AddSetMember[string]{Value: "v"})

	if !a.HasKey("x") || !a.HasKey("y") {
		t.Fatal("expected both keys")
	}
	if a.Len() != 2 {
		t.Fatalf("expected len 2, got %d", a.Len())
	}

	keys := a.Keys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %v", keys)
	}
}

// ---------------------------------------------------------------------------
// Pubsub scenario
// ---------------------------------------------------------------------------

func TestDeltaMap_PubsubScenario(t *testing.T) {
	net := newTestNet()
	kind := ORSetKind[string]{Codec: StringCodec{}}

	// byNode: nodeID → set of topics
	byNodeA := NewDeltaMap(1, kind,
		WithTransport(net.transport(1)), WithTopology(net.topology(1)))
	byNodeB := NewDeltaMap(2, kind,
		WithTransport(net.transport(2)), WithTopology(net.topology(2)))
	net.addPeer(1)
	net.addPeer(2)
	defer byNodeA.Close()
	defer byNodeB.Close()

	ctx := context.Background()

	// Node 1 subscribes to topics.
	byNodeA.Mutate(ctx, "node-1", AddSetMember[string]{Value: "chat.general"})
	byNodeA.Mutate(ctx, "node-1", AddSetMember[string]{Value: "chat.dev"})

	// Node 2 subscribes to topics.
	byNodeB.Mutate(ctx, "node-2", AddSetMember[string]{Value: "chat.general"})
	byNodeB.Mutate(ctx, "node-2", AddSetMember[string]{Value: "chat.random"})

	// Wait for convergence.
	eventually(t, pollTimeout, func() bool {
		return byNodeA.Len() == 2 && byNodeB.Len() == 2
	})

	// Node 1 leaves — remove all its subscriptions.
	byNodeA.RemoveKey(ctx, "node-1")

	eventually(t, pollTimeout, func() bool {
		return !byNodeB.HasKey("node-1") && byNodeB.Len() == 1
	})

	// Node 2's subscriptions are unaffected.
	r := byNodeB.Query("node-2", SetLen[string]{})
	if r == nil || r.(int) != 2 {
		t.Fatalf("node-2 should still have 2 topics, got %v", r)
	}
}
