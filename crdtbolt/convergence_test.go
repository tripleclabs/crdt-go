package crdtbolt

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/tripleclabs/crdt-go"
)

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

func TestBoltLWWMap_TwoReplicaConvergence(t *testing.T) {
	ba := tempDB(t)
	bb := tempDB(t)

	net := newTestNet()
	a := crdt.NewLWWMap[string](1, crdt.StringCodec{},
		crdt.WithTransport(net.transport(1)), crdt.WithTopology(net.topology(1)), crdt.WithBackend(ba))
	b := crdt.NewLWWMap[string](2, crdt.StringCodec{},
		crdt.WithTransport(net.transport(2)), crdt.WithTopology(net.topology(2)), crdt.WithBackend(bb))
	net.addPeer(1)
	net.addPeer(2)

	ctx := context.Background()
	a.Put(ctx, "name", "alice")
	a.Put(ctx, "city", "paris")
	b.Put(ctx, "lang", "go")
	b.Put(ctx, "color", "blue")

	eventually(t, pollTimeout, func() bool {
		return a.Len() == 4 && b.Len() == 4
	})

	v, ok := b.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("b: expected name=alice, got %v ok=%v", v, ok)
	}
	v, ok = a.Get("lang")
	if !ok || v != "go" {
		t.Fatalf("a: expected lang=go, got %v ok=%v", v, ok)
	}
}

func TestBoltORSet_AntiEntropy(t *testing.T) {
	ba := tempDB(t)
	bb := tempDB(t)

	net := newTestNet()
	a := crdt.NewORSet[string](1, crdt.StringCodec{},
		crdt.WithTransport(net.transport(1)), crdt.WithTopology(net.topology(1)), crdt.WithBackend(ba))
	b := crdt.NewORSet[string](2, crdt.StringCodec{},
		crdt.WithTransport(net.transport(2)), crdt.WithTopology(net.topology(2)), crdt.WithBackend(bb))
	net.addPeer(1)
	net.addPeer(2)

	ctx := context.Background()
	a.Add(ctx, "apple")
	a.Add(ctx, "banana")
	b.Add(ctx, "cherry")
	b.Add(ctx, "date")

	for name, n := range map[string]*crdt.ORSet[string]{"a": a, "b": b} {
		n := n
		eventually(t, pollTimeout, func() bool {
			return n.Len() == 4
		})
		elems, err := n.Elements()
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(elems)
		expected := []string{"apple", "banana", "cherry", "date"}
		for i, e := range expected {
			if elems[i] != e {
				t.Fatalf("replica %s: expected %q at %d, got %q", name, e, i, elems[i])
			}
		}
	}
}

func TestBoltLWWMap_PersistAndRecover(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persist.db")

	// Phase 1: write to a bolt-backed replica, then close.
	b1, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	r1 := crdt.NewLWWMap[string](1, crdt.StringCodec{}, crdt.WithBackend(b1))

	ctx := context.Background()
	r1.Put(ctx, "color", "blue")
	r1.Put(ctx, "size", "large")
	r1.Put(ctx, "temp", "warm")
	r1.Remove(ctx, "temp")

	if r1.Len() != 2 {
		t.Fatalf("expected 2 entries before close, got %d", r1.Len())
	}

	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: reopen and verify data survived.
	b2, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b2.Close() })

	net := newTestNet()
	r2 := crdt.NewLWWMap[string](1, crdt.StringCodec{},
		crdt.WithTransport(net.transport(1)), crdt.WithTopology(net.topology(1)), crdt.WithBackend(b2))
	net.addPeer(1)

	v, ok := r2.Get("color")
	if !ok || v != "blue" {
		t.Fatalf("expected color=blue after reopen, got %v ok=%v", v, ok)
	}
	v, ok = r2.Get("size")
	if !ok || v != "large" {
		t.Fatalf("expected size=large after reopen, got %v ok=%v", v, ok)
	}

	// Phase 3: add a peer, re-put to trigger propagation, verify peer gets it.
	b3 := tempDB(t)
	peer := crdt.NewLWWMap[string](2, crdt.StringCodec{},
		crdt.WithTransport(net.transport(2)), crdt.WithTopology(net.topology(2)), crdt.WithBackend(b3))
	net.addPeer(2)

	r2.Put(ctx, "color", "blue")
	r2.Put(ctx, "size", "large")

	eventually(t, pollTimeout, func() bool {
		pv, ok := peer.Get("color")
		return ok && pv == "blue"
	})
	eventually(t, pollTimeout, func() bool {
		pv, ok := peer.Get("size")
		return ok && pv == "large"
	})
	t.Log("persist and recover verified")
}
