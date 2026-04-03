package crdtbolt

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/3clabs/crdt"
)

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

	for label, n := range map[string]*crdt.LWWMap[string]{"a": a, "b": b} {
		if n.Len() != 4 {
			t.Fatalf("replica %s: expected 4, got %d", label, n.Len())
		}
	}

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
		elems, err := n.Elements()
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(elems)
		if len(elems) != 4 {
			t.Fatalf("replica %s: expected 4 elements, got %d: %v", name, len(elems), elems)
		}
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

	net := newTestNet()

	b1, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	r1 := crdt.NewLWWMap[string](1, crdt.StringCodec{},
		crdt.WithTransport(net.transport(1)), crdt.WithTopology(net.topology(1)), crdt.WithBackend(b1))
	net.addPeer(1)

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

	b2, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b2.Close() })

	net2 := newTestNet()
	r2 := crdt.NewLWWMap[string](1, crdt.StringCodec{},
		crdt.WithTransport(net2.transport(1)), crdt.WithTopology(net2.topology(1)), crdt.WithBackend(b2))
	net2.addPeer(1)

	v, ok := r2.Get("color")
	if !ok || v != "blue" {
		t.Fatalf("expected color=blue after reopen, got %v ok=%v", v, ok)
	}
	v, ok = r2.Get("size")
	if !ok || v != "large" {
		t.Fatalf("expected size=large after reopen, got %v ok=%v", v, ok)
	}
	_, ok = r2.Get("temp")
	if ok {
		t.Fatal("temp should still be removed after reopen")
	}
	if r2.Len() != 2 {
		t.Fatalf("expected 2 entries after reopen, got %d", r2.Len())
	}

	b3 := tempDB(t)
	peer := crdt.NewLWWMap[string](2, crdt.StringCodec{},
		crdt.WithTransport(net2.transport(2)), crdt.WithTopology(net2.topology(2)), crdt.WithBackend(b3))
	net2.addPeer(2)

	r2.Put(ctx, "color", "blue")
	r2.Put(ctx, "size", "large")

	pv, ok := peer.Get("color")
	if !ok || pv != "blue" {
		t.Fatalf("peer expected color=blue, got %v ok=%v", pv, ok)
	}
	pv, ok = peer.Get("size")
	if !ok || pv != "large" {
		t.Fatalf("peer expected size=large, got %v ok=%v", pv, ok)
	}
	t.Log("persist and recover verified")
}
