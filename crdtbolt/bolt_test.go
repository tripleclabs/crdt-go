package crdtbolt

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tripleclabs/crdt-go"
)

func tempDB(t *testing.T) *BoltBackend {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// --- Low-level Backend tests ---

func TestBoltBackend_EntryOps(t *testing.T) {
	b := tempDB(t)
	_, _, ok := b.GetEntry("missing")
	if ok {
		t.Fatal("expected not found")
	}
	b.PutEntry("k", []byte("val"), []byte{1, 2})
	v, m, ok := b.GetEntry("k")
	if !ok {
		t.Fatal("expected found")
	}
	if string(v) != "val" || len(m) != 2 || m[0] != 1 {
		t.Fatalf("got v=%s m=%v", v, m)
	}
}

func TestBoltBackend_DeleteEntry(t *testing.T) {
	b := tempDB(t)
	b.PutEntry("k", []byte("v"), []byte{1})
	b.DeleteEntry("k")
	_, _, ok := b.GetEntry("k")
	if ok {
		t.Fatal("expected not found after delete")
	}
}

func TestBoltBackend_RangeEntries(t *testing.T) {
	b := tempDB(t)
	b.PutEntry("a", []byte("1"), []byte{10})
	b.PutEntry("b", []byte("2"), []byte{20})
	count := 0
	b.RangeEntries(func(key string, value []byte, meta []byte) bool {
		count++
		return true
	})
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestBoltBackend_EntryLen(t *testing.T) {
	b := tempDB(t)
	if b.EntryLen() != 0 {
		t.Fatal("expected 0")
	}
	b.PutEntry("k", []byte("v"), []byte{1})
	if b.EntryLen() != 1 {
		t.Fatalf("expected 1, got %d", b.EntryLen())
	}
}

func TestBoltBackend_TombstoneOps(t *testing.T) {
	b := tempDB(t)
	_, ok := b.GetTombstone("missing")
	if ok {
		t.Fatal("expected not found")
	}
	b.PutTombstone("k", []byte{1, 2, 3})
	m, ok := b.GetTombstone("k")
	if !ok || len(m) != 3 || m[0] != 1 {
		t.Fatalf("got m=%v ok=%v", m, ok)
	}
}

func TestBoltBackend_DeleteTombstone(t *testing.T) {
	b := tempDB(t)
	b.PutTombstone("k", []byte{1})
	b.DeleteTombstone("k")
	_, ok := b.GetTombstone("k")
	if ok {
		t.Fatal("expected not found after delete")
	}
}

func TestBoltBackend_RangeTombstones(t *testing.T) {
	b := tempDB(t)
	b.PutTombstone("a", []byte{1})
	b.PutTombstone("b", []byte{2})
	count := 0
	b.RangeTombstones(func(key string, meta []byte) bool {
		count++
		return true
	})
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestBoltBackend_TombstoneLen(t *testing.T) {
	b := tempDB(t)
	if b.TombstoneLen() != 0 {
		t.Fatal("expected 0")
	}
	b.PutTombstone("a", []byte{1})
	if b.TombstoneLen() != 1 {
		t.Fatalf("expected 1, got %d", b.TombstoneLen())
	}
}

func TestBoltBackend_EmptyValueAndMeta(t *testing.T) {
	b := tempDB(t)
	b.PutEntry("k", nil, nil)
	v, m, ok := b.GetEntry("k")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if len(v) != 0 || len(m) != 0 {
		t.Fatalf("expected empty, got v=%v m=%v", v, m)
	}
}

func TestBoltBackend_LargeValueMeta(t *testing.T) {
	b := tempDB(t)
	bigVal := make([]byte, 10000)
	for i := range bigVal {
		bigVal[i] = byte(i % 256)
	}
	bigMeta := []byte{42, 43, 44}
	b.PutEntry("big", bigVal, bigMeta)
	v, m, ok := b.GetEntry("big")
	if !ok {
		t.Fatal("expected key")
	}
	if len(v) != 10000 || v[0] != 0 || v[9999] != byte(9999%256) {
		t.Fatal("value mismatch")
	}
	if len(m) != 3 || m[0] != 42 {
		t.Fatal("meta mismatch")
	}
}

func TestBoltBackend_EncodeDecodeEntry(t *testing.T) {
	val := []byte("hello")
	meta := []byte{1, 2, 3}
	encoded := encodeEntry(val, meta)
	dv, dm := decodeEntry(encoded)
	if string(dv) != "hello" || len(dm) != 3 || dm[0] != 1 {
		t.Fatalf("got dv=%s dm=%v", dv, dm)
	}
}

func TestBoltBackend_EncodeDecodeEntryEmpty(t *testing.T) {
	encoded := encodeEntry(nil, nil)
	dv, dm := decodeEntry(encoded)
	if len(dv) != 0 || len(dm) != 0 {
		t.Fatalf("expected empty, got dv=%v dm=%v", dv, dm)
	}
}

func TestDecodeEntry_ShortData(t *testing.T) {
	v, m := decodeEntry([]byte{1, 2}) // too short
	if v != nil || m != nil {
		t.Fatal("expected nil for short data")
	}
}

// --- Integration: use BoltBackend with user-facing CRDT types ---

func TestBoltBackend_WithLWWMap(t *testing.T) {
	b := tempDB(t)
	r := crdt.NewLWWMap[string](1, crdt.StringCodec{}, crdt.WithBackend(b))
	ctx := context.Background()

	r.Put(ctx, "name", "alice")
	r.Put(ctx, "age", "30")

	v, ok := r.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
	if r.Len() != 2 {
		t.Fatalf("expected 2, got %d", r.Len())
	}
}

func TestBoltBackend_WithLWWMap_Convergence(t *testing.T) {
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

	a.Put(ctx, "from-a", "a-val")
	b.Put(ctx, "from-b", "b-val")

	eventually(t, pollTimeout, func() bool {
		return a.Len() == 2 && b.Len() == 2
	})
}

func TestBoltBackend_WithORSet(t *testing.T) {
	b := tempDB(t)
	r := crdt.NewORSet[string](1, crdt.StringCodec{}, crdt.WithBackend(b))
	ctx := context.Background()

	r.Add(ctx, "alice")
	r.Add(ctx, "bob")

	if !r.Contains("alice") || !r.Contains("bob") {
		t.Fatal("expected both elements")
	}
	if r.Len() != 2 {
		t.Fatalf("expected 2, got %d", r.Len())
	}

	r.Remove(ctx, "alice")
	if r.Contains("alice") {
		t.Fatal("alice should be removed")
	}
}

func TestBoltBackend_WithORMap(t *testing.T) {
	db := tempDB(t)
	r := crdt.NewORMap[string](1, crdt.StringCodec{}, crdt.WithBackend(db))
	ctx := context.Background()

	r.Put(ctx, "key", "value")
	v, ok := r.Get("key")
	if !ok || v != "value" {
		t.Fatalf("expected value, got %v", v)
	}
}

func TestBoltBackend_WithGList(t *testing.T) {
	b := tempDB(t)
	r := crdt.NewGList[string](1, crdt.StringCodec{}, crdt.WithBackend(b))
	ctx := context.Background()

	r.Append(ctx, "first")
	r.Append(ctx, "second")

	items, err := r.Items()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0] != "first" || items[1] != "second" {
		t.Fatalf("expected [first second], got %v", items)
	}
}

func TestBoltBackend_Snapshot(t *testing.T) {
	b := tempDB(t)
	b.PutEntry("k", []byte("val"), []byte{1})

	dir := t.TempDir()
	snapPath := filepath.Join(dir, "snapshot.db")
	f, err := os.Create(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Snapshot(f); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()

	restored, err := Open(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()

	v, _, ok := restored.GetEntry("k")
	if !ok || string(v) != "val" {
		t.Fatalf("expected val, got %v ok=%v", v, ok)
	}
}
