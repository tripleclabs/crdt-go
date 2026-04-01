package crdtbolt

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/3clabs/crdt"
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

// --- Integration: use BoltBackend with Replica types ---

func TestBoltBackend_WithLWWMap(t *testing.T) {
	b := tempDB(t)
	r := crdt.NewLWWMapReplica[string](1, crdt.StringCodec{}, crdt.WithBackend(b))

	r.Data.Put("name", "alice", r.NextDot())
	r.Data.Put("age", "30", r.NextDot())

	v, _, ok := r.Data.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
	if r.Data.Len() != 2 {
		t.Fatalf("expected 2, got %d", r.Data.Len())
	}
}

func TestBoltBackend_WithLWWMap_Convergence(t *testing.T) {
	ba := tempDB(t)
	bb := tempDB(t)

	a := crdt.NewLWWMapReplica[string](1, crdt.StringCodec{}, crdt.WithBackend(ba))
	b := crdt.NewLWWMapReplica[string](2, crdt.StringCodec{}, crdt.WithBackend(bb))

	da, _ := a.Data.Put("from-a", "a-val", a.NextDot())
	db, _ := b.Data.Put("from-b", "b-val", b.NextDot())

	a.ApplyDelta(db)
	b.ApplyDelta(da)

	if a.Data.Len() != 2 || b.Data.Len() != 2 {
		t.Fatalf("expected both 2, got a=%d b=%d", a.Data.Len(), b.Data.Len())
	}
}

func TestBoltBackend_WithORSet(t *testing.T) {
	b := tempDB(t)
	r := crdt.NewORSetReplica[string](1, crdt.StringCodec{}, crdt.WithBackend(b))

	r.Data.Add("alice", r.NextDot())
	r.Data.Add("bob", r.NextDot())

	if !r.Data.Contains("alice") || !r.Data.Contains("bob") {
		t.Fatal("expected both elements")
	}
	if r.Data.Len() != 2 {
		t.Fatalf("expected 2, got %d", r.Data.Len())
	}

	r.Data.Remove("alice", r.HWM())
	if r.Data.Contains("alice") {
		t.Fatal("alice should be removed")
	}
}

func TestBoltBackend_WithORMap(t *testing.T) {
	b := tempDB(t)
	r := crdt.NewORMapReplica[string](1, crdt.StringCodec{}, crdt.WithBackend(b))

	r.Data.Put("key", "value", r.NextDot())
	v, _, ok := r.Data.Get("key")
	if !ok || v != "value" {
		t.Fatalf("expected value, got %v", v)
	}
}

func TestBoltBackend_WithGList(t *testing.T) {
	b := tempDB(t)
	r := crdt.NewGListReplica[string](1, crdt.StringCodec{}, crdt.WithBackend(b))

	r.Data.Append("first", r.NextDot())
	r.Data.Append("second", r.NextDot())

	items, err := r.Data.Items()
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
