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

func TestBoltBackend_EntryOps(t *testing.T) {
	b := tempDB(t)
	b.PutEntry("a", []byte("hello"), []byte{1, 2})

	v, m, ok := b.GetEntry("a")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(v) != "hello" {
		t.Fatalf("got value %q, want %q", v, "hello")
	}
	if len(m) != 2 || m[0] != 1 || m[1] != 2 {
		t.Fatalf("got meta %v, want [1 2]", m)
	}

	_, _, ok = b.GetEntry("missing")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestBoltBackend_DeleteEntry(t *testing.T) {
	b := tempDB(t)
	b.PutEntry("a", []byte("hello"), []byte{1})
	b.DeleteEntry("a")

	_, _, ok := b.GetEntry("a")
	if ok {
		t.Fatal("expected key to be deleted")
	}
	b.DeleteEntry("nonexistent") // no-op
}

func TestBoltBackend_RangeEntries(t *testing.T) {
	b := tempDB(t)
	b.PutEntry("a", []byte("1"), nil)
	b.PutEntry("b", []byte("2"), nil)
	b.PutEntry("c", []byte("3"), nil)

	count := 0
	b.RangeEntries(func(key string, value []byte, meta []byte) bool {
		count++
		return true
	})
	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}

	// Early stop.
	stopped := 0
	b.RangeEntries(func(key string, value []byte, meta []byte) bool {
		stopped++
		return false
	})
	if stopped != 1 {
		t.Fatalf("expected early stop after 1, got %d", stopped)
	}
}

func TestBoltBackend_EntryLen(t *testing.T) {
	b := tempDB(t)
	if b.EntryLen() != 0 {
		t.Fatal("expected 0")
	}
	b.PutEntry("a", []byte("1"), nil)
	b.PutEntry("b", []byte("2"), nil)
	if b.EntryLen() != 2 {
		t.Fatalf("expected 2, got %d", b.EntryLen())
	}
}

func TestBoltBackend_TombstoneOps(t *testing.T) {
	b := tempDB(t)
	b.PutTombstone("a", []byte{3, 4})

	m, ok := b.GetTombstone("a")
	if !ok {
		t.Fatal("expected tombstone")
	}
	if len(m) != 2 || m[0] != 3 {
		t.Fatalf("got %v, want [3 4]", m)
	}

	_, ok = b.GetTombstone("missing")
	if ok {
		t.Fatal("expected false")
	}
}

func TestBoltBackend_DeleteTombstone(t *testing.T) {
	b := tempDB(t)
	b.PutTombstone("a", []byte{1})
	b.DeleteTombstone("a")
	_, ok := b.GetTombstone("a")
	if ok {
		t.Fatal("expected tombstone to be deleted")
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
		t.Fatalf("expected empty value and meta, got v=%v m=%v", v, m)
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

// --- Integration: use BoltBackend with CRDT types ---

func TestBoltBackend_WithLWWMap(t *testing.T) {
	b := tempDB(t)
	m := crdt.NewLWWMap(1, crdt.WithBackend(b))

	m.Put("name", "alice")
	m.Put("age", 30)

	v, ok := m.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
	v, ok = m.Get("age")
	if !ok || v != 30 {
		t.Fatalf("expected 30, got %v", v)
	}
	if m.Len() != 2 {
		t.Fatalf("expected 2 entries, got %d", m.Len())
	}
}

func TestBoltBackend_WithLWWMap_Merge(t *testing.T) {
	ba := tempDB(t)
	bb := tempDB(t)

	a := crdt.NewLWWMap(1, crdt.WithBackend(ba))
	b := crdt.NewLWWMap(2, crdt.WithBackend(bb))

	a.Put("from-a", "a-val")
	b.Put("from-b", "b-val")

	a.Merge(b)
	if a.Len() != 2 {
		t.Fatalf("expected 2, got %d", a.Len())
	}
}

func TestBoltBackend_WithORSet(t *testing.T) {
	b := tempDB(t)
	s := crdt.NewORSet(1, crdt.WithBackend(b))

	s.Add("alice")
	s.Add("bob")

	if !s.Contains("alice") || !s.Contains("bob") {
		t.Fatal("expected both elements")
	}
	if s.Len() != 2 {
		t.Fatalf("expected 2, got %d", s.Len())
	}

	s.Remove("alice")
	if s.Contains("alice") {
		t.Fatal("alice should be removed")
	}
}

func TestBoltBackend_WithORMap(t *testing.T) {
	b := tempDB(t)
	m := crdt.NewORMap(1, crdt.WithBackend(b))

	m.Put("key", "value")
	v, ok := m.Get("key")
	if !ok || v != "value" {
		t.Fatalf("expected value, got %v", v)
	}
}

func TestBoltBackend_WithGList(t *testing.T) {
	b := tempDB(t)
	l := crdt.NewGList(1, crdt.WithBackend(b))

	l.Append("first")
	l.Append("second")

	items := l.Items()
	if len(items) != 2 || items[0] != "first" || items[1] != "second" {
		t.Fatalf("expected [first second], got %v", items)
	}
}

func TestBoltBackend_Snapshot(t *testing.T) {
	b := tempDB(t)
	b.PutEntry("k", []byte("val"), []byte{1})

	// Snapshot to a temp file.
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

	// Open snapshot and verify data.
	restored, err := Open(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()

	v, _, ok := restored.GetEntry("k")
	if !ok || string(v) != "val" {
		t.Fatal("snapshot missing data")
	}
}

func TestBoltBackend_EncodeDecodeEntry(t *testing.T) {
	value := []byte("hello world")
	meta := []byte{1, 2, 3}
	encoded := encodeEntry(value, meta)

	v, m := decodeEntry(encoded)
	if string(v) != "hello world" {
		t.Fatalf("value mismatch: %q", v)
	}
	if len(m) != 3 || m[0] != 1 || m[1] != 2 || m[2] != 3 {
		t.Fatalf("meta mismatch: %v", m)
	}
}

func TestBoltBackend_EncodeDecodeEntryEmpty(t *testing.T) {
	encoded := encodeEntry(nil, nil)
	v, m := decodeEntry(encoded)
	if len(v) != 0 || len(m) != 0 {
		t.Fatalf("expected empty, got v=%v m=%v", v, m)
	}
}

func TestDecodeEntry_ShortData(t *testing.T) {
	v, m := decodeEntry([]byte{1})
	if v != nil || m != nil {
		t.Fatal("expected nil for short data")
	}
}
