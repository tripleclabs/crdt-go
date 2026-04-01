package crdt

import (
	"sort"
	"testing"
)

// --- GCounter ---

func TestGCounter_SetGet(t *testing.T) {
	c := NewGCounter()
	c.Set(1, 10)
	c.Set(2, 20)
	if c.Get(1) != 10 || c.Get(2) != 20 || c.Get(99) != 0 {
		t.Fatal("get mismatch")
	}
	if c.Int64() != 30 {
		t.Fatalf("expected 30, got %d", c.Int64())
	}
	if c.Len() != 2 {
		t.Fatalf("expected 2, got %d", c.Len())
	}
}

func TestGCounter_Range(t *testing.T) {
	c := NewGCounter()
	c.Set(1, 5)
	c.Set(2, 3)
	sum := uint64(0)
	c.Range(func(_ ReplicaID, count uint64) bool { sum += count; return true })
	if sum != 8 {
		t.Fatalf("expected 8, got %d", sum)
	}
}

// --- PNCounter ---

func TestPNCounter_SetGet(t *testing.T) {
	c := NewPNCounter()
	c.SetPositive(1, 10)
	c.SetNegative(1, 3)
	if c.GetPositive(1) != 10 || c.GetNegative(1) != 3 {
		t.Fatal("get mismatch")
	}
	if c.Int64() != 7 {
		t.Fatalf("expected 7, got %d", c.Int64())
	}
}

func TestPNCounter_Range(t *testing.T) {
	c := NewPNCounter()
	c.SetPositive(1, 5)
	c.SetNegative(2, 3)
	var posCount, negCount int
	c.RangePositive(func(_ ReplicaID, _ uint64) bool { posCount++; return true })
	c.RangeNegative(func(_ ReplicaID, _ uint64) bool { negCount++; return true })
	if posCount != 1 || negCount != 1 {
		t.Fatalf("expected 1,1, got %d,%d", posCount, negCount)
	}
}

// --- LWWRegister ---

func TestLWWRegister_SetGet(t *testing.T) {
	r := NewLWWRegister(StringCodec{})
	r.Set("hello", Dot{1, 1})
	v, dot, ok := r.Get()
	if !ok || v != "hello" || dot != (Dot{1, 1}) {
		t.Fatalf("expected hello/{1,1}, got %v/%v/%v", v, dot, ok)
	}
}

func TestLWWRegister_SetBytes(t *testing.T) {
	r := NewLWWRegister(StringCodec{})
	r.SetBytes([]byte("hi"), Dot{2, 3})
	b, dot, ok := r.GetBytes()
	if !ok || string(b) != "hi" || dot != (Dot{2, 3}) {
		t.Fatal("bytes mismatch")
	}
}

func TestLWWRegister_Empty(t *testing.T) {
	r := NewLWWRegister(StringCodec{})
	_, _, ok := r.Get()
	if ok {
		t.Fatal("expected not set")
	}
	if r.HasValue() {
		t.Fatal("expected no value")
	}
}

// --- MVRegister ---

func TestMVRegister_SetValues(t *testing.T) {
	r := NewMVRegister(StringCodec{})
	r.Set("hello", Dot{1, 1})
	vals, _ := r.Values()
	if len(vals) != 1 || vals[0] != "hello" {
		t.Fatalf("expected [hello], got %v", vals)
	}
}

func TestMVRegister_SetEntries(t *testing.T) {
	r := NewMVRegister(StringCodec{})
	r.SetEntries([]struct{ ValBytes []byte; Dot Dot }{
		{[]byte("a"), Dot{1, 1}},
		{[]byte("b"), Dot{2, 1}},
	})
	if r.Len() != 2 {
		t.Fatalf("expected 2, got %d", r.Len())
	}
}

func TestMVRegister_RangeEntries(t *testing.T) {
	r := NewMVRegister(StringCodec{})
	r.Set("x", Dot{1, 1})
	count := 0
	r.RangeEntries(func(_ []byte, _ Dot) bool { count++; return true })
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

// --- LWWMap ---

func TestLWWMap_PutGet(t *testing.T) {
	m := NewLWWMap(StringCodec{})
	m.Put("name", "alice", Dot{1, 1})
	v, dot, ok := m.Get("name")
	if !ok || v != "alice" || dot != (Dot{1, 1}) {
		t.Fatalf("got %v %v %v", v, dot, ok)
	}
	if m.Len() != 1 {
		t.Fatalf("expected 1, got %d", m.Len())
	}
}

func TestLWWMap_PutBytes(t *testing.T) {
	m := NewLWWMap(StringCodec{})
	m.PutBytes("k", []byte("val"), Dot{1, 1})
	b, dot, ok := m.GetBytes("k")
	if !ok || string(b) != "val" || dot != (Dot{1, 1}) {
		t.Fatal("bytes mismatch")
	}
}

func TestLWWMap_Remove(t *testing.T) {
	m := NewLWWMap(StringCodec{})
	m.Put("k", "val", Dot{1, 1})
	m.Remove("k", Dot{1, 2})
	_, _, ok := m.Get("k")
	if ok {
		t.Fatal("should be removed")
	}
	dot, tok := m.GetTombstone("k")
	if !tok || dot != (Dot{1, 2}) {
		t.Fatalf("expected tombstone {1,2}, got %v %v", dot, tok)
	}
	if m.TombstoneLen() != 1 {
		t.Fatalf("expected 1 tombstone, got %d", m.TombstoneLen())
	}
}

func TestLWWMap_Range(t *testing.T) {
	m := NewLWWMap(StringCodec{})
	m.Put("a", "1", Dot{1, 1})
	m.Put("b", "2", Dot{1, 2})
	var keys []string
	m.Range(func(k string, _ string, _ Dot) bool { keys = append(keys, k); return true })
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("expected [a b], got %v", keys)
	}
}

func TestLWWMap_RangeTombstones(t *testing.T) {
	m := NewLWWMap(StringCodec{})
	m.Put("k", "val", Dot{1, 1})
	m.Remove("k", Dot{1, 2})
	count := 0
	m.RangeTombstones(func(_ string, _ Dot) bool { count++; return true })
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

// --- ORSet ---

func TestORSet_PutContains(t *testing.T) {
	s := NewORSet(StringCodec{})
	s.Put("alice", DotMap{1: 1})
	if !s.Contains("alice") {
		t.Fatal("should contain alice")
	}
	if s.Len() != 1 {
		t.Fatalf("expected 1, got %d", s.Len())
	}
}

func TestORSet_Remove(t *testing.T) {
	s := NewORSet(StringCodec{})
	s.Put("alice", DotMap{1: 1})
	s.Remove("alice", VClock{})
	if s.Contains("alice") {
		t.Fatal("should be removed")
	}
}

func TestORSet_GetDotMap(t *testing.T) {
	s := NewORSet(StringCodec{})
	s.Put("x", DotMap{1: 1, 2: 3})
	dm, ok := s.Get("x")
	if !ok || dm[1] != 1 || dm[2] != 3 {
		t.Fatalf("dotmap mismatch: %v", dm)
	}
}

func TestORSet_Elements(t *testing.T) {
	s := NewORSet(StringCodec{})
	s.Put("b", DotMap{1: 1})
	s.Put("a", DotMap{1: 2})
	elems, _ := s.Elements()
	sort.Strings(elems)
	if len(elems) != 2 || elems[0] != "a" || elems[1] != "b" {
		t.Fatalf("expected [a b], got %v", elems)
	}
}

func TestORSet_Range(t *testing.T) {
	s := NewORSet(StringCodec{})
	s.Put("x", DotMap{1: 1})
	count := 0
	s.Range(func(_ string, _ DotMap) bool { count++; return true })
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

// --- ORMap ---

func TestORMap_PutGet(t *testing.T) {
	m := NewORMap(StringCodec{})
	m.Put("k", "val", Dot{1, 1})
	v, dm, ok := m.Get("k")
	if !ok || v != "val" || dm[1] != 1 {
		t.Fatalf("got %v %v %v", v, dm, ok)
	}
}

func TestORMap_Remove(t *testing.T) {
	m := NewORMap(StringCodec{})
	m.Put("k", "val", Dot{1, 1})
	m.Remove("k", VClock{1: 1})
	_, _, ok := m.Get("k")
	if ok {
		t.Fatal("should be removed")
	}
}

func TestORMap_Range(t *testing.T) {
	m := NewORMap(StringCodec{})
	m.Put("a", "1", Dot{1, 1})
	m.Put("b", "2", Dot{1, 2})
	count := 0
	m.Range(func(_ string, _ string, _ DotMap) bool { count++; return true })
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

// --- AWLWWMap ---

func TestAWLWWMap_PutGet(t *testing.T) {
	m := NewAWLWWMap(StringCodec{})
	m.Put("k", "val", Dot{1, 1})
	v, dot, ok := m.Get("k")
	if !ok || v != "val" || dot != (Dot{1, 1}) {
		t.Fatalf("got %v %v %v", v, dot, ok)
	}
}

func TestAWLWWMap_Remove(t *testing.T) {
	m := NewAWLWWMap(StringCodec{})
	m.Put("k", "val", Dot{1, 1})
	m.Remove("k", Dot{1, 2}, VClock{1: 1})
	_, _, ok := m.Get("k")
	if ok {
		t.Fatal("should be removed")
	}
	dot, ctx, tok := m.GetTombstone("k")
	if !tok || dot != (Dot{1, 2}) || ctx.Get(1) != 1 {
		t.Fatalf("tombstone mismatch: %v %v", dot, ctx)
	}
}

// --- GList ---

func TestGList_Append(t *testing.T) {
	l := NewGList(StringCodec{})
	l.Append("first", Dot{1, 1})
	l.Append("second", Dot{1, 2})
	items, _ := l.Items()
	if len(items) != 2 || items[0] != "first" || items[1] != "second" {
		t.Fatalf("expected [first second], got %v", items)
	}
	if l.Len() != 2 {
		t.Fatalf("expected 2, got %d", l.Len())
	}
}

func TestGList_Has(t *testing.T) {
	l := NewGList(StringCodec{})
	l.Append("x", Dot{1, 1})
	if !l.Has(Dot{1, 1}) {
		t.Fatal("should have dot {1,1}")
	}
	if l.Has(Dot{1, 2}) {
		t.Fatal("should not have dot {1,2}")
	}
}

func TestGList_CausalOrder(t *testing.T) {
	l := NewGList(StringCodec{})
	l.Append("b1", Dot{2, 1})
	l.Append("a1", Dot{1, 1})
	items, _ := l.Items()
	if items[0] != "a1" || items[1] != "b1" {
		t.Fatalf("expected causal order [a1 b1], got %v", items)
	}
}

func TestGList_AppendBytes(t *testing.T) {
	l := NewGList(StringCodec{})
	l.AppendBytes([]byte("raw"), Dot{1, 1})
	if l.Len() != 1 {
		t.Fatal("expected 1")
	}
}

// --- AWLWWMap additional ---

func TestAWLWWMap_PutBytes(t *testing.T) {
	m := NewAWLWWMap(StringCodec{})
	m.PutBytes("k", []byte("val"), Dot{1, 1})
	b, dot, ok := m.GetBytes("k")
	if !ok || string(b) != "val" || dot != (Dot{1, 1}) {
		t.Fatal("bytes mismatch")
	}
}

func TestAWLWWMap_Range(t *testing.T) {
	m := NewAWLWWMap(StringCodec{})
	m.Put("a", "1", Dot{1, 1})
	m.Put("b", "2", Dot{1, 2})
	count := 0
	m.Range(func(_ string, _ string, _ Dot) bool { count++; return true })
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
	if m.Len() != 2 {
		t.Fatalf("expected 2, got %d", m.Len())
	}
}

func TestAWLWWMap_RangeTombstones(t *testing.T) {
	m := NewAWLWWMap(StringCodec{})
	m.Put("k", "val", Dot{1, 1})
	m.Remove("k", Dot{1, 2}, VClock{1: 1})
	count := 0
	m.RangeTombstones(func(_ string, _ Dot, _ VClock) bool { count++; return true })
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

// --- ORMap additional ---

func TestORMap_PutBytesGetBytes(t *testing.T) {
	m := NewORMap(StringCodec{})
	m.PutBytes("k", []byte("val"), DotMap{1: 1})
	b, dm, ok := m.GetBytes("k")
	if !ok || string(b) != "val" || dm[1] != 1 {
		t.Fatal("bytes mismatch")
	}
	if m.Len() != 1 {
		t.Fatalf("expected 1, got %d", m.Len())
	}
}

func TestORMap_RangeBytes(t *testing.T) {
	m := NewORMap(StringCodec{})
	m.PutBytes("k", []byte("v"), DotMap{1: 1})
	count := 0
	m.RangeBytes(func(_ string, _ []byte, _ DotMap) bool { count++; return true })
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

// --- ORSet additional ---

func TestORSet_EncodedOps(t *testing.T) {
	s := NewORSet(StringCodec{})
	s.PutEncoded("alice", DotMap{1: 1})
	dm, ok := s.GetEncoded("alice")
	if !ok || dm[1] != 1 {
		t.Fatal("encoded mismatch")
	}
	s.RemoveEncoded("alice")
	if s.Contains("alice") {
		t.Fatal("should be removed")
	}
}

// --- LWWMap additional ---

func TestLWWMap_RangeBytes(t *testing.T) {
	m := NewLWWMap(StringCodec{})
	m.PutBytes("k", []byte("v"), Dot{1, 1})
	count := 0
	m.RangeBytes(func(_ string, _ []byte, _ Dot) bool { count++; return true })
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

// --- WithBackend ---

func TestWithBackend(t *testing.T) {
	b := NewMemoryBackend()
	m := NewLWWMap(StringCodec{}, WithBackend(b))
	m.Put("k", "v", Dot{1, 1})
	// Verify it used the provided backend.
	_, _, ok := b.GetEntry("k")
	if !ok {
		t.Fatal("should use provided backend")
	}
}

func TestGList_Range(t *testing.T) {
	l := NewGList(StringCodec{})
	l.Append("x", Dot{1, 1})
	count := 0
	l.Range(func(_ []byte, _ Dot) bool { count++; return true })
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}
