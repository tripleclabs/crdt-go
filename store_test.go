package crdt

import "testing"

func TestMapStore_PutGet(t *testing.T) {
	s := NewMapStore()
	s.Put("a", []byte("hello"))

	v, ok := s.Get("a")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(v) != "hello" {
		t.Fatalf("got %q, want %q", v, "hello")
	}

	_, ok = s.Get("missing")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestMapStore_Delete(t *testing.T) {
	s := NewMapStore()
	s.Put("a", []byte("hello"))
	s.Delete("a")

	_, ok := s.Get("a")
	if ok {
		t.Fatal("expected key to be deleted")
	}

	// Delete non-existent key is a no-op.
	s.Delete("nonexistent")
}

func TestMapStore_Range(t *testing.T) {
	s := NewMapStore()
	s.Put("a", []byte("1"))
	s.Put("b", []byte("2"))
	s.Put("c", []byte("3"))

	seen := make(map[string]string)
	s.Range(func(key string, value []byte) bool {
		seen[key] = string(value)
		return true
	})
	if len(seen) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(seen))
	}

	// Early stop.
	count := 0
	s.Range(func(key string, value []byte) bool {
		count++
		return false
	})
	if count != 1 {
		t.Fatalf("expected early stop after 1 iteration, got %d", count)
	}
}

func TestMapStore_Len(t *testing.T) {
	s := NewMapStore()
	if s.Len() != 0 {
		t.Fatalf("expected 0, got %d", s.Len())
	}
	s.Put("a", []byte("1"))
	s.Put("b", []byte("2"))
	if s.Len() != 2 {
		t.Fatalf("expected 2, got %d", s.Len())
	}
	s.Delete("a")
	if s.Len() != 1 {
		t.Fatalf("expected 1, got %d", s.Len())
	}
}

func TestMapStore_ZeroValue(t *testing.T) {
	// The zero value should be usable without NewMapStore.
	var s MapStore

	_, ok := s.Get("x")
	if ok {
		t.Fatal("expected false from zero-value store")
	}

	s.Delete("x") // no-op, should not panic

	s.Range(func(key string, value []byte) bool {
		t.Fatal("should not iterate on zero-value store")
		return true
	})

	if s.Len() != 0 {
		t.Fatal("expected 0 length")
	}

	// Put initializes the internal map.
	s.Put("x", []byte("y"))
	v, ok := s.Get("x")
	if !ok || string(v) != "y" {
		t.Fatal("expected put to work on zero-value store")
	}
}

func TestWithStore(t *testing.T) {
	custom := NewMapStore()
	opt := applyOptions([]Option{WithStore(custom)})
	if opt.store != custom {
		t.Fatal("expected custom store to be set")
	}

	// No options: store should be nil.
	opt2 := applyOptions(nil)
	if opt2.store != nil {
		t.Fatal("expected nil store when no options")
	}
}
