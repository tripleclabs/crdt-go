package crdt

import "testing"

func TestLocalClock_Next(t *testing.T) {
	lc := NewLocalClock(42)
	d1 := lc.Next()
	d2 := lc.Next()
	d3 := lc.Next()

	if d1 != (Dot{42, 1}) {
		t.Fatalf("expected {42,1}, got %v", d1)
	}
	if d2 != (Dot{42, 2}) {
		t.Fatalf("expected {42,2}, got %v", d2)
	}
	if d3 != (Dot{42, 3}) {
		t.Fatalf("expected {42,3}, got %v", d3)
	}
}

func TestLocalClock_Current(t *testing.T) {
	lc := NewLocalClock(1)
	if lc.Current() != (Dot{1, 0}) {
		t.Fatal("initial current should be {1,0}")
	}
	lc.Next()
	if lc.Current() != (Dot{1, 1}) {
		t.Fatal("after Next, current should be {1,1}")
	}
}

func TestLocalClock_Counter(t *testing.T) {
	lc := NewLocalClock(1)
	if lc.Counter() != 0 {
		t.Fatal("initial counter should be 0")
	}
	lc.Next()
	lc.Next()
	if lc.Counter() != 2 {
		t.Fatalf("expected 2, got %d", lc.Counter())
	}
}

func TestLocalClock_Replica(t *testing.T) {
	lc := NewLocalClock(99)
	if lc.Replica() != 99 {
		t.Fatalf("expected 99, got %d", lc.Replica())
	}
}

func TestLocalClock_SetCounter(t *testing.T) {
	lc := NewLocalClock(1)
	lc.SetCounter(10)
	d := lc.Next()
	if d != (Dot{1, 11}) {
		t.Fatalf("expected {1,11}, got %v", d)
	}
}
