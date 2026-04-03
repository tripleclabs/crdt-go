package crdt

import "testing"

func TestReceivedClock_InOrder(t *testing.T) {
	rc := newReceivedClock()
	rc.Record(1, 1)
	rc.Record(1, 2)
	rc.Record(1, 3)

	if rc.Get(1) != 3 {
		t.Fatalf("expected hwm 3, got %d", rc.Get(1))
	}
}

func TestReceivedClock_OutOfOrder(t *testing.T) {
	rc := newReceivedClock()
	rc.Record(1, 3) // gap: missing 1, 2
	rc.Record(1, 1)
	// hwm should be 1 (still missing 2)
	if rc.Get(1) != 1 {
		t.Fatalf("expected hwm 1, got %d", rc.Get(1))
	}

	rc.Record(1, 2) // fills gap → hwm advances to 3
	if rc.Get(1) != 3 {
		t.Fatalf("expected hwm 3, got %d", rc.Get(1))
	}
}

func TestReceivedClock_OutOfOrder_Large(t *testing.T) {
	rc := newReceivedClock()
	// Receive 5, 3, 1, 4, 2
	rc.Record(1, 5)
	rc.Record(1, 3)
	rc.Record(1, 1)
	if rc.Get(1) != 1 {
		t.Fatalf("expected 1, got %d", rc.Get(1))
	}
	rc.Record(1, 4)
	if rc.Get(1) != 1 {
		t.Fatalf("expected 1, got %d", rc.Get(1))
	}
	rc.Record(1, 2) // fills 2→3→4→5
	if rc.Get(1) != 5 {
		t.Fatalf("expected 5, got %d", rc.Get(1))
	}
}

func TestReceivedClock_MultipleReplicas(t *testing.T) {
	rc := newReceivedClock()
	rc.Record(1, 1)
	rc.Record(1, 2)
	rc.Record(2, 1)

	if rc.Get(1) != 2 {
		t.Fatalf("expected replica 1 hwm 2, got %d", rc.Get(1))
	}
	if rc.Get(2) != 1 {
		t.Fatalf("expected replica 2 hwm 1, got %d", rc.Get(2))
	}
	if rc.Get(99) != 0 {
		t.Fatal("unknown replica should be 0")
	}
}

func TestReceivedClock_Duplicate(t *testing.T) {
	rc := newReceivedClock()
	rc.Record(1, 1)
	rc.Record(1, 1) // duplicate
	rc.Record(1, 1) // duplicate
	if rc.Get(1) != 1 {
		t.Fatalf("expected 1, got %d", rc.Get(1))
	}
}

func TestReceivedClock_Zero(t *testing.T) {
	rc := newReceivedClock()
	rc.Record(1, 0) // should be no-op
	if rc.Get(1) != 0 {
		t.Fatal("counter 0 should be ignored")
	}
}

func TestReceivedClock_Covers(t *testing.T) {
	rc := newReceivedClock()
	rc.Record(1, 1)
	rc.Record(1, 3) // pending

	if !rc.Covers(1, 1) {
		t.Fatal("should cover 1 (at hwm)")
	}
	if rc.Covers(1, 2) {
		t.Fatal("should not cover 2 (gap)")
	}
	if !rc.Covers(1, 3) {
		t.Fatal("should cover 3 (in pending)")
	}
	if rc.Covers(1, 4) {
		t.Fatal("should not cover 4")
	}
}

func TestReceivedClock_HWM(t *testing.T) {
	rc := newReceivedClock()
	rc.Record(1, 1)
	rc.Record(1, 2)
	rc.Record(2, 1)

	hwm := rc.HWM()
	if hwm.Get(1) != 2 || hwm.Get(2) != 1 {
		t.Fatalf("unexpected hwm: %v", hwm)
	}
}

func TestReceivedClock_SetHWM(t *testing.T) {
	rc := newReceivedClock()
	rc.Record(1, 4) // pending
	rc.Record(1, 5) // pending
	rc.SetHWM(1, 3) // set hwm to 3, pending has 4 and 5 → advances to 5
	if rc.Get(1) != 5 {
		t.Fatalf("expected 5, got %d", rc.Get(1))
	}
}

func TestReceivedClock_SetHWM_WithGap(t *testing.T) {
	rc := newReceivedClock()
	rc.Record(1, 5) // pending, missing 4
	rc.SetHWM(1, 3) // hwm to 3, but 4 is missing → stays at 3
	if rc.Get(1) != 3 {
		t.Fatalf("expected 3 (gap at 4), got %d", rc.Get(1))
	}
}

func TestReceivedClock_Clone(t *testing.T) {
	rc := newReceivedClock()
	rc.Record(1, 1)
	rc.Record(1, 3) // pending

	c := rc.Clone()
	c.Record(1, 2) // should advance clone to 3

	if c.Get(1) != 3 {
		t.Fatalf("clone should be 3, got %d", c.Get(1))
	}
	if rc.Get(1) != 1 {
		t.Fatalf("original should still be 1, got %d", rc.Get(1))
	}
}

func TestReceivedClock_BelowHWM(t *testing.T) {
	rc := newReceivedClock()
	rc.Record(1, 1)
	rc.Record(1, 2)
	rc.Record(1, 3)

	// Recording something already below hwm is a no-op.
	rc.Record(1, 1)
	if rc.Get(1) != 3 {
		t.Fatalf("expected 3, got %d", rc.Get(1))
	}
}
