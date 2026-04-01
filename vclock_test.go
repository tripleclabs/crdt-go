package crdt

import "testing"

func TestNewVClock(t *testing.T) {
	vc := NewVClock()
	if len(vc) != 0 {
		t.Fatal("expected empty vclock")
	}
}

func TestVClock_Get(t *testing.T) {
	vc := VClock{1: 5, 2: 3}
	if vc.Get(1) != 5 {
		t.Fatal("expected 5")
	}
	if vc.Get(99) != 0 {
		t.Fatal("expected 0 for missing replica")
	}
}

func TestVClock_Increment(t *testing.T) {
	vc := VClock{1: 5, 2: 3}
	inc := vc.Increment(1)

	if inc.Get(1) != 6 {
		t.Fatalf("expected 6, got %d", inc.Get(1))
	}
	// Original unchanged.
	if vc.Get(1) != 5 {
		t.Fatal("original should not be modified")
	}

	// Increment new replica.
	inc2 := vc.Increment(3)
	if inc2.Get(3) != 1 {
		t.Fatalf("expected 1, got %d", inc2.Get(3))
	}
}

func TestVClock_Merge(t *testing.T) {
	a := VClock{1: 5, 2: 3}
	b := VClock{1: 4, 3: 2}
	m := a.Merge(b)

	want := VClock{1: 5, 2: 3, 3: 2}
	if !m.Equal(want) {
		t.Fatalf("got %v, want %v", m, want)
	}
	// Originals unchanged.
	if a.Get(3) != 0 {
		t.Fatal("original a should not be modified")
	}
}

func TestVClock_MergeEmpty(t *testing.T) {
	a := VClock{1: 5}
	m := a.Merge(VClock{})
	if !m.Equal(a) {
		t.Fatal("merge with empty should equal original")
	}

	m2 := (VClock{}).Merge(a)
	if !m2.Equal(a) {
		t.Fatal("empty merge with non-empty should equal other")
	}
}

func TestVClock_LTE(t *testing.T) {
	tests := []struct {
		name string
		a, b VClock
		want bool
	}{
		{"equal", VClock{1: 2, 2: 1}, VClock{1: 2, 2: 1}, true},
		{"strictly less", VClock{1: 2}, VClock{1: 3}, true},
		{"subset replicas", VClock{1: 1}, VClock{1: 1, 2: 1}, true},
		{"strictly greater", VClock{1: 3}, VClock{1: 2}, false},
		{"more replicas", VClock{1: 1, 2: 1}, VClock{1: 1}, false},
		{"concurrent", VClock{1: 2, 2: 1}, VClock{1: 1, 2: 2}, false},
		{"empty lte any", VClock{}, VClock{1: 1}, true},
		{"both empty", VClock{}, VClock{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.LTE(tt.b); got != tt.want {
				t.Fatalf("LTE = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVClock_Dominates(t *testing.T) {
	a := VClock{1: 3}
	b := VClock{1: 2}
	if !a.Dominates(b) {
		t.Fatal("expected a to dominate b")
	}
	if b.Dominates(a) {
		t.Fatal("b should not dominate a")
	}
	if !a.Dominates(a) {
		t.Fatal("a should dominate itself")
	}
}

func TestVClock_Concurrent(t *testing.T) {
	a := VClock{1: 2, 2: 1}
	b := VClock{1: 1, 2: 2}
	if !a.Concurrent(b) {
		t.Fatal("expected concurrent")
	}

	c := VClock{1: 1}
	d := VClock{1: 2}
	if c.Concurrent(d) {
		t.Fatal("c <= d, not concurrent")
	}

	if a.Concurrent(a) {
		t.Fatal("equal clocks are not concurrent")
	}
}

func TestVClock_Equal(t *testing.T) {
	a := VClock{1: 5, 2: 3}
	b := VClock{1: 5, 2: 3}
	if !a.Equal(b) {
		t.Fatal("expected equal")
	}

	c := VClock{1: 5, 2: 4}
	if a.Equal(c) {
		t.Fatal("expected not equal")
	}

	d := VClock{1: 5}
	if a.Equal(d) {
		t.Fatal("expected not equal (different length)")
	}
}

func TestVClock_Clone(t *testing.T) {
	a := VClock{1: 5, 2: 3}
	b := a.Clone()
	b[1] = 99
	if a.Get(1) != 5 {
		t.Fatal("clone modified original")
	}
}

func TestVClock_LowerBound(t *testing.T) {
	a := VClock{1: 5, 2: 3}
	b := VClock{1: 4, 2: 6}
	lb := a.LowerBound(b)
	want := VClock{1: 4, 2: 3}
	if !lb.Equal(want) {
		t.Fatalf("got %v, want %v", lb, want)
	}

	// No shared replicas.
	c := VClock{1: 5}
	d := VClock{2: 3}
	lb2 := c.LowerBound(d)
	if len(lb2) != 0 {
		t.Fatal("expected empty lower bound")
	}
}

func TestVClock_Fingerprint(t *testing.T) {
	a := VClock{1: 5, 2: 3}
	b := VClock{1: 5, 2: 3}
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatal("equal clocks should have same fingerprint")
	}

	c := VClock{1: 5, 2: 4}
	if a.Fingerprint() == c.Fingerprint() {
		t.Fatal("different clocks should (almost certainly) have different fingerprints")
	}

	// Empty clock fingerprint should be consistent.
	e := VClock{}
	if e.Fingerprint() != (VClock{}).Fingerprint() {
		t.Fatal("empty clock fingerprints should match")
	}
}

func TestVClock_NilReceiver(t *testing.T) {
	var vc VClock
	if vc.Get(1) != 0 {
		t.Fatal("nil vclock Get should return 0")
	}
	if !vc.LTE(VClock{1: 1}) {
		t.Fatal("nil vclock should be LTE anything")
	}
	if !vc.Equal(VClock{}) {
		t.Fatal("nil vclock should equal empty vclock")
	}
	if vc.Fingerprint() != (VClock{}).Fingerprint() {
		t.Fatal("nil vclock fingerprint should match empty")
	}
}
