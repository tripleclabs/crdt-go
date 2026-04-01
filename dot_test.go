package crdt

import "testing"

func TestDotGT(t *testing.T) {
	tests := []struct {
		name string
		a, b Dot
		want bool
	}{
		{"higher counter wins", Dot{1, 5}, Dot{2, 3}, true},
		{"lower counter loses", Dot{2, 3}, Dot{1, 5}, false},
		{"equal counter lower replica wins", Dot{1, 5}, Dot{2, 5}, true},
		{"equal counter higher replica loses", Dot{2, 5}, Dot{1, 5}, false},
		{"equal dots", Dot{1, 5}, Dot{1, 5}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DotGT(tt.a, tt.b); got != tt.want {
				t.Fatalf("DotGT(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestMaxDot(t *testing.T) {
	dm := DotMap{1: 5, 2: 3, 3: 5}
	got := MaxDot(dm)
	// Replica 1 and 3 both have counter 5. Lower replica (1) wins.
	want := Dot{1, 5}
	if got != want {
		t.Fatalf("MaxDot = %v, want %v", got, want)
	}

	// Empty map.
	zero := MaxDot(DotMap{})
	if zero != (Dot{}) {
		t.Fatalf("MaxDot(empty) = %v, want zero", zero)
	}

	// Single entry.
	single := MaxDot(DotMap{42: 7})
	if single != (Dot{42, 7}) {
		t.Fatalf("MaxDot(single) = %v, want {42, 7}", single)
	}
}

func TestCombineDots(t *testing.T) {
	a := DotMap{1: 5, 2: 3}
	b := DotMap{1: 3, 3: 7}
	got := CombineDots(a, b)
	want := DotMap{1: 5, 2: 3, 3: 7}

	if !DotMapEqual(got, want) {
		t.Fatalf("CombineDots = %v, want %v", got, want)
	}
	// Originals unchanged.
	if a[3] != 0 {
		t.Fatal("original a modified")
	}
}

func TestCombineDotsEmpty(t *testing.T) {
	a := DotMap{1: 5}
	got := CombineDots(a, DotMap{})
	if !DotMapEqual(got, a) {
		t.Fatalf("combine with empty = %v, want %v", got, a)
	}

	got2 := CombineDots(DotMap{}, a)
	if !DotMapEqual(got2, a) {
		t.Fatalf("empty combine with non-empty = %v, want %v", got2, a)
	}
}

func TestNextDot(t *testing.T) {
	dm := DotMap{1: 5, 2: 3}
	got := NextDot(1, dm)
	if got != (Dot{1, 6}) {
		t.Fatalf("NextDot = %v, want {1, 6}", got)
	}

	// New replica.
	got2 := NextDot(99, dm)
	if got2 != (Dot{99, 1}) {
		t.Fatalf("NextDot(new) = %v, want {99, 1}", got2)
	}

	// Empty map.
	got3 := NextDot(1, DotMap{})
	if got3 != (Dot{1, 1}) {
		t.Fatalf("NextDot(empty) = %v, want {1, 1}", got3)
	}
}

func TestDotMember(t *testing.T) {
	dm := DotMap{1: 5, 2: 3}

	if !DotMember(dm, Dot{1, 3}) {
		t.Fatal("expected {1,3} to be member (5 >= 3)")
	}
	if !DotMember(dm, Dot{1, 5}) {
		t.Fatal("expected {1,5} to be member (5 >= 5)")
	}
	if DotMember(dm, Dot{1, 6}) {
		t.Fatal("expected {1,6} to not be member (5 < 6)")
	}
	if DotMember(dm, Dot{99, 1}) {
		t.Fatal("expected {99,1} to not be member (missing replica)")
	}
}

func TestCloneDotMap(t *testing.T) {
	dm := DotMap{1: 5, 2: 3}
	c := CloneDotMap(dm)
	c[1] = 99
	if dm[1] != 5 {
		t.Fatal("clone modified original")
	}
}

func TestDotMapLTE(t *testing.T) {
	tests := []struct {
		name string
		a, b DotMap
		want bool
	}{
		{"equal", DotMap{1: 5}, DotMap{1: 5}, true},
		{"less", DotMap{1: 3}, DotMap{1: 5}, true},
		{"greater", DotMap{1: 5}, DotMap{1: 3}, false},
		{"subset", DotMap{1: 1}, DotMap{1: 1, 2: 1}, true},
		{"superset", DotMap{1: 1, 2: 1}, DotMap{1: 1}, false},
		{"empty lte any", DotMap{}, DotMap{1: 1}, true},
		{"both empty", DotMap{}, DotMap{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DotMapLTE(tt.a, tt.b); got != tt.want {
				t.Fatalf("DotMapLTE = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDotMapEqual(t *testing.T) {
	if !DotMapEqual(DotMap{1: 5, 2: 3}, DotMap{1: 5, 2: 3}) {
		t.Fatal("expected equal")
	}
	if DotMapEqual(DotMap{1: 5}, DotMap{1: 5, 2: 3}) {
		t.Fatal("expected not equal (different len)")
	}
	if DotMapEqual(DotMap{1: 5}, DotMap{1: 6}) {
		t.Fatal("expected not equal (different value)")
	}
}
