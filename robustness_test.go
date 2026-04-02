package crdt

import (
	"fmt"
	"sort"
	"testing"
)

// sortedValues returns the Values of an MVRegister sorted for deterministic comparison.
func sortedValues(t *testing.T, r *Replica[*MVRegister[string]]) []string {
	t.Helper()
	vals, err := r.Data.Values()
	if err != nil {
		t.Fatalf("Values() error: %v", err)
	}
	sort.Strings(vals)
	return vals
}

// ---------- 1. Corrupted Delta Tests ----------

func TestCorruptedDelta_AllTypes(t *testing.T) {
	corruptCases := []struct {
		name  string
		delta []byte
	}{
		{"empty", []byte{}},
		{"single_byte_0xFF", []byte{0xFF}},
		{"truncated_put", append([]byte{OpPut}, []byte("key")...)},
		{"random_garbage", []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}},
		{"wrong_length_dot_15bytes", []byte{
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
			0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
		}},
		{"truncated_varint_prefix", []byte{0x80, 0x80, 0x80}},
		{"large_varint_length", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x07}},
	}

	type replicaCase struct {
		name  string
		apply func(delta []byte) error
	}

	replicas := []replicaCase{
		{"LWWMap", func(d []byte) error {
			r := NewLWWMapReplica[string](1, StringCodec{})
			return r.ApplyDelta(d)
		}},
		{"ORSet", func(d []byte) error {
			r := NewORSetReplica[string](1, StringCodec{})
			return r.ApplyDelta(d)
		}},
		{"ORMap", func(d []byte) error {
			r := NewORMapReplica[string](1, StringCodec{})
			return r.ApplyDelta(d)
		}},
		{"AWLWWMap", func(d []byte) error {
			r := NewAWLWWMapReplica[string](1, StringCodec{})
			return r.ApplyDelta(d)
		}},
		{"GList", func(d []byte) error {
			r := NewGListReplica[string](1, StringCodec{})
			return r.ApplyDelta(d)
		}},
		{"GCounter", func(d []byte) error {
			r := NewGCounterReplica(1)
			return r.ApplyDelta(d)
		}},
		{"PNCounter", func(d []byte) error {
			r := NewPNCounterReplica(1)
			return r.ApplyDelta(d)
		}},
		{"LWWRegister", func(d []byte) error {
			r := NewLWWRegisterReplica[string](1, StringCodec{})
			return r.ApplyDelta(d)
		}},
		{"MVRegister", func(d []byte) error {
			r := NewMVRegisterReplica[string](1, StringCodec{})
			return r.ApplyDelta(d)
		}},
	}

	for _, rc := range replicas {
		t.Run(rc.name, func(t *testing.T) {
			for _, cc := range corruptCases {
				t.Run(cc.name, func(t *testing.T) {
					// Use recover to catch panics — a panic is a test failure.
					func() {
						defer func() {
							if r := recover(); r != nil {
								t.Fatalf("panic on corrupted delta: %v", r)
							}
						}()
						// An error return is acceptable; a panic is not.
						_ = rc.apply(cc.delta)
					}()
				})
			}
		})
	}
}

// ---------- 2. MVRegister Long Resolution Chains ----------

func TestMVRegister_LongResolutionChain(t *testing.T) {
	A := NewMVRegisterReplica[string](1, StringCodec{})
	B := NewMVRegisterReplica[string](2, StringCodec{})
	C := NewMVRegisterReplica[string](3, StringCodec{})

	// Step 1: A writes "v1", B writes "v2" concurrently.
	// Capture the deltas from Write so we can apply the exact concurrent deltas.
	deltaA1, err := A.Data.Write("v1", A.NextDot(), A.HWM())
	if err != nil {
		t.Fatalf("A.Write v1: %v", err)
	}

	deltaB1, err := B.Data.Write("v2", B.NextDot(), B.HWM())
	if err != nil {
		t.Fatalf("B.Write v2: %v", err)
	}

	// Exchange concurrent deltas directly.
	if err := A.ApplyDelta(deltaB1); err != nil {
		t.Fatalf("A.ApplyDelta(B1): %v", err)
	}
	if err := B.ApplyDelta(deltaA1); err != nil {
		t.Fatalf("B.ApplyDelta(A1): %v", err)
	}

	got := sortedValues(t, A)
	if fmt.Sprint(got) != "[v1 v2]" {
		t.Fatalf("A after A<->B merge: expected [v1 v2], got %v", got)
	}
	got = sortedValues(t, B)
	if fmt.Sprint(got) != "[v1 v2]" {
		t.Fatalf("B after A<->B merge: expected [v1 v2], got %v", got)
	}

	// Step 2: C writes "v3" concurrently (before syncing with A or B).
	deltaC1, err := C.Data.Write("v3", C.NextDot(), C.HWM())
	if err != nil {
		t.Fatalf("C.Write v3: %v", err)
	}

	// Apply C's delta to A and B, and A's + B's deltas to C.
	if err := A.ApplyDelta(deltaC1); err != nil {
		t.Fatalf("A.ApplyDelta(C1): %v", err)
	}
	if err := B.ApplyDelta(deltaC1); err != nil {
		t.Fatalf("B.ApplyDelta(C1): %v", err)
	}
	if err := C.ApplyDelta(deltaA1); err != nil {
		t.Fatalf("C.ApplyDelta(A1): %v", err)
	}
	if err := C.ApplyDelta(deltaB1); err != nil {
		t.Fatalf("C.ApplyDelta(B1): %v", err)
	}

	for _, pair := range []struct {
		name string
		r    *Replica[*MVRegister[string]]
	}{{"A", A}, {"B", B}, {"C", C}} {
		got := sortedValues(t, pair.r)
		if fmt.Sprint(got) != "[v1 v2 v3]" {
			t.Fatalf("%s after 3-way merge: expected [v1 v2 v3], got %v", pair.name, got)
		}
	}

	// Step 3: A writes "resolved" with full context of all 3 values.
	deltaResolve, err := A.Data.Write("resolved", A.NextDot(), A.HWM())
	if err != nil {
		t.Fatalf("A.Write resolved: %v", err)
	}

	if err := B.ApplyDelta(deltaResolve); err != nil {
		t.Fatalf("B.ApplyDelta(resolve): %v", err)
	}
	if err := C.ApplyDelta(deltaResolve); err != nil {
		t.Fatalf("C.ApplyDelta(resolve): %v", err)
	}

	for _, pair := range []struct {
		name string
		r    *Replica[*MVRegister[string]]
	}{{"A", A}, {"B", B}, {"C", C}} {
		got := sortedValues(t, pair.r)
		if fmt.Sprint(got) != "[resolved]" {
			t.Fatalf("%s after resolve: expected [resolved], got %v", pair.name, got)
		}
	}

	// Step 4: B writes "final" -> all merge -> should have just ["final"].
	deltaFinal, err := B.Data.Write("final", B.NextDot(), B.HWM())
	if err != nil {
		t.Fatalf("B.Write final: %v", err)
	}

	if err := A.ApplyDelta(deltaFinal); err != nil {
		t.Fatalf("A.ApplyDelta(final): %v", err)
	}
	if err := C.ApplyDelta(deltaFinal); err != nil {
		t.Fatalf("C.ApplyDelta(final): %v", err)
	}

	for _, pair := range []struct {
		name string
		r    *Replica[*MVRegister[string]]
	}{{"A", A}, {"B", B}, {"C", C}} {
		got := sortedValues(t, pair.r)
		if fmt.Sprint(got) != "[final]" {
			t.Fatalf("%s after final: expected [final], got %v", pair.name, got)
		}
	}
}

// ---------- 3. Diamond Sync Pattern for MVRegister ----------

func TestMVRegister_DiamondSync(t *testing.T) {
	A := NewMVRegisterReplica[string](1, StringCodec{})
	B := NewMVRegisterReplica[string](2, StringCodec{})
	C := NewMVRegisterReplica[string](3, StringCodec{})
	D := NewMVRegisterReplica[string](4, StringCodec{})

	// A writes initial value.
	deltaA, err := A.Data.Write("origin", A.NextDot(), A.HWM())
	if err != nil {
		t.Fatalf("A.Write origin: %v", err)
	}

	// A -> B, A -> C (both receive A's state).
	if err := B.ApplyDelta(deltaA); err != nil {
		t.Fatalf("B.ApplyDelta(A): %v", err)
	}
	if err := C.ApplyDelta(deltaA); err != nil {
		t.Fatalf("C.ApplyDelta(A): %v", err)
	}

	// B and C each write independently (concurrent). Both have seen A's value
	// so their context covers A's dot, meaning "origin" will be pruned.
	deltaB, err := B.Data.Write("from-B", B.NextDot(), B.HWM())
	if err != nil {
		t.Fatalf("B.Write from-B: %v", err)
	}

	deltaC, err := C.Data.Write("from-C", C.NextDot(), C.HWM())
	if err != nil {
		t.Fatalf("C.Write from-C: %v", err)
	}

	// B -> D, C -> D (diamond merge at D).
	if err := D.ApplyDelta(deltaB); err != nil {
		t.Fatalf("D.ApplyDelta(B): %v", err)
	}
	if err := D.ApplyDelta(deltaC); err != nil {
		t.Fatalf("D.ApplyDelta(C): %v", err)
	}

	// D should have both concurrent values.
	got := sortedValues(t, D)
	if fmt.Sprint(got) != "[from-B from-C]" {
		t.Fatalf("D after diamond merge: expected [from-B from-C], got %v", got)
	}

	// Verify D has exactly 2 entries.
	if D.Data.Len() != 2 {
		t.Fatalf("D.Len: expected 2, got %d", D.Data.Len())
	}
}
