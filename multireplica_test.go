package crdt

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// syncPair performs a bidirectional anti-entropy exchange between two replicas.
func syncPairLWW(a, b *Replica[*LWWMap[string]]) {
	for _, d := range a.DeltasSince(b.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.HWM()) {
		a.ApplyDelta(d)
	}
}

func syncPairORSet(a, b *Replica[*ORSet[string]]) {
	for _, d := range a.DeltasSince(b.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.HWM()) {
		a.ApplyDelta(d)
	}
}

func syncPairAWLWW(a, b *Replica[*AWLWWMap[string]]) {
	for _, d := range a.DeltasSince(b.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.HWM()) {
		a.ApplyDelta(d)
	}
}

// collectLWWEntries returns a sorted list of "key=value" strings from an LWWMap replica.
func collectLWWEntries(r *Replica[*LWWMap[string]]) []string {
	var entries []string
	r.Data.Range(func(key string, value string, dot Dot) bool {
		entries = append(entries, key+"="+value)
		return true
	})
	sort.Strings(entries)
	return entries
}

// collectAWLWWEntries returns a sorted list of "key=value" strings from an AWLWWMap replica.
func collectAWLWWEntries(r *Replica[*AWLWWMap[string]]) []string {
	var entries []string
	r.Data.Range(func(key string, value string, dot Dot) bool {
		entries = append(entries, key+"="+value)
		return true
	})
	sort.Strings(entries)
	return entries
}

// TestLWWMap_DiamondSync tests diamond-shaped synchronization:
// A puts "x"="a", sends to B and C independently.
// B puts "x"="b", C puts "x"="c".
// B and C exchange deltas. Both should converge to the same winner.
func TestLWWMap_DiamondSync(t *testing.T) {
	A := NewLWWMapReplica[string](1, StringCodec{})
	B := NewLWWMapReplica[string](2, StringCodec{})
	C := NewLWWMapReplica[string](3, StringCodec{})

	// A puts "x"="a".
	deltaA, err := A.Data.Put("x", "a", A.NextDot())
	if err != nil {
		t.Fatal(err)
	}

	// Send A's delta to B and C independently.
	if err := B.ApplyDelta(deltaA); err != nil {
		t.Fatal(err)
	}
	if err := C.ApplyDelta(deltaA); err != nil {
		t.Fatal(err)
	}

	// B puts "x"="b" (overwrites A's value with a higher dot).
	deltaB, err := B.Data.Put("x", "b", B.NextDot())
	if err != nil {
		t.Fatal(err)
	}

	// C puts "x"="c" (overwrites A's value with a higher dot).
	deltaC, err := C.Data.Put("x", "c", C.NextDot())
	if err != nil {
		t.Fatal(err)
	}

	// B and C exchange deltas (diamond merge).
	if err := B.ApplyDelta(deltaC); err != nil {
		t.Fatal(err)
	}
	if err := C.ApplyDelta(deltaB); err != nil {
		t.Fatal(err)
	}

	// Both B and C should have the same value for "x".
	vB, _, okB := B.Data.Get("x")
	vC, _, okC := C.Data.Get("x")
	if !okB || !okC {
		t.Fatalf("expected both to have key x: B=%v C=%v", okB, okC)
	}
	if vB != vC {
		t.Fatalf("B and C did not converge: B=%q C=%q", vB, vC)
	}
	t.Logf("B and C converged on x=%q", vB)
}

// TestORSet_ThreeWayAntiEntropy tests 3 replicas each adding different elements,
// then doing a full round of anti-entropy. All 3 should end up with the same elements.
func TestORSet_ThreeWayAntiEntropy(t *testing.T) {
	A := NewORSetReplica[string](1, StringCodec{})
	B := NewORSetReplica[string](2, StringCodec{})
	C := NewORSetReplica[string](3, StringCodec{})

	// Each replica adds a unique element.
	if _, err := A.Data.Add("alpha", A.NextDot()); err != nil {
		t.Fatal(err)
	}
	if _, err := B.Data.Add("beta", B.NextDot()); err != nil {
		t.Fatal(err)
	}
	if _, err := C.Data.Add("gamma", C.NextDot()); err != nil {
		t.Fatal(err)
	}

	// Full round of anti-entropy: every pair exchanges.
	syncPairORSet(A, B)
	syncPairORSet(B, C)
	syncPairORSet(A, C)

	// Verify all 3 have the same elements.
	for _, r := range []*Replica[*ORSet[string]]{A, B, C} {
		elems, err := r.Data.Elements()
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(elems)
		if len(elems) != 3 {
			t.Fatalf("expected 3 elements, got %d: %v", len(elems), elems)
		}
		if elems[0] != "alpha" || elems[1] != "beta" || elems[2] != "gamma" {
			t.Fatalf("unexpected elements: %v", elems)
		}
	}
}

// TestLWWMap_PartitionHeal tests 5 replicas in two partitions {A,B} and {C,D,E}.
// Each partition does internal operations, then partitions heal via full anti-entropy.
// All 5 should converge.
func TestLWWMap_PartitionHeal(t *testing.T) {
	replicas := make([]*Replica[*LWWMap[string]], 5)
	for i := range replicas {
		replicas[i] = NewLWWMapReplica[string](uint64(i+1), StringCodec{})
	}
	A, B, C, D, E := replicas[0], replicas[1], replicas[2], replicas[3], replicas[4]

	// Partition 1: {A, B} do some operations.
	deltaA1, _ := A.Data.Put("p1-key", "from-a", A.NextDot())
	B.ApplyDelta(deltaA1)
	deltaB1, _ := B.Data.Put("shared", "b-val", B.NextDot())
	A.ApplyDelta(deltaB1)

	// Partition 2: {C, D, E} do some operations.
	deltaC1, _ := C.Data.Put("p2-key", "from-c", C.NextDot())
	D.ApplyDelta(deltaC1)
	E.ApplyDelta(deltaC1)
	deltaD1, _ := D.Data.Put("shared", "d-val", D.NextDot())
	C.ApplyDelta(deltaD1)
	E.ApplyDelta(deltaD1)
	deltaE1, _ := E.Data.Put("e-only", "from-e", E.NextDot())
	C.ApplyDelta(deltaE1)
	D.ApplyDelta(deltaE1)

	// Heal: full anti-entropy among all pairs.
	for i := 0; i < len(replicas); i++ {
		for j := i + 1; j < len(replicas); j++ {
			syncPairLWW(replicas[i], replicas[j])
		}
	}
	// Second pass to propagate transitive state.
	for i := 0; i < len(replicas); i++ {
		for j := i + 1; j < len(replicas); j++ {
			syncPairLWW(replicas[i], replicas[j])
		}
	}

	// Verify all 5 converge to the same state.
	reference := collectLWWEntries(replicas[0])
	for i := 1; i < len(replicas); i++ {
		got := collectLWWEntries(replicas[i])
		if len(got) != len(reference) {
			t.Fatalf("replica %d has %d entries, expected %d: %v vs %v",
				i+1, len(got), len(reference), got, reference)
		}
		for k := range reference {
			if got[k] != reference[k] {
				t.Fatalf("replica %d diverged at entry %d: %q vs %q",
					i+1, k, got[k], reference[k])
			}
		}
	}
	t.Logf("all 5 replicas converged: %v", reference)
}

// TestORSet_ConcurrentAddRemove_ThreeNodes tests add-wins semantics:
// A adds "x", sends to B. B removes "x". Meanwhile C adds "x" independently.
// After full exchange, "x" should be in the set (C's add is concurrent with B's remove).
func TestORSet_ConcurrentAddRemove_ThreeNodes(t *testing.T) {
	A := NewORSetReplica[string](1, StringCodec{})
	B := NewORSetReplica[string](2, StringCodec{})
	C := NewORSetReplica[string](3, StringCodec{})

	// A adds "x".
	deltaAddA, err := A.Data.Add("x", A.NextDot())
	if err != nil {
		t.Fatal(err)
	}

	// Send A's add to B.
	if err := B.ApplyDelta(deltaAddA); err != nil {
		t.Fatal(err)
	}

	// B removes "x" (having seen A's add dot).
	deltaRemB, err := B.Data.Remove("x", B.HWM())
	if err != nil {
		t.Fatal(err)
	}

	// Meanwhile, C adds "x" independently (has not seen anything).
	deltaAddC, err := C.Data.Add("x", C.NextDot())
	if err != nil {
		t.Fatal(err)
	}

	// Full exchange: apply all deltas everywhere.
	// A receives B's remove and C's add.
	A.ApplyDelta(deltaRemB)
	A.ApplyDelta(deltaAddC)

	// B receives C's add.
	B.ApplyDelta(deltaAddC)

	// C receives A's add and B's remove.
	C.ApplyDelta(deltaAddA)
	C.ApplyDelta(deltaRemB)

	// Do a full anti-entropy round to ensure complete convergence.
	syncPairORSet(A, B)
	syncPairORSet(B, C)
	syncPairORSet(A, C)

	// "x" should be in the set on all replicas: C's add was concurrent with
	// B's remove, and ORSet has add-wins semantics.
	for name, r := range map[string]*Replica[*ORSet[string]]{"A": A, "B": B, "C": C} {
		if !r.Data.Contains("x") {
			t.Fatalf("replica %s: expected x to be in set (add-wins)", name)
		}
	}
}

// TestAWLWWMap_FiveNodeConvergence tests 5 replicas doing random puts and removes,
// then exchanging all deltas and verifying convergence.
func TestAWLWWMap_FiveNodeConvergence(t *testing.T) {
	const numReplicas = 5
	const opsPerReplica = 20

	rng := rand.New(rand.NewSource(42))

	replicas := make([]*Replica[*AWLWWMap[string]], numReplicas)
	// Collect all deltas per replica so we can broadcast them.
	allDeltas := make([][][]byte, numReplicas)

	for i := range replicas {
		replicas[i] = NewAWLWWMapReplica[string](uint64(i+1), StringCodec{})
	}

	keys := []string{"a", "b", "c", "d", "e"}

	// Each replica performs random operations.
	for i, r := range replicas {
		for op := 0; op < opsPerReplica; op++ {
			key := keys[rng.Intn(len(keys))]
			if rng.Float64() < 0.3 {
				// Remove with 30% probability.
				delta := r.Data.Remove(key, r.NextDot(), r.HWM())
				allDeltas[i] = append(allDeltas[i], delta)
			} else {
				// Put with 70% probability.
				val := fmt.Sprintf("r%d-op%d", i+1, op)
				delta, err := r.Data.Put(key, val, r.NextDot())
				if err != nil {
					t.Fatal(err)
				}
				allDeltas[i] = append(allDeltas[i], delta)
			}
		}
	}

	// Broadcast all deltas from each replica to all others.
	for src := 0; src < numReplicas; src++ {
		for dst := 0; dst < numReplicas; dst++ {
			if src == dst {
				continue
			}
			for _, delta := range allDeltas[src] {
				if err := replicas[dst].ApplyDelta(delta); err != nil {
					t.Fatalf("replica %d -> %d: %v", src+1, dst+1, err)
				}
			}
		}
	}

	// Full anti-entropy to ensure complete convergence.
	for i := 0; i < numReplicas; i++ {
		for j := i + 1; j < numReplicas; j++ {
			syncPairAWLWW(replicas[i], replicas[j])
		}
	}

	// Verify all replicas have identical state.
	reference := collectAWLWWEntries(replicas[0])
	for i := 1; i < numReplicas; i++ {
		got := collectAWLWWEntries(replicas[i])
		if len(got) != len(reference) {
			t.Fatalf("replica %d has %d entries, expected %d\n  ref: %v\n  got: %v",
				i+1, len(got), len(reference), reference, got)
		}
		for k := range reference {
			if got[k] != reference[k] {
				t.Fatalf("replica %d diverged at entry %d: %q vs %q",
					i+1, k, got[k], reference[k])
			}
		}
	}
	t.Logf("all %d replicas converged with %d entries: %v", numReplicas, len(reference), reference)
}
