package crdt

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"pgregory.net/rapid"
)

// Convergence tests verify the fundamental CRDT properties:
// - Commutativity: merge(A, B) == merge(B, A)
// - Associativity: merge(merge(A, B), C) == merge(A, merge(B, C))
// - Idempotency: merge(A, A) == A
// - Convergence: N replicas performing random ops converge after full state exchange
//
// These use full-state merges (not deltas) because state-based CRDTs guarantee
// convergence when full states are exchanged. Delta application requires causal
// ordering which is a transport-layer concern.

// --- GCounter ---

func TestGCounter_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 5).Draw(t, "numReplicas")
		ops := rapid.IntRange(1, 30).Draw(t, "numOps")

		replicas := make([]*GCounter, n)
		for i := range replicas {
			replicas[i] = NewGCounter(ReplicaID(i + 1))
		}

		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			amt := rapid.Uint64Range(1, 100).Draw(t, "amt")
			replicas[r], _ = replicas[r].Increment(amt)
		}

		convergeAll(replicas, func(a, b *GCounter) *GCounter {
			return a.Merge(b).(*GCounter)
		})

		expected := replicas[0].Int64()
		for i, r := range replicas[1:] {
			if r.Int64() != expected {
				t.Fatalf("replica %d diverged: %d vs %d", i+1, r.Int64(), expected)
			}
		}
	})
}

func TestGCounter_Commutativity(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a := NewGCounter(1)
		a, _ = a.Increment(rapid.Uint64Range(1, 100).Draw(t, "a"))
		b := NewGCounter(2)
		b, _ = b.Increment(rapid.Uint64Range(1, 100).Draw(t, "b"))
		ab := a.Merge(b).(*GCounter)
		ba := b.Merge(a).(*GCounter)
		if ab.Int64() != ba.Int64() {
			t.Fatalf("not commutative: %d vs %d", ab.Int64(), ba.Int64())
		}
	})
}

func TestGCounter_Idempotency(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a := NewGCounter(1)
		a, _ = a.Increment(rapid.Uint64Range(1, 100).Draw(t, "a"))
		merged := a.Merge(a).(*GCounter)
		if merged.Int64() != a.Int64() {
			t.Fatalf("not idempotent: %d vs %d", merged.Int64(), a.Int64())
		}
	})
}

// --- PNCounter ---

func TestPNCounter_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 4).Draw(t, "n")
		ops := rapid.IntRange(1, 30).Draw(t, "ops")

		replicas := make([]*PNCounter, n)
		for i := range replicas {
			replicas[i] = NewPNCounter(ReplicaID(i + 1))
		}

		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			amt := rapid.Uint64Range(1, 50).Draw(t, "amt")
			if rapid.Bool().Draw(t, "inc") {
				replicas[r], _ = replicas[r].Increment(amt)
			} else {
				replicas[r], _ = replicas[r].Decrement(amt)
			}
		}

		convergeAll(replicas, func(a, b *PNCounter) *PNCounter {
			return a.Merge(b).(*PNCounter)
		})

		expected := replicas[0].Int64()
		for _, r := range replicas[1:] {
			if r.Int64() != expected {
				t.Fatalf("PNCounter diverged: %d vs %d", r.Int64(), expected)
			}
		}
	})
}

// --- LWWRegister ---

func TestLWWRegister_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 4).Draw(t, "n")
		ops := rapid.IntRange(1, 20).Draw(t, "ops")

		replicas := make([]*LWWRegister, n)
		for i := range replicas {
			replicas[i] = NewLWWRegister(ReplicaID(i + 1))
		}

		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			val := rapid.String().Draw(t, "val")
			replicas[r], _ = replicas[r].Set(val)
		}

		convergeAll(replicas, func(a, b *LWWRegister) *LWWRegister {
			return a.Merge(b).(*LWWRegister)
		})

		expected := replicas[0].Value()
		for _, r := range replicas[1:] {
			if r.Value() != expected {
				t.Fatalf("LWWRegister diverged: %v vs %v", r.Value(), expected)
			}
		}
	})
}

// --- MVRegister ---

func TestMVRegister_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 4).Draw(t, "n")
		ops := rapid.IntRange(1, 20).Draw(t, "ops")

		replicas := make([]*MVRegister, n)
		for i := range replicas {
			replicas[i] = NewMVRegister(ReplicaID(i + 1))
		}

		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			val := rapid.String().Draw(t, "val")
			replicas[r], _ = replicas[r].Write(val)
		}

		convergeAll(replicas, func(a, b *MVRegister) *MVRegister {
			return a.Merge(b).(*MVRegister)
		})

		expected := sortedAnySlice(replicas[0].Values())
		for _, r := range replicas[1:] {
			got := sortedAnySlice(r.Values())
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("MVRegister diverged: %v vs %v", got, expected)
			}
		}
	})
}

// --- ORSet ---

func TestORSet_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 4).Draw(t, "n")
		ops := rapid.IntRange(1, 30).Draw(t, "ops")

		replicas := make([]*ORSet, n)
		for i := range replicas {
			replicas[i] = NewORSet(ReplicaID(i + 1))
		}

		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			val := rapid.StringMatching(`[a-e]`).Draw(t, "val")
			if rapid.Bool().Draw(t, "add") {
				replicas[r], _ = replicas[r].Add(val)
			} else {
				replicas[r], _ = replicas[r].Remove(val)
			}
		}

		convergeAll(replicas, func(a, b *ORSet) *ORSet {
			return a.Merge(b).(*ORSet)
		})

		expected := sortedAnySlice(replicas[0].Elements())
		for _, r := range replicas[1:] {
			got := sortedAnySlice(r.Elements())
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("ORSet diverged: %v vs %v", got, expected)
			}
		}
	})
}

// --- GList ---

func TestGList_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 4).Draw(t, "n")
		ops := rapid.IntRange(1, 30).Draw(t, "ops")

		replicas := make([]*GList, n)
		for i := range replicas {
			replicas[i] = NewGList(ReplicaID(i + 1))
		}

		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			val := rapid.String().Draw(t, "val")
			replicas[r], _ = replicas[r].Append(val)
		}

		convergeAll(replicas, func(a, b *GList) *GList {
			return a.Merge(b).(*GList)
		})

		expected := replicas[0].Items()
		for _, r := range replicas[1:] {
			got := r.Items()
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("GList diverged: %v vs %v", got, expected)
			}
		}
	})
}

// --- ORMap ---

func TestORMap_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 4).Draw(t, "n")
		ops := rapid.IntRange(1, 20).Draw(t, "ops")

		replicas := make([]*ORMap, n)
		for i := range replicas {
			replicas[i] = NewORMap(ReplicaID(i + 1))
		}

		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			key := rapid.StringMatching(`[a-c]`).Draw(t, "key")
			if rapid.Bool().Draw(t, "put") {
				val := rapid.String().Draw(t, "val")
				replicas[r], _ = replicas[r].Put(key, val)
			} else {
				replicas[r], _ = replicas[r].Remove(key)
			}
		}

		convergeAll(replicas, func(a, b *ORMap) *ORMap {
			return a.Merge(b).(*ORMap)
		})

		expected := replicas[0].Map()
		for _, r := range replicas[1:] {
			got := r.Map()
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("ORMap diverged: %v vs %v", got, expected)
			}
		}
	})
}

// --- LWWMap ---

func TestLWWMap_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 4).Draw(t, "n")
		ops := rapid.IntRange(1, 20).Draw(t, "ops")

		replicas := make([]*LWWMap, n)
		for i := range replicas {
			replicas[i] = NewLWWMap(ReplicaID(i + 1))
		}

		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			key := rapid.StringMatching(`[a-c]`).Draw(t, "key")
			if rapid.Bool().Draw(t, "put") {
				val := rapid.String().Draw(t, "val")
				replicas[r], _ = replicas[r].Put(key, val)
			} else {
				replicas[r], _ = replicas[r].Remove(key)
			}
		}

		convergeAll(replicas, func(a, b *LWWMap) *LWWMap {
			return a.Merge(b).(*LWWMap)
		})

		expected := replicas[0].Map()
		for _, r := range replicas[1:] {
			got := r.Map()
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("LWWMap diverged: %v vs %v", got, expected)
			}
		}
	})
}

// --- AWLWWMap ---

func TestAWLWWMap_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 4).Draw(t, "n")
		ops := rapid.IntRange(1, 20).Draw(t, "ops")

		replicas := make([]*AWLWWMap, n)
		for i := range replicas {
			replicas[i] = NewAWLWWMap(ReplicaID(i + 1))
		}

		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			key := rapid.StringMatching(`[a-c]`).Draw(t, "key")
			if rapid.Bool().Draw(t, "put") {
				val := rapid.String().Draw(t, "val")
				replicas[r], _ = replicas[r].Put(key, val)
			} else {
				replicas[r], _ = replicas[r].Remove(key)
			}
		}

		convergeAll(replicas, func(a, b *AWLWWMap) *AWLWWMap {
			return a.Merge(b).(*AWLWWMap)
		})

		expected := replicas[0].Map()
		for _, r := range replicas[1:] {
			got := r.Map()
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("AWLWWMap diverged: %v vs %v", got, expected)
			}
		}
	})
}

// --- Helpers ---

// convergeAll merges every replica's state with every other replica's state
// until all replicas are converged. This simulates full state exchange.
func convergeAll[T any](replicas []T, merge func(a, b T) T) {
	// Two rounds of all-pairs merge guarantees convergence.
	for round := 0; round < 2; round++ {
		for i := range replicas {
			for j := range replicas {
				if i != j {
					replicas[i] = merge(replicas[i], replicas[j])
				}
			}
		}
	}
}

func sortedAnySlice(s []any) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[i] = fmt.Sprint(v)
	}
	sort.Strings(out)
	return out
}
