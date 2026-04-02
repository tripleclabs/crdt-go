package crdt

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestLWWMap_10KEntries(t *testing.T) {
	r1 := NewLWWMapReplica[string](1, StringCodec{})

	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		delta, err := r1.Data.Put(key, val, r1.NextDot())
		if err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		_ = delta
	}

	if r1.Data.Len() != 10_000 {
		t.Fatalf("r1: expected 10000 entries, got %d", r1.Data.Len())
	}

	// Sync to second replica via DeltasSince.
	r2 := NewLWWMapReplica[string](2, StringCodec{})
	deltas := r1.DeltasSince(r2.HWM())
	for _, d := range deltas {
		if err := r2.ApplyDelta(d); err != nil {
			t.Fatalf("apply delta: %v", err)
		}
	}

	if r2.Data.Len() != 10_000 {
		t.Fatalf("r2: expected 10000 entries, got %d", r2.Data.Len())
	}

	// Verify correct values.
	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("val-%d", i)
		v, _, ok := r2.Data.Get(key)
		if !ok {
			t.Fatalf("r2 missing key %s", key)
		}
		if v != expected {
			t.Fatalf("r2 key %s: expected %s, got %s", key, expected, v)
		}
	}
}

func TestORSet_1KElements_WithRemoves(t *testing.T) {
	r1 := NewORSetReplica[string](1, StringCodec{})

	// Add 1000 elements.
	for i := 0; i < 1000; i++ {
		elem := fmt.Sprintf("elem-%d", i)
		if _, err := r1.Data.Add(elem, r1.NextDot()); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}

	if r1.Data.Len() != 1000 {
		t.Fatalf("after adds: expected 1000, got %d", r1.Data.Len())
	}

	// Remove the first 500 elements.
	for i := 0; i < 500; i++ {
		elem := fmt.Sprintf("elem-%d", i)
		if _, err := r1.Data.Remove(elem, r1.HWM()); err != nil {
			t.Fatalf("remove %d: %v", i, err)
		}
	}

	if r1.Data.Len() != 500 {
		t.Fatalf("after removes: expected 500, got %d", r1.Data.Len())
	}

	// Sync to second replica.
	r2 := NewORSetReplica[string](2, StringCodec{})
	deltas := r1.DeltasSince(r2.HWM())
	for _, d := range deltas {
		if err := r2.ApplyDelta(d); err != nil {
			t.Fatalf("apply delta: %v", err)
		}
	}

	if r2.Data.Len() != 500 {
		t.Fatalf("r2: expected 500, got %d", r2.Data.Len())
	}

	// Verify the surviving elements are elem-500 through elem-999.
	for i := 500; i < 1000; i++ {
		elem := fmt.Sprintf("elem-%d", i)
		if !r2.Data.Contains(elem) {
			t.Fatalf("r2 missing %s", elem)
		}
	}
	for i := 0; i < 500; i++ {
		elem := fmt.Sprintf("elem-%d", i)
		if r2.Data.Contains(elem) {
			t.Fatalf("r2 should not contain removed %s", elem)
		}
	}
}

func TestLWWMap_10Replicas_1KOps(t *testing.T) {
	const numReplicas = 10
	const opsPerReplica = 100

	rng := rand.New(rand.NewSource(42))
	replicas := make([]*Replica[*LWWMap[string]], numReplicas)
	for i := range replicas {
		replicas[i] = NewLWWMapReplica[string](ReplicaID(i+1), StringCodec{})
	}

	// Each replica does 100 random put/remove ops.
	for _, r := range replicas {
		for op := 0; op < opsPerReplica; op++ {
			key := fmt.Sprintf("key-%d", rng.Intn(200))
			if rng.Intn(4) == 0 {
				// 25% chance of remove.
				r.Data.Remove(key, r.NextDot())
			} else {
				val := fmt.Sprintf("val-%d", rng.Intn(10000))
				if _, err := r.Data.Put(key, val, r.NextDot()); err != nil {
					t.Fatalf("put: %v", err)
				}
			}
		}
	}

	// Full pairwise sync until convergence.
	// Two rounds ensure transitive propagation.
	for round := 0; round < 2; round++ {
		for i, src := range replicas {
			for j, dst := range replicas {
				if i == j {
					continue
				}
				for _, d := range src.DeltasSince(dst.HWM()) {
					if err := dst.ApplyDelta(d); err != nil {
						t.Fatalf("apply delta from %d to %d: %v", i+1, j+1, err)
					}
				}
			}
		}
	}

	// Verify all replicas converge to the same state.
	refLen := replicas[0].Data.Len()
	for i, r := range replicas[1:] {
		if r.Data.Len() != refLen {
			t.Fatalf("replica %d has %d entries, expected %d", i+2, r.Data.Len(), refLen)
		}
	}

	// Verify all key-value pairs match.
	replicas[0].Data.Range(func(key string, val string, dot Dot) bool {
		for i, r := range replicas[1:] {
			v, d, ok := r.Data.Get(key)
			if !ok {
				t.Fatalf("replica %d missing key %s", i+2, key)
			}
			if v != val {
				t.Fatalf("replica %d key %s: expected %s, got %s", i+2, key, val, v)
			}
			if d != dot {
				t.Fatalf("replica %d key %s: dot mismatch", i+2, key)
			}
		}
		return true
	})
}

func TestGCounter_100Replicas(t *testing.T) {
	const numReplicas = 100

	replicas := make([]*Replica[*GCounter], numReplicas)
	var allDeltas [][]byte

	for i := range replicas {
		rid := ReplicaID(i + 1)
		replicas[i] = NewGCounterReplica(rid)
		d := replicas[i].Data.Increment(rid, uint64(i+1))
		replicas[i].Received.Record(rid, uint64(i+1))
		allDeltas = append(allDeltas, d)
	}

	// Full sync: apply all deltas to all replicas.
	for _, r := range replicas {
		for _, d := range allDeltas {
			if err := r.ApplyDelta(d); err != nil {
				t.Fatalf("apply delta: %v", err)
			}
		}
	}

	// Expected total: sum of 1..100 = 5050.
	expectedTotal := int64(numReplicas * (numReplicas + 1) / 2)
	for i, r := range replicas {
		if r.Data.Int64() != expectedTotal {
			t.Fatalf("replica %d: expected %d, got %d", i+1, expectedTotal, r.Data.Int64())
		}
	}
}
