package crdt

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

const (
	convergenceIterations = 100
	minOps                = 5
	maxOps                = 20
	minReplicas           = 2
	maxReplicas           = 5
)

// antiEntropySync uses DeltasSince to sync all replicas pairwise until no
// new deltas are produced.
func antiEntropySync[M Mergeable](replicas []*Replica[M]) {
	for round := 0; round < 10; round++ {
		changed := false
		for i, src := range replicas {
			for j, dst := range replicas {
				if i == j {
					continue
				}
				deltas := src.DeltasSince(dst.HWM())
				if len(deltas) > 0 {
					changed = true
				}
				for _, d := range deltas {
					if err := dst.ApplyDelta(d); err != nil {
						panic(fmt.Sprintf("ApplyDelta error: %v", err))
					}
				}
			}
		}
		if !changed {
			break
		}
	}
}

// random key/value generators

func randKey(rng *rand.Rand) string {
	keys := []string{"a", "b", "c", "d", "e"}
	return keys[rng.Intn(len(keys))]
}

func randVal(rng *rand.Rand) string {
	vals := []string{"x", "y", "z", "w", "v"}
	return vals[rng.Intn(len(vals))]
}

func randElem(rng *rand.Rand) string {
	elems := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	return elems[rng.Intn(len(elems))]
}

// snapshot helpers

func snapshotLWWMap(r *Replica[*LWWMap[string]]) map[string]string {
	m := make(map[string]string)
	r.Data.Range(func(key string, value string, _ Dot) bool {
		m[key] = value
		return true
	})
	return m
}

func snapshotORSet(r *Replica[*ORSet[string]]) []string {
	elems, err := r.Data.Elements()
	if err != nil {
		panic(fmt.Sprintf("Elements error: %v", err))
	}
	sort.Strings(elems)
	return elems
}

func snapshotORMap(r *Replica[*ORMap[string]]) map[string]string {
	m := make(map[string]string)
	r.Data.Range(func(key string, value string, _ DotMap) bool {
		m[key] = value
		return true
	})
	return m
}

func snapshotAWLWWMap(r *Replica[*AWLWWMap[string]]) map[string]string {
	m := make(map[string]string)
	r.Data.Range(func(key string, value string, _ Dot) bool {
		m[key] = value
		return true
	})
	return m
}

func snapshotGList(r *Replica[*GList[string]]) []string {
	items, err := r.Data.Items()
	if err != nil {
		panic(fmt.Sprintf("Items error: %v", err))
	}
	return items
}

func snapshotMVRegister(r *Replica[*MVRegister[string]]) []string {
	vals, err := r.Data.Values()
	if err != nil {
		panic(fmt.Sprintf("Values error: %v", err))
	}
	sort.Strings(vals)
	return vals
}

// --- Convergence Tests ---

// TestConvergenceLWWMap verifies that LWWMap replicas converge after random
// Put and Remove operations followed by anti-entropy synchronization.
// LWWMap DeltasSince includes both entries and tombstones, so anti-entropy
// alone is sufficient.
func TestConvergenceLWWMap(t *testing.T) {
	for iter := 0; iter < convergenceIterations; iter++ {
		seed := int64(iter)
		rng := rand.New(rand.NewSource(seed))
		n := minReplicas + rng.Intn(maxReplicas-minReplicas+1)
		numOps := minOps + rng.Intn(maxOps-minOps+1)

		replicas := make([]*Replica[*LWWMap[string]], n)
		for i := range replicas {
			replicas[i] = NewLWWMapReplica[string](ReplicaID(i+1), StringCodec{})
		}

		for op := 0; op < numOps; op++ {
			r := replicas[rng.Intn(n)]
			if rng.Intn(3) == 0 {
				key := randKey(rng)
				r.Data.Remove(key, r.NextDot())
			} else {
				key := randKey(rng)
				val := randVal(rng)
				if _, err := r.Data.Put(key, val, r.NextDot()); err != nil {
					t.Fatalf("seed=%d op=%d Put error: %v", seed, op, err)
				}
			}
		}

		antiEntropySync(replicas)

		expected := snapshotLWWMap(replicas[0])
		for i := 1; i < n; i++ {
			got := snapshotLWWMap(replicas[i])
			if !reflect.DeepEqual(expected, got) {
				t.Fatalf("seed=%d LWWMap: replica 0 = %v, replica %d = %v", seed, expected, i, got)
			}
		}
	}
}

// TestConvergenceORSet verifies that ORSet replicas converge. Each operation
// immediately broadcasts its delta to all other replicas so that remove
// contexts are applied in causal order. A final anti-entropy round ensures
// completeness.
func TestConvergenceORSet(t *testing.T) {
	for iter := 0; iter < convergenceIterations; iter++ {
		seed := int64(iter)
		rng := rand.New(rand.NewSource(seed))
		n := minReplicas + rng.Intn(maxReplicas-minReplicas+1)
		numOps := minOps + rng.Intn(maxOps-minOps+1)

		replicas := make([]*Replica[*ORSet[string]], n)
		for i := range replicas {
			replicas[i] = NewORSetReplica[string](ReplicaID(i+1), StringCodec{})
		}

		for op := 0; op < numOps; op++ {
			idx := rng.Intn(n)
			r := replicas[idx]
			var delta []byte
			if rng.Intn(3) == 0 {
				elem := randElem(rng)
				if r.Data.Contains(elem) {
					d, err := r.Data.Remove(elem, r.HWM())
					if err != nil {
						t.Fatalf("seed=%d op=%d Remove error: %v", seed, op, err)
					}
					delta = d
				}
			} else {
				elem := randElem(rng)
				d, err := r.Data.Add(elem, r.NextDot())
				if err != nil {
					t.Fatalf("seed=%d op=%d Add error: %v", seed, op, err)
				}
				delta = d
			}
			if delta != nil {
				for j, dst := range replicas {
					if j == idx {
						continue
					}
					if err := dst.ApplyDelta(delta); err != nil {
						t.Fatalf("seed=%d op=%d ApplyDelta error: %v", seed, op, err)
					}
				}
			}
		}

		antiEntropySync(replicas)

		expected := snapshotORSet(replicas[0])
		for i := 1; i < n; i++ {
			got := snapshotORSet(replicas[i])
			if !reflect.DeepEqual(expected, got) {
				t.Fatalf("seed=%d ORSet: replica 0 = %v, replica %d = %v", seed, expected, i, got)
			}
		}
	}
}

// TestConvergenceORMap verifies that ORMap replicas converge after random
// Put operations followed by anti-entropy synchronization. ORMap's
// DeltasSince returns full dotmaps for each live entry, which ensures
// correct value resolution via max-dot comparison during anti-entropy.
// Removes are tested by broadcasting from a synced state, guaranteeing
// the remove context covers all relevant dots.
func TestConvergenceORMap(t *testing.T) {
	for iter := 0; iter < convergenceIterations; iter++ {
		seed := int64(iter)
		rng := rand.New(rand.NewSource(seed))
		n := minReplicas + rng.Intn(maxReplicas-minReplicas+1)
		numOps := minOps + rng.Intn(maxOps-minOps+1)

		replicas := make([]*Replica[*ORMap[string]], n)
		for i := range replicas {
			replicas[i] = NewORMapReplica[string](ReplicaID(i+1), StringCodec{})
		}

		// Phase 1: random puts on isolated replicas, then anti-entropy.
		for op := 0; op < numOps; op++ {
			r := replicas[rng.Intn(n)]
			key := randKey(rng)
			val := randVal(rng)
			if _, err := r.Data.Put(key, val, r.NextDot()); err != nil {
				t.Fatalf("seed=%d op=%d Put error: %v", seed, op, err)
			}
		}

		antiEntropySync(replicas)

		expected := snapshotORMap(replicas[0])
		for i := 1; i < n; i++ {
			got := snapshotORMap(replicas[i])
			if !reflect.DeepEqual(expected, got) {
				t.Fatalf("seed=%d ORMap puts: replica 0 = %v, replica %d = %v",
					seed, expected, i, got)
			}
		}

		// Phase 2: from synced state, do a few removes via immediate
		// broadcast. Since all replicas share identical state and HWM,
		// context-based removes converge.
		numRemoveOps := rng.Intn(3) + 1
		for op := 0; op < numRemoveOps; op++ {
			idx := rng.Intn(n)
			r := replicas[idx]
			key := randKey(rng)
			delta := r.Data.Remove(key, r.HWM())
			for j, dst := range replicas {
				if j == idx {
					continue
				}
				if err := dst.ApplyDelta(delta); err != nil {
					t.Fatalf("seed=%d rm op=%d ApplyDelta error: %v", seed, op, err)
				}
			}
			// Sync after each remove to keep replicas aligned.
			antiEntropySync(replicas)
		}

		expected2 := snapshotORMap(replicas[0])
		for i := 1; i < n; i++ {
			got := snapshotORMap(replicas[i])
			if !reflect.DeepEqual(expected2, got) {
				t.Fatalf("seed=%d ORMap removes: replica 0 = %v, replica %d = %v",
					seed, expected2, i, got)
			}
		}
	}
}

// TestConvergenceAWLWWMap verifies that AWLWWMap replicas converge.
// AWLWWMap DeltasSince includes both entries and tombstones, so
// anti-entropy alone is sufficient.
func TestConvergenceAWLWWMap(t *testing.T) {
	for iter := 0; iter < convergenceIterations; iter++ {
		seed := int64(iter)
		rng := rand.New(rand.NewSource(seed))
		n := minReplicas + rng.Intn(maxReplicas-minReplicas+1)
		numOps := minOps + rng.Intn(maxOps-minOps+1)

		replicas := make([]*Replica[*AWLWWMap[string]], n)
		for i := range replicas {
			replicas[i] = NewAWLWWMapReplica[string](ReplicaID(i+1), StringCodec{})
		}

		for op := 0; op < numOps; op++ {
			r := replicas[rng.Intn(n)]
			if rng.Intn(3) == 0 {
				key := randKey(rng)
				r.Data.Remove(key, r.NextDot(), r.HWM())
			} else {
				key := randKey(rng)
				val := randVal(rng)
				if _, err := r.Data.Put(key, val, r.NextDot()); err != nil {
					t.Fatalf("seed=%d op=%d Put error: %v", seed, op, err)
				}
			}
		}

		antiEntropySync(replicas)

		expected := snapshotAWLWWMap(replicas[0])
		for i := 1; i < n; i++ {
			got := snapshotAWLWWMap(replicas[i])
			if !reflect.DeepEqual(expected, got) {
				t.Fatalf("seed=%d AWLWWMap: replica 0 = %v, replica %d = %v", seed, expected, i, got)
			}
		}
	}
}

// TestConvergenceGList verifies that GList replicas converge after random
// Append operations followed by anti-entropy synchronization.
func TestConvergenceGList(t *testing.T) {
	for iter := 0; iter < convergenceIterations; iter++ {
		seed := int64(iter)
		rng := rand.New(rand.NewSource(seed))
		n := minReplicas + rng.Intn(maxReplicas-minReplicas+1)
		numOps := minOps + rng.Intn(maxOps-minOps+1)

		replicas := make([]*Replica[*GList[string]], n)
		for i := range replicas {
			replicas[i] = NewGListReplica[string](ReplicaID(i+1), StringCodec{})
		}

		for op := 0; op < numOps; op++ {
			r := replicas[rng.Intn(n)]
			val := randVal(rng)
			if _, err := r.Data.Append(val, r.NextDot()); err != nil {
				t.Fatalf("seed=%d op=%d Append error: %v", seed, op, err)
			}
		}

		antiEntropySync(replicas)

		expected := snapshotGList(replicas[0])
		for i := 1; i < n; i++ {
			got := snapshotGList(replicas[i])
			if !reflect.DeepEqual(expected, got) {
				t.Fatalf("seed=%d GList: replica 0 = %v, replica %d = %v", seed, expected, i, got)
			}
		}
	}
}

// TestConvergenceGCounter verifies that GCounter replicas converge after
// random Increment operations followed by anti-entropy synchronization.
func TestConvergenceGCounter(t *testing.T) {
	for iter := 0; iter < convergenceIterations; iter++ {
		seed := int64(iter)
		rng := rand.New(rand.NewSource(seed))
		n := minReplicas + rng.Intn(maxReplicas-minReplicas+1)
		numOps := minOps + rng.Intn(maxOps-minOps+1)

		replicas := make([]*Replica[*GCounter], n)
		for i := range replicas {
			replicas[i] = NewGCounterReplica(ReplicaID(i + 1))
		}

		for op := 0; op < numOps; op++ {
			r := replicas[rng.Intn(n)]
			amount := uint64(rng.Intn(10) + 1)
			rid := r.Local.Replica()
			r.Data.Increment(rid, amount)
			// GCounter uses the count as the dot counter, so record it
			// in the received clock manually.
			count := r.Data.Get(rid)
			r.Received.Record(rid, count)
			r.Local.SetCounter(count)
		}

		antiEntropySync(replicas)

		expected := replicas[0].Data.Int64()
		for i := 1; i < n; i++ {
			got := replicas[i].Data.Int64()
			if expected != got {
				t.Fatalf("seed=%d GCounter: replica 0 = %d, replica %d = %d", seed, expected, i, got)
			}
		}
	}
}

// TestConvergencePNCounter verifies that PNCounter replicas converge after
// random Increment and Decrement operations followed by anti-entropy
// synchronization.
func TestConvergencePNCounter(t *testing.T) {
	for iter := 0; iter < convergenceIterations; iter++ {
		seed := int64(iter)
		rng := rand.New(rand.NewSource(seed))
		n := minReplicas + rng.Intn(maxReplicas-minReplicas+1)
		numOps := minOps + rng.Intn(maxOps-minOps+1)

		replicas := make([]*Replica[*PNCounter], n)
		for i := range replicas {
			replicas[i] = NewPNCounterReplica(ReplicaID(i + 1))
		}

		for op := 0; op < numOps; op++ {
			r := replicas[rng.Intn(n)]
			amount := uint64(rng.Intn(10) + 1)
			rid := r.Local.Replica()
			if rng.Intn(2) == 0 {
				r.Data.Decrement(rid, amount, r.NextDot())
			} else {
				r.Data.Increment(rid, amount, r.NextDot())
			}
		}

		antiEntropySync(replicas)

		expected := replicas[0].Data.Int64()
		for i := 1; i < n; i++ {
			got := replicas[i].Data.Int64()
			if expected != got {
				t.Fatalf("seed=%d PNCounter: replica 0 = %d, replica %d = %d", seed, expected, i, got)
			}
		}
	}
}

// TestConvergenceLWWRegister verifies that LWWRegister replicas converge
// after random Set operations followed by anti-entropy synchronization.
func TestConvergenceLWWRegister(t *testing.T) {
	for iter := 0; iter < convergenceIterations; iter++ {
		seed := int64(iter)
		rng := rand.New(rand.NewSource(seed))
		n := minReplicas + rng.Intn(maxReplicas-minReplicas+1)
		numOps := minOps + rng.Intn(maxOps-minOps+1)

		replicas := make([]*Replica[*LWWRegister[string]], n)
		for i := range replicas {
			replicas[i] = NewLWWRegisterReplica[string](ReplicaID(i+1), StringCodec{})
		}

		for op := 0; op < numOps; op++ {
			r := replicas[rng.Intn(n)]
			val := randVal(rng)
			if _, err := r.Data.Set(val, r.NextDot()); err != nil {
				t.Fatalf("seed=%d op=%d Set error: %v", seed, op, err)
			}
		}

		antiEntropySync(replicas)

		val0, _, ok0 := replicas[0].Data.Get()
		for i := 1; i < n; i++ {
			valI, _, okI := replicas[i].Data.Get()
			if ok0 != okI || val0 != valI {
				t.Fatalf("seed=%d LWWRegister: replica 0 = (%v, %v), replica %d = (%v, %v)",
					seed, val0, ok0, i, valI, okI)
			}
		}
	}
}

// TestConvergenceMVRegister verifies that MVRegister replicas converge.
// Operations are broadcast immediately so that the causal context in
// Write deltas reflects the correct state.
func TestConvergenceMVRegister(t *testing.T) {
	for iter := 0; iter < convergenceIterations; iter++ {
		seed := int64(iter)
		rng := rand.New(rand.NewSource(seed))
		n := minReplicas + rng.Intn(maxReplicas-minReplicas+1)
		numOps := minOps + rng.Intn(maxOps-minOps+1)

		replicas := make([]*Replica[*MVRegister[string]], n)
		for i := range replicas {
			replicas[i] = NewMVRegisterReplica[string](ReplicaID(i+1), StringCodec{})
		}

		for op := 0; op < numOps; op++ {
			idx := rng.Intn(n)
			r := replicas[idx]
			val := randVal(rng)
			delta, err := r.Data.Write(val, r.NextDot(), r.HWM())
			if err != nil {
				t.Fatalf("seed=%d op=%d Write error: %v", seed, op, err)
			}
			for j, dst := range replicas {
				if j == idx {
					continue
				}
				if err := dst.ApplyDelta(delta); err != nil {
					t.Fatalf("seed=%d op=%d ApplyDelta error: %v", seed, op, err)
				}
			}
		}

		antiEntropySync(replicas)

		expected := snapshotMVRegister(replicas[0])
		for i := 1; i < n; i++ {
			got := snapshotMVRegister(replicas[i])
			if !reflect.DeepEqual(expected, got) {
				t.Fatalf("seed=%d MVRegister: replica 0 = %v, replica %d = %v", seed, expected, i, got)
			}
		}
	}
}
