package crdtbolt

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/3clabs/crdt"
)

// TestBoltLWWMap_TwoReplicaConvergence tests two bolt-backed LWWMap replicas
// exchanging deltas and converging to identical state.
func TestBoltLWWMap_TwoReplicaConvergence(t *testing.T) {
	ba := tempDB(t)
	bb := tempDB(t)

	a := crdt.NewLWWMapReplica[string](1, crdt.StringCodec{}, crdt.WithBackend(ba))
	b := crdt.NewLWWMapReplica[string](2, crdt.StringCodec{}, crdt.WithBackend(bb))

	// Each replica does some puts.
	a.Data.Put("name", "alice", a.NextDot())
	a.Data.Put("city", "paris", a.NextDot())
	b.Data.Put("name", "bob", b.NextDot())
	b.Data.Put("lang", "go", b.NextDot())

	// Anti-entropy: a -> b, then b -> a.
	for _, d := range a.DeltasSince(b.HWM()) {
		if err := b.ApplyDelta(d); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range b.DeltasSince(a.HWM()) {
		if err := a.ApplyDelta(d); err != nil {
			t.Fatal(err)
		}
	}

	// Collect entries from both and verify convergence.
	collectEntries := func(r *crdt.Replica[*crdt.LWWMap[string]]) []string {
		var entries []string
		r.Data.Range(func(key string, value string, dot crdt.Dot) bool {
			entries = append(entries, key+"="+value)
			return true
		})
		sort.Strings(entries)
		return entries
	}

	ea := collectEntries(a)
	eb := collectEntries(b)

	if len(ea) != len(eb) {
		t.Fatalf("different entry counts: a=%d b=%d", len(ea), len(eb))
	}
	for i := range ea {
		if ea[i] != eb[i] {
			t.Fatalf("diverged at %d: a=%q b=%q", i, ea[i], eb[i])
		}
	}

	// Both should have 3 keys: city, lang, and name (name resolved by LWW).
	if len(ea) != 3 {
		t.Fatalf("expected 3 entries, got %d: %v", len(ea), ea)
	}
	t.Logf("converged: %v", ea)
}

// TestBoltORSet_AntiEntropy tests two bolt-backed ORSet replicas exchanging
// elements via anti-entropy and converging.
func TestBoltORSet_AntiEntropy(t *testing.T) {
	ba := tempDB(t)
	bb := tempDB(t)

	a := crdt.NewORSetReplica[string](1, crdt.StringCodec{}, crdt.WithBackend(ba))
	b := crdt.NewORSetReplica[string](2, crdt.StringCodec{}, crdt.WithBackend(bb))

	// Each adds different elements.
	a.Data.Add("apple", a.NextDot())
	a.Data.Add("banana", a.NextDot())
	b.Data.Add("cherry", b.NextDot())
	b.Data.Add("date", b.NextDot())

	// Anti-entropy exchange.
	for _, d := range a.DeltasSince(b.HWM()) {
		if err := b.ApplyDelta(d); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range b.DeltasSince(a.HWM()) {
		if err := a.ApplyDelta(d); err != nil {
			t.Fatal(err)
		}
	}

	// Both should have all 4 elements.
	for name, r := range map[string]*crdt.Replica[*crdt.ORSet[string]]{"a": a, "b": b} {
		elems, err := r.Data.Elements()
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(elems)
		if len(elems) != 4 {
			t.Fatalf("replica %s: expected 4 elements, got %d: %v", name, len(elems), elems)
		}
		expected := []string{"apple", "banana", "cherry", "date"}
		for i, e := range expected {
			if elems[i] != e {
				t.Fatalf("replica %s: expected %q at %d, got %q", name, e, i, elems[i])
			}
		}
	}
}

// TestBoltLWWMap_PersistAndRecover tests that a bolt-backed replica survives
// close and reopen, and that anti-entropy works after recovery.
func TestBoltLWWMap_PersistAndRecover(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persist.db")

	// Phase 1: create replica, do operations, close.
	b1, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	r1 := crdt.NewLWWMapReplica[string](1, crdt.StringCodec{}, crdt.WithBackend(b1))

	r1.Data.Put("color", "blue", r1.NextDot())
	r1.Data.Put("size", "large", r1.NextDot())
	r1.Data.Put("temp", "warm", r1.NextDot())
	r1.Data.Remove("temp", r1.NextDot())

	// Verify state before close.
	if r1.Data.Len() != 2 {
		t.Fatalf("expected 2 entries before close, got %d", r1.Data.Len())
	}

	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: reopen and verify data survived.
	b2, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b2.Close() })

	r2 := crdt.NewLWWMapReplica[string](1, crdt.StringCodec{}, crdt.WithBackend(b2))

	v, _, ok := r2.Data.Get("color")
	if !ok || v != "blue" {
		t.Fatalf("expected color=blue after reopen, got %v ok=%v", v, ok)
	}
	v, _, ok = r2.Data.Get("size")
	if !ok || v != "large" {
		t.Fatalf("expected size=large after reopen, got %v ok=%v", v, ok)
	}
	_, _, ok = r2.Data.Get("temp")
	if ok {
		t.Fatal("temp should still be removed after reopen")
	}
	if r2.Data.Len() != 2 {
		t.Fatalf("expected 2 entries after reopen, got %d", r2.Data.Len())
	}

	// Phase 3: verify DeltasSince works after recovery for anti-entropy.
	// A fresh peer with empty HWM should receive all deltas from the recovered replica.
	b3 := tempDB(t)
	peer := crdt.NewLWWMapReplica[string](2, crdt.StringCodec{}, crdt.WithBackend(b3))

	deltas := r2.DeltasSince(peer.HWM())
	if len(deltas) == 0 {
		t.Fatal("expected deltas from recovered replica, got none")
	}
	for _, d := range deltas {
		if err := peer.ApplyDelta(d); err != nil {
			t.Fatal(err)
		}
	}

	// Peer should now have the same live entries.
	pv, _, ok := peer.Data.Get("color")
	if !ok || pv != "blue" {
		t.Fatalf("peer expected color=blue, got %v ok=%v", pv, ok)
	}
	pv, _, ok = peer.Data.Get("size")
	if !ok || pv != "large" {
		t.Fatalf("peer expected size=large, got %v ok=%v", pv, ok)
	}
	t.Logf("persist and recover verified; anti-entropy delivered %d deltas", len(deltas))
}
