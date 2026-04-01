package crdtbolt

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/3clabs/crdt"
	"pgregory.net/rapid"
)

func boltBackend(t *rapid.T, dir string, name string) *BoltBackend {
	path := filepath.Join(dir, name+".db")
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// convergeByDelta: each replica applies all deltas from all replicas.
// This avoids the commutativity issue of mutable Merge.

func TestBoltLWWMap_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		dir, _ := os.MkdirTemp("", "crdt-bolt-conv-*")
		defer os.RemoveAll(dir)

		n := rapid.IntRange(2, 3).Draw(t, "n")
		ops := rapid.IntRange(1, 10).Draw(t, "ops")

		replicas := make([]*crdt.LWWMap, n)
		backends := make([]*BoltBackend, n)
		for i := range replicas {
			backends[i] = boltBackend(t, dir, fmt.Sprintf("lww-%d", i))
			replicas[i] = crdt.NewLWWMap(crdt.ReplicaID(i+1), crdt.WithBackend(backends[i]))
		}
		defer func() {
			for _, b := range backends {
				b.Close()
			}
		}()

		// Collect all deltas.
		var deltas []crdt.State
		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			key := rapid.StringMatching(`[a-c]`).Draw(t, "key")
			var d *crdt.Delta
			if rapid.Bool().Draw(t, "put") {
				val := rapid.String().Draw(t, "val")
				d = replicas[r].Put(key, val)
			} else {
				d = replicas[r].Remove(key)
			}
			deltas = append(deltas, d.State)
		}

		// Each replica merges all deltas.
		for i := range replicas {
			for _, d := range deltas {
				replicas[i].Merge(d)
			}
		}

		expected := replicas[0].Map()
		for _, r := range replicas[1:] {
			got := r.Map()
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("BoltLWWMap diverged: %v vs %v", got, expected)
			}
		}
	})
}

func TestBoltORSet_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		dir, _ := os.MkdirTemp("", "crdt-bolt-conv-*")
		defer os.RemoveAll(dir)

		n := rapid.IntRange(2, 3).Draw(t, "n")
		ops := rapid.IntRange(1, 10).Draw(t, "ops")

		replicas := make([]*crdt.ORSet, n)
		backends := make([]*BoltBackend, n)
		for i := range replicas {
			backends[i] = boltBackend(t, dir, fmt.Sprintf("orset-%d", i))
			replicas[i] = crdt.NewORSet(crdt.ReplicaID(i+1), crdt.WithBackend(backends[i]))
		}
		defer func() {
			for _, b := range backends {
				b.Close()
			}
		}()

		var deltas []crdt.State
		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			val := rapid.StringMatching(`[a-e]`).Draw(t, "val")
			var d *crdt.Delta
			if rapid.Bool().Draw(t, "add") {
				d = replicas[r].Add(val)
			} else {
				d = replicas[r].Remove(val)
			}
			deltas = append(deltas, d.State)
		}

		for i := range replicas {
			for _, d := range deltas {
				replicas[i].Merge(d)
			}
		}

		expected := sortedAny(replicas[0].Elements())
		for _, r := range replicas[1:] {
			got := sortedAny(r.Elements())
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("BoltORSet diverged: %v vs %v", got, expected)
			}
		}
	})
}

func TestBoltGList_Convergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		dir, _ := os.MkdirTemp("", "crdt-bolt-conv-*")
		defer os.RemoveAll(dir)

		n := rapid.IntRange(2, 3).Draw(t, "n")
		ops := rapid.IntRange(1, 10).Draw(t, "ops")

		replicas := make([]*crdt.GList, n)
		backends := make([]*BoltBackend, n)
		for i := range replicas {
			backends[i] = boltBackend(t, dir, fmt.Sprintf("glist-%d", i))
			replicas[i] = crdt.NewGList(crdt.ReplicaID(i+1), crdt.WithBackend(backends[i]))
		}
		defer func() {
			for _, b := range backends {
				b.Close()
			}
		}()

		var deltas []crdt.State
		for i := 0; i < ops; i++ {
			r := rapid.IntRange(0, n-1).Draw(t, "r")
			val := rapid.String().Draw(t, "val")
			d := replicas[r].Append(val)
			deltas = append(deltas, d.State)
		}

		for i := range replicas {
			for _, d := range deltas {
				replicas[i].Merge(d)
			}
		}

		expected := replicas[0].Items()
		for _, r := range replicas[1:] {
			got := r.Items()
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("BoltGList diverged: %v vs %v", got, expected)
			}
		}
	})
}

func sortedAny(s []any) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[i] = fmt.Sprint(v)
	}
	sort.Strings(out)
	return out
}
