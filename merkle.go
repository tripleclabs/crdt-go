package crdt

import (
	"hash/maphash"
	"sort"
)

// MerkleMap is a hash-based structure for efficient equality checking and
// partial diff computation on key-value data. It maintains a root hash that
// is updated lazily when entries change, enabling O(1) equality checks between
// two MerkleMaps (useful for sync protocols to detect divergence without
// comparing full state).
//
// The zero value is ready to use.
type MerkleMap struct {
	entries map[string][]byte
	hash    uint64
	dirty   bool
	seed    maphash.Seed
}

// NewMerkleMap returns an initialized MerkleMap.
func NewMerkleMap() *MerkleMap {
	return &MerkleMap{
		entries: make(map[string][]byte),
		seed:    maphash.MakeSeed(),
	}
}

// Put stores a value under key. The root hash is marked dirty and will be
// recomputed on the next call to [MerkleMap.Hash].
func (mm *MerkleMap) Put(key string, value []byte) {
	if mm.entries == nil {
		mm.entries = make(map[string][]byte)
		mm.seed = maphash.MakeSeed()
	}
	mm.entries[key] = value
	mm.dirty = true
}

// Delete removes the entry for key. The root hash is marked dirty.
func (mm *MerkleMap) Delete(key string) {
	if mm.entries == nil {
		return
	}
	delete(mm.entries, key)
	mm.dirty = true
}

// Get retrieves the value for key.
func (mm *MerkleMap) Get(key string) ([]byte, bool) {
	if mm.entries == nil {
		return nil, false
	}
	v, ok := mm.entries[key]
	return v, ok
}

// Len returns the number of entries.
func (mm *MerkleMap) Len() int {
	return len(mm.entries)
}

// Hash returns the root hash of the MerkleMap. If the map has been modified
// since the last call, the hash is recomputed. Two MerkleMaps with identical
// entries produce the same hash (order-independent).
func (mm *MerkleMap) Hash() uint64 {
	if mm.dirty || mm.hash == 0 {
		mm.recomputeHash()
		mm.dirty = false
	}
	return mm.hash
}

// Equal reports whether mm and other contain exactly the same entries.
// This first compares hashes for a fast inequality check, then falls back
// to element-wise comparison only if hashes match.
func (mm *MerkleMap) Equal(other *MerkleMap) bool {
	if mm.Len() != other.Len() {
		return false
	}
	if mm.Hash() != other.Hash() {
		return false
	}
	// Hash collision possible — verify element-wise.
	for k, v := range mm.entries {
		ov, ok := other.entries[k]
		if !ok {
			return false
		}
		if string(v) != string(ov) {
			return false
		}
	}
	return true
}

// DivergentKeys returns the keys that differ between mm and other. This is
// useful for sync protocols that need to identify which entries to exchange.
func (mm *MerkleMap) DivergentKeys(other *MerkleMap) []string {
	var divergent []string

	// Keys in mm but not in other, or with different values.
	for k, v := range mm.entries {
		if ov, ok := other.entries[k]; !ok || string(v) != string(ov) {
			divergent = append(divergent, k)
		}
	}
	// Keys in other but not in mm.
	for k := range other.entries {
		if _, ok := mm.entries[k]; !ok {
			divergent = append(divergent, k)
		}
	}

	sort.Strings(divergent)
	return divergent
}

// recomputeHash computes an order-independent hash by XORing per-entry hashes.
func (mm *MerkleMap) recomputeHash() {
	var h maphash.Hash
	h.SetSeed(mm.seed)
	var fp uint64
	for k, v := range mm.entries {
		h.Reset()
		h.WriteString(k)
		h.Write(v)
		fp ^= h.Sum64()
	}
	mm.hash = fp
}
