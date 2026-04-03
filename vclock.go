package crdt

import "hash/maphash"

// VClock is a vector clock mapping replica IDs to their logical timestamps
// (counters). Vector clocks track causality in distributed systems, allowing
// CRDTs to determine the causal relationship between operations.
//
// The zero value is a valid, empty clock. VClock values are not safe for
// concurrent use; the caller must provide synchronization if needed.
type VClock map[ReplicaID]uint64

// newVClock returns an initialized empty vector clock.
func newVClock() VClock {
	return make(VClock)
}

// Get returns the counter for the given replica. Returns 0 if the replica
// is not present in the clock.
func (vc VClock) Get(replica ReplicaID) uint64 {
	return vc[replica]
}

// Increment returns a new VClock with the given replica's counter incremented
// by one. The receiver is not modified.
func (vc VClock) Increment(replica ReplicaID) VClock {
	out := vc.Clone()
	out[replica] = out[replica] + 1
	return out
}

// Merge returns a new VClock containing the maximum counter for each replica
// present in either clock. This is the standard vector clock join (least upper
// bound) operation. The receiver is not modified.
func (vc VClock) Merge(other VClock) VClock {
	out := vc.Clone()
	for r, c := range other {
		if c > out[r] {
			out[r] = c
		}
	}
	return out
}

// LTE reports whether vc is less than or equal to other — meaning every entry
// in vc has a counter ≤ the corresponding entry in other. An empty clock is
// ≤ any clock.
//
// This is optimized with an early size check: if vc has more replicas than
// other, it cannot be ≤.
func (vc VClock) LTE(other VClock) bool {
	if len(vc) > len(other) {
		return false
	}
	for r, c := range vc {
		if c > other[r] {
			return false
		}
	}
	return true
}

// Dominates reports whether vc causally dominates other — meaning vc ≥ other.
// Equivalent to other.LTE(vc).
func (vc VClock) Dominates(other VClock) bool {
	return other.LTE(vc)
}

// Concurrent reports whether vc and other are causally concurrent — neither
// dominates the other. Concurrent clocks indicate conflicting updates that
// require merging.
func (vc VClock) Concurrent(other VClock) bool {
	return !vc.LTE(other) && !other.LTE(vc)
}

// Equal reports whether vc and other contain exactly the same replica-counter
// pairs.
func (vc VClock) Equal(other VClock) bool {
	if len(vc) != len(other) {
		return false
	}
	for r, c := range vc {
		if other[r] != c {
			return false
		}
	}
	return true
}

// Clone returns a shallow copy of the vector clock.
func (vc VClock) Clone() VClock {
	out := make(VClock, len(vc))
	for r, c := range vc {
		out[r] = c
	}
	return out
}

// LowerBound returns a new VClock containing the minimum counter for each
// replica present in both clocks. Replicas present in only one clock are
// excluded from the result.
func (vc VClock) LowerBound(other VClock) VClock {
	out := make(VClock)
	for r, c := range vc {
		if oc, ok := other[r]; ok {
			out[r] = min(c, oc)
		}
	}
	return out
}

// vclockHashSeed is a process-lifetime seed for VClock fingerprinting.
var vclockHashSeed = maphash.MakeSeed()

// Fingerprint returns a hash of the vector clock suitable for fast inequality
// checks. Two equal VClocks produce the same fingerprint. Different VClocks
// will almost always produce different fingerprints, but collisions are
// possible — use [VClock.Equal] for definitive comparison.
func (vc VClock) Fingerprint() uint64 {
	var h maphash.Hash
	h.SetSeed(vclockHashSeed)
	// Sort-independent: XOR of per-entry hashes.
	var fp uint64
	for r, c := range vc {
		h.Reset()
		// Write replica ID (8 bytes, big-endian).
		var buf [16]byte
		buf[0] = byte(r >> 56)
		buf[1] = byte(r >> 48)
		buf[2] = byte(r >> 40)
		buf[3] = byte(r >> 32)
		buf[4] = byte(r >> 24)
		buf[5] = byte(r >> 16)
		buf[6] = byte(r >> 8)
		buf[7] = byte(r)
		// Write counter (8 bytes, big-endian).
		buf[8] = byte(c >> 56)
		buf[9] = byte(c >> 48)
		buf[10] = byte(c >> 40)
		buf[11] = byte(c >> 32)
		buf[12] = byte(c >> 24)
		buf[13] = byte(c >> 16)
		buf[14] = byte(c >> 8)
		buf[15] = byte(c)
		h.Write(buf[:])
		fp ^= h.Sum64()
	}
	return fp
}
