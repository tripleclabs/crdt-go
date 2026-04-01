package crdt

// Dot represents a single causal event as a (replica, counter) pair. Every
// mutation in a CRDT produces a new dot by incrementing the replica's counter.
// Dots are used throughout the library for causality tracking, conflict
// detection, and last-write-wins tie-breaking.
type Dot struct {
	Replica ReplicaID
	Counter uint64
}

// DotMap is a compressed vector clock mapping replica IDs to their latest
// counters. It is used inside CRDT entries to track which replicas contributed
// to a particular element or key.
type DotMap = map[ReplicaID]uint64

// DotGT reports whether dot a is "greater than" dot b using last-write-wins
// semantics. A higher counter wins. When counters are equal, the lower replica
// ID wins (deterministic tie-breaking that ensures all replicas converge to
// the same winner).
func DotGT(a, b Dot) bool {
	if a.Counter != b.Counter {
		return a.Counter > b.Counter
	}
	return a.Replica < b.Replica
}

// MaxDot returns the greatest dot in dm according to [DotGT] semantics.
// Returns a zero Dot if dm is empty.
func MaxDot(dm DotMap) Dot {
	var best Dot
	for r, c := range dm {
		d := Dot{Replica: r, Counter: c}
		if best.Counter == 0 || DotGT(d, best) {
			best = d
		}
	}
	return best
}

// CombineDots merges two DotMaps by taking the maximum counter for each
// replica. Neither input is modified; a new map is returned.
func CombineDots(a, b DotMap) DotMap {
	out := make(DotMap, len(a))
	for r, c := range a {
		out[r] = c
	}
	for r, c := range b {
		if c > out[r] {
			out[r] = c
		}
	}
	return out
}

// NextDot returns the next dot for the given replica, computed by incrementing
// the replica's current counter in dm by one. If the replica is not present
// in dm, the counter starts at 1.
func NextDot(replica ReplicaID, dm DotMap) Dot {
	return Dot{Replica: replica, Counter: dm[replica] + 1}
}

// DotMember reports whether dot d is a member of dm — that is, whether dm
// contains an entry for d's replica with a counter ≥ d's counter. This is
// the membership test for a compressed vector clock.
func DotMember(dm DotMap, d Dot) bool {
	return dm[d.Replica] >= d.Counter
}

// CloneDotMap returns a shallow copy of dm.
func CloneDotMap(dm DotMap) DotMap {
	out := make(DotMap, len(dm))
	for r, c := range dm {
		out[r] = c
	}
	return out
}

// DotMapLTE reports whether every entry in a is ≤ the corresponding entry in b.
// Replicas present in a but absent from b are compared against 0.
func DotMapLTE(a, b DotMap) bool {
	if len(a) > len(b) {
		return false
	}
	for r, c := range a {
		if c > b[r] {
			return false
		}
	}
	return true
}

// DotMapEqual reports whether a and b contain exactly the same entries.
func DotMapEqual(a, b DotMap) bool {
	if len(a) != len(b) {
		return false
	}
	for r, c := range a {
		if b[r] != c {
			return false
		}
	}
	return true
}
