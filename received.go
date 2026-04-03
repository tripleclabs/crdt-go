package crdt

// receivedClock tracks which operations have been received from each remote
// replica. It handles out-of-order delivery by maintaining a set of received
// counters per replica and computing the contiguous high-water mark.
//
// The high-water mark for a replica is the largest N such that all counters
// 1..N have been received. This is what gets sent to peers during
// anti-entropy — "I have everything from replica R up to N."
type receivedClock struct {
	// hwm is the high-water mark per replica: the largest contiguous counter.
	hwm map[ReplicaID]uint64
	// pending tracks counters received above the hwm (out-of-order).
	// Only populated when there are gaps.
	pending map[ReplicaID]map[uint64]struct{}
}

// newReceivedClock returns an initialized ReceivedClock.
func newReceivedClock() *receivedClock {
	return &receivedClock{
		hwm:     make(map[ReplicaID]uint64),
		pending: make(map[ReplicaID]map[uint64]struct{}),
	}
}

// Record records that a counter from the given replica has been received.
// This is called when applying a delta. The high-water mark is advanced
// if the counter fills a contiguous gap.
func (rc *receivedClock) Record(replica ReplicaID, counter uint64) {
	if counter == 0 {
		return
	}
	current := rc.hwm[replica]

	if counter <= current {
		// Already seen (at or below hwm).
		return
	}

	if counter == current+1 {
		// Next in sequence — advance hwm.
		rc.hwm[replica] = counter
		// Check if pending counters now fill the gap.
		rc.advanceHWM(replica)
	} else {
		// Out of order — record in pending.
		if rc.pending[replica] == nil {
			rc.pending[replica] = make(map[uint64]struct{})
		}
		rc.pending[replica][counter] = struct{}{}
	}
}

// advanceHWM advances the high-water mark using pending counters.
func (rc *receivedClock) advanceHWM(replica ReplicaID) {
	pending := rc.pending[replica]
	if pending == nil {
		return
	}
	for {
		next := rc.hwm[replica] + 1
		if _, ok := pending[next]; ok {
			rc.hwm[replica] = next
			delete(pending, next)
		} else {
			break
		}
	}
	if len(pending) == 0 {
		delete(rc.pending, replica)
	}
}

// Get returns the high-water mark for the given replica — the largest N
// such that all operations 1..N have been received.
func (rc *receivedClock) Get(replica ReplicaID) uint64 {
	return rc.hwm[replica]
}

// HWM returns the full high-water mark map. This is what gets sent to
// peers during anti-entropy.
func (rc *receivedClock) HWM() VClock {
	out := make(VClock, len(rc.hwm))
	for r, c := range rc.hwm {
		out[r] = c
	}
	return out
}

// Covers reports whether the counter from the given replica has been
// received (either at or below the hwm, or in pending).
func (rc *receivedClock) Covers(replica ReplicaID, counter uint64) bool {
	if counter <= rc.hwm[replica] {
		return true
	}
	if pending, ok := rc.pending[replica]; ok {
		_, found := pending[counter]
		return found
	}
	return false
}

// SetHWM sets the high-water mark for a replica directly. Used when
// initializing from a known state (e.g., after applying a full snapshot).
func (rc *receivedClock) SetHWM(replica ReplicaID, counter uint64) {
	rc.hwm[replica] = counter
	rc.advanceHWM(replica)
}

// Clone returns a deep copy of the received clock.
func (rc *receivedClock) Clone() *receivedClock {
	c := &receivedClock{
		hwm:     make(map[ReplicaID]uint64, len(rc.hwm)),
		pending: make(map[ReplicaID]map[uint64]struct{}, len(rc.pending)),
	}
	for r, v := range rc.hwm {
		c.hwm[r] = v
	}
	for r, p := range rc.pending {
		cp := make(map[uint64]struct{}, len(p))
		for k := range p {
			cp[k] = struct{}{}
		}
		c.pending[r] = cp
	}
	return c
}
