package crdt

// PNCounter is a positive-negative counter CRDT that supports both increment
// and decrement operations. It is implemented as two [GCounter]-style maps:
// one for positive counts and one for negative counts. The value is the
// difference: sum(positive) - sum(negative).
//
// The zero value is not usable; create instances with [NewPNCounter].
type PNCounter struct {
	replica  ReplicaID
	positive map[ReplicaID]uint64
	negative map[ReplicaID]uint64
}

// NewPNCounter returns a new PNCounter owned by the given replica.
func NewPNCounter(replica ReplicaID) *PNCounter {
	return &PNCounter{
		replica:  replica,
		positive: make(map[ReplicaID]uint64),
		negative: make(map[ReplicaID]uint64),
	}
}

// Increment adds amount to the positive count for this replica and returns
// the new state with a [Delta]. The receiver is not modified.
func (c *PNCounter) Increment(amount uint64) (*PNCounter, *Delta) {
	newPos := cloneMapU64(c.positive)
	newPos[c.replica] += amount

	next := &PNCounter{
		replica:  c.replica,
		positive: newPos,
		negative: cloneMapU64(c.negative),
	}

	delta := &PNCounter{
		replica:  c.replica,
		positive: map[ReplicaID]uint64{c.replica: newPos[c.replica]},
		negative: make(map[ReplicaID]uint64),
	}
	return next, &Delta{Type: TypePNCounter, State: delta}
}

// Decrement adds amount to the negative count for this replica and returns
// the new state with a [Delta]. The receiver is not modified.
func (c *PNCounter) Decrement(amount uint64) (*PNCounter, *Delta) {
	newNeg := cloneMapU64(c.negative)
	newNeg[c.replica] += amount

	next := &PNCounter{
		replica:  c.replica,
		positive: cloneMapU64(c.positive),
		negative: newNeg,
	}

	delta := &PNCounter{
		replica:  c.replica,
		positive: make(map[ReplicaID]uint64),
		negative: map[ReplicaID]uint64{c.replica: newNeg[c.replica]},
	}
	return next, &Delta{Type: TypePNCounter, State: delta}
}

// Value returns the counter value as an int64: sum(positive) - sum(negative).
func (c *PNCounter) Value() any {
	return c.Int64()
}

// Int64 returns the counter value as a typed int64.
func (c *PNCounter) Int64() int64 {
	var pos, neg uint64
	for _, v := range c.positive {
		pos += v
	}
	for _, v := range c.negative {
		neg += v
	}
	return int64(pos) - int64(neg)
}

// VClock returns a combined vector clock. For each replica, the clock entry
// is the sum of positive and negative counts, reflecting the total number of
// operations from that replica.
func (c *PNCounter) VClock() VClock {
	vc := make(VClock)
	for r, v := range c.positive {
		vc[r] += v
	}
	for r, v := range c.negative {
		vc[r] += v
	}
	return vc
}

// Merge merges a remote PNCounter state and returns the result. For each
// replica, the maximum count is kept in both the positive and negative maps.
// The receiver is not modified.
func (c *PNCounter) Merge(other State) State {
	o := other.(*PNCounter)
	mergedPos := cloneMapU64(c.positive)
	for r, v := range o.positive {
		if v > mergedPos[r] {
			mergedPos[r] = v
		}
	}
	mergedNeg := cloneMapU64(c.negative)
	for r, v := range o.negative {
		if v > mergedNeg[r] {
			mergedNeg[r] = v
		}
	}
	return &PNCounter{
		replica:  c.replica,
		positive: mergedPos,
		negative: mergedNeg,
	}
}

// CRDTType returns [TypePNCounter].
func (c *PNCounter) CRDTType() TypeID {
	return TypePNCounter
}

// MarshalBinary encodes the PNCounter into a binary format.
func (c *PNCounter) MarshalBinary() ([]byte, error) {
	return gobEncode(c.replica, c.positive, c.negative)
}

// UnmarshalBinary decodes a PNCounter from binary format.
func (c *PNCounter) UnmarshalBinary(data []byte) error {
	return gobDecode(data, &c.replica, &c.positive, &c.negative)
}
