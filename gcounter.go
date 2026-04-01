package crdt

// GCounter is a grow-only counter CRDT. Each replica maintains its own
// monotonically increasing count, and the total value is the sum of all
// replicas' counts. GCounter supports only increment — for decrement,
// use [PNCounter].
//
// The zero value is not usable; create instances with [NewGCounter].
type GCounter struct {
	replica ReplicaID
	counts  map[ReplicaID]uint64
}

// NewGCounter returns a new GCounter owned by the given replica.
func NewGCounter(replica ReplicaID) *GCounter {
	return &GCounter{
		replica: replica,
		counts:  make(map[ReplicaID]uint64),
	}
}

// Increment adds amount to this replica's count and returns the new counter
// state along with a [Delta] containing only the changed replica's count.
// The receiver is not modified.
func (c *GCounter) Increment(amount uint64) (*GCounter, *Delta) {
	newCounts := cloneMapU64(c.counts)
	newCounts[c.replica] += amount

	next := &GCounter{replica: c.replica, counts: newCounts}

	deltaCounts := map[ReplicaID]uint64{c.replica: newCounts[c.replica]}
	delta := &GCounter{replica: c.replica, counts: deltaCounts}

	return next, &Delta{Type: TypeGCounter, State: delta}
}

// Value returns the total count as an int64 (sum of all replicas' counts).
func (c *GCounter) Value() any {
	return c.Int64()
}

// Int64 returns the total count as a typed int64.
func (c *GCounter) Int64() int64 {
	var total uint64
	for _, v := range c.counts {
		total += v
	}
	return int64(total)
}

// VClock returns the vector clock for this counter. For a GCounter, the
// counts map doubles as the vector clock — each replica's count is its
// logical timestamp.
func (c *GCounter) VClock() VClock {
	vc := make(VClock, len(c.counts))
	for r, v := range c.counts {
		vc[r] = v
	}
	return vc
}

// Merge merges a remote GCounter state into this counter and returns the
// merged result. For each replica, the maximum count is kept. The receiver
// is not modified.
func (c *GCounter) Merge(other State) State {
	o := other.(*GCounter)
	merged := make(map[ReplicaID]uint64, len(c.counts))
	for r, v := range c.counts {
		merged[r] = v
	}
	for r, v := range o.counts {
		if v > merged[r] {
			merged[r] = v
		}
	}
	return &GCounter{replica: c.replica, counts: merged}
}

// CRDTType returns [TypeGCounter].
func (c *GCounter) CRDTType() TypeID {
	return TypeGCounter
}

// MarshalBinary encodes the GCounter into a binary format.
func (c *GCounter) MarshalBinary() ([]byte, error) {
	return gobEncode(c.replica, c.counts)
}

// UnmarshalBinary decodes a GCounter from binary format.
func (c *GCounter) UnmarshalBinary(data []byte) error {
	return gobDecode(data, &c.replica, &c.counts)
}

func cloneMapU64(m map[ReplicaID]uint64) map[ReplicaID]uint64 {
	out := make(map[ReplicaID]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
