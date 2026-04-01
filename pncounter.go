package crdt

// PNCounter is a positive-negative counter. Two maps: positive and negative
// counts per replica. Value = sum(positive) - sum(negative).
//
// This is pure storage — no clocks, no merge logic.
type PNCounter struct {
	positive map[ReplicaID]uint64
	negative map[ReplicaID]uint64
}

// NewPNCounter returns an initialized PNCounter.
func NewPNCounter() *PNCounter {
	return &PNCounter{
		positive: make(map[ReplicaID]uint64),
		negative: make(map[ReplicaID]uint64),
	}
}

// SetPositive sets the positive count for a replica.
func (c *PNCounter) SetPositive(replica ReplicaID, count uint64) {
	c.positive[replica] = count
}

// SetNegative sets the negative count for a replica.
func (c *PNCounter) SetNegative(replica ReplicaID, count uint64) {
	c.negative[replica] = count
}

// GetPositive returns the positive count for a replica.
func (c *PNCounter) GetPositive(replica ReplicaID) uint64 {
	return c.positive[replica]
}

// GetNegative returns the negative count for a replica.
func (c *PNCounter) GetNegative(replica ReplicaID) uint64 {
	return c.negative[replica]
}

// Int64 returns the counter value: sum(positive) - sum(negative).
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

// RangePositive calls fn for each replica's positive count.
func (c *PNCounter) RangePositive(fn func(replica ReplicaID, count uint64) bool) {
	for r, v := range c.positive {
		if !fn(r, v) {
			return
		}
	}
}

// RangeNegative calls fn for each replica's negative count.
func (c *PNCounter) RangeNegative(fn func(replica ReplicaID, count uint64) bool) {
	for r, v := range c.negative {
		if !fn(r, v) {
			return
		}
	}
}
