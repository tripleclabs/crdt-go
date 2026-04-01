package crdt

// GCounter is a grow-only counter. Each replica maintains its own
// monotonically increasing count, and the total value is the sum.
//
// This is pure storage — no clocks, no merge logic.
type GCounter struct {
	counts map[ReplicaID]uint64
}

// NewGCounter returns an initialized GCounter.
func NewGCounter() *GCounter {
	return &GCounter{counts: make(map[ReplicaID]uint64)}
}

// Set sets the count for a replica. The caller is responsible for ensuring
// this is only used with correct values (e.g., max-wins in the replica layer).
func (c *GCounter) Set(replica ReplicaID, count uint64) {
	c.counts[replica] = count
}

// Get returns the count for a replica.
func (c *GCounter) Get(replica ReplicaID) uint64 {
	return c.counts[replica]
}

// Range calls fn for each replica-count pair.
func (c *GCounter) Range(fn func(replica ReplicaID, count uint64) bool) {
	for r, v := range c.counts {
		if !fn(r, v) {
			return
		}
	}
}

// Int64 returns the total count as int64 (sum of all replicas).
func (c *GCounter) Int64() int64 {
	var total uint64
	for _, v := range c.counts {
		total += v
	}
	return int64(total)
}

// Len returns the number of replicas with counts.
func (c *GCounter) Len() int {
	return len(c.counts)
}
