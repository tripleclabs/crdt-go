package crdt

import "encoding/binary"

// GCounter is a grow-only counter. Each replica maintains its own
// monotonically increasing count, and the total value is the sum.
//
// GCounter implements [Mergeable] for use with [Replica] and [MaxWinsClock].
type GCounter struct {
	counts map[ReplicaID]uint64
}

// NewGCounter returns an initialized GCounter.
func NewGCounter() *GCounter {
	return &GCounter{counts: make(map[ReplicaID]uint64)}
}

// NewGCounterReplica creates a [Replica] wrapping a [GCounter] with [MaxWinsClock].
func NewGCounterReplica(replicaID ReplicaID) *Replica[*GCounter] {
	return NewReplica[*GCounter](replicaID, NewGCounter(), MaxWinsClock{})
}

// --- Mutations ---

// Increment adds amount to this replica's count and returns the encoded delta.
// The caller must also update the Replica's LocalClock via SetCounter.
//
// Delta format: [8 bytes replica][8 bytes count]
func (c *GCounter) Increment(replica ReplicaID, amount uint64) []byte {
	newCount := c.counts[replica] + amount
	c.counts[replica] = newCount

	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], replica)
	binary.BigEndian.PutUint64(buf[8:16], newCount)
	return buf
}

// --- Reads ---

// Set sets the count for a replica.
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

// --- Queryable ---

// EntryMeta returns the count for the given replica as 8-byte big-endian.
// The key is the stringified replica ID.
func (c *GCounter) EntryMeta(key string) ([]byte, bool) {
	rid := parseReplicaKey(key)
	count, ok := c.counts[rid]
	if !ok {
		return nil, false
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, count)
	return b, true
}

// TombstoneMeta always returns false — GCounter has no tombstones.
func (c *GCounter) TombstoneMeta(string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [DeltaInfo] from a GCounter delta.
func (c *GCounter) ParseDelta(delta []byte) (DeltaInfo, error) {
	if len(delta) < 16 {
		return DeltaInfo{}, ErrShortBuffer
	}
	replica := binary.BigEndian.Uint64(delta[0:8])
	count := binary.BigEndian.Uint64(delta[8:16])
	return DeltaInfo{
		Key:  formatReplicaKey(replica),
		Meta: delta[8:16],
		Dots: []Dot{{Replica: replica, Counter: count}},
	}, nil
}

// Apply unconditionally sets the count for the replica in the delta.
func (c *GCounter) Apply(delta []byte) error {
	if len(delta) < 16 {
		return ErrShortBuffer
	}
	replica := binary.BigEndian.Uint64(delta[0:8])
	count := binary.BigEndian.Uint64(delta[8:16])
	c.counts[replica] = count
	return nil
}

// DeltasSince returns deltas for replicas with counts above peerHWM.
func (c *GCounter) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte
	for replica, count := range c.counts {
		if count > peerHWM.Get(replica) {
			buf := make([]byte, 16)
			binary.BigEndian.PutUint64(buf[0:8], replica)
			binary.BigEndian.PutUint64(buf[8:16], count)
			deltas = append(deltas, buf)
		}
	}
	return deltas
}

// helpers for Queryable key encoding
func formatReplicaKey(rid ReplicaID) string {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, rid)
	return string(b)
}

func parseReplicaKey(key string) ReplicaID {
	if len(key) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64([]byte(key))
}
