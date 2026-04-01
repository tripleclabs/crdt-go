package crdt

import "encoding/binary"

// PNCounter is a positive-negative counter. Two maps: positive and negative
// counts per replica. Value = sum(positive) - sum(negative).
//
// PNCounter implements [Mergeable] for use with [Replica] and [MaxWinsClock].
type PNCounter struct {
	positive map[ReplicaID]uint64
	negative map[ReplicaID]uint64
}

// PNCounter op codes within the delta.
const (
	pnInc byte = 0x01
	pnDec byte = 0x02
)

// NewPNCounter returns an initialized PNCounter.
func NewPNCounter() *PNCounter {
	return &PNCounter{
		positive: make(map[ReplicaID]uint64),
		negative: make(map[ReplicaID]uint64),
	}
}

// NewPNCounterReplica creates a [Replica] wrapping a [PNCounter] with [MaxWinsClock].
func NewPNCounterReplica(replicaID ReplicaID) *Replica[*PNCounter] {
	return NewReplica[*PNCounter](replicaID, NewPNCounter(), MaxWinsClock{})
}

// --- Mutations ---

// Increment adds amount to the positive side and returns the encoded delta.
// Delta format: [1 byte op=0x01][8 bytes replica][8 bytes count]
func (c *PNCounter) Increment(replica ReplicaID, amount uint64) []byte {
	newCount := c.positive[replica] + amount
	c.positive[replica] = newCount
	return encodePNDelta(pnInc, replica, newCount)
}

// Decrement adds amount to the negative side and returns the encoded delta.
// Delta format: [1 byte op=0x02][8 bytes replica][8 bytes count]
func (c *PNCounter) Decrement(replica ReplicaID, amount uint64) []byte {
	newCount := c.negative[replica] + amount
	c.negative[replica] = newCount
	return encodePNDelta(pnDec, replica, newCount)
}

// --- Reads ---

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

// --- Queryable ---

// EntryMeta returns the count for the given key as 8-byte big-endian.
// The key encodes the op (pnInc/pnDec) and replica ID.
func (c *PNCounter) EntryMeta(key string) ([]byte, bool) {
	if len(key) < 9 {
		return nil, false
	}
	op := key[0]
	rid := binary.BigEndian.Uint64([]byte(key[1:]))
	var count uint64
	var ok bool
	switch op {
	case pnInc:
		count, ok = c.positive[rid]
	case pnDec:
		count, ok = c.negative[rid]
	default:
		return nil, false
	}
	if !ok {
		return nil, false
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, count)
	return b, true
}

// TombstoneMeta always returns false — PNCounter has no tombstones.
func (c *PNCounter) TombstoneMeta(string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [DeltaInfo] from a PNCounter delta.
func (c *PNCounter) ParseDelta(delta []byte) (DeltaInfo, error) {
	if len(delta) < 17 {
		return DeltaInfo{}, ErrShortBuffer
	}
	op := delta[0]
	replica := binary.BigEndian.Uint64(delta[1:9])
	count := binary.BigEndian.Uint64(delta[9:17])

	// Build a key that encodes the side (inc/dec) + replica.
	key := make([]byte, 9)
	key[0] = op
	binary.BigEndian.PutUint64(key[1:], replica)

	return DeltaInfo{
		Op:   op,
		Key:  string(key),
		Meta: delta[9:17],
		Dots: []Dot{{Replica: replica, Counter: count}},
	}, nil
}

// Apply unconditionally sets the count for the replica.
func (c *PNCounter) Apply(delta []byte) error {
	if len(delta) < 17 {
		return ErrShortBuffer
	}
	op := delta[0]
	replica := binary.BigEndian.Uint64(delta[1:9])
	count := binary.BigEndian.Uint64(delta[9:17])

	switch op {
	case pnInc:
		c.positive[replica] = count
	case pnDec:
		c.negative[replica] = count
	default:
		return ErrUnknownOp
	}
	return nil
}

// DeltasSince returns deltas for replicas with counts above peerHWM.
func (c *PNCounter) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte
	for replica, count := range c.positive {
		if count > peerHWM.Get(replica) {
			deltas = append(deltas, encodePNDelta(pnInc, replica, count))
		}
	}
	for replica, count := range c.negative {
		if count > peerHWM.Get(replica) {
			deltas = append(deltas, encodePNDelta(pnDec, replica, count))
		}
	}
	return deltas
}

func encodePNDelta(op byte, replica ReplicaID, count uint64) []byte {
	b := make([]byte, 17)
	b[0] = op
	binary.BigEndian.PutUint64(b[1:9], replica)
	binary.BigEndian.PutUint64(b[9:17], count)
	return b
}
