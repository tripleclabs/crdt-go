package crdt

import "encoding/binary"

// PNCounter is a positive-negative counter. Two maps: positive and negative
// counts per replica, each tracked with a [Dot] for causality. Value =
// sum(positive) - sum(negative).
//
// PNCounter implements [Mergeable] for use with [Replica] and
// [AlwaysMergeClock] (max-wins comparison is done in [PNCounter.Apply]).
type PNCounter struct {
	positive map[ReplicaID]pnEntry
	negative map[ReplicaID]pnEntry
}

type pnEntry struct {
	count uint64
	dot   Dot
}

// PNCounter op codes within the delta.
const (
	pnInc byte = 0x01
	pnDec byte = 0x02
)

// NewPNCounter returns an initialized PNCounter.
func NewPNCounter() *PNCounter {
	return &PNCounter{
		positive: make(map[ReplicaID]pnEntry),
		negative: make(map[ReplicaID]pnEntry),
	}
}

// NewPNCounterReplica creates a [Replica] wrapping a [PNCounter] with
// [AlwaysMergeClock]. Max-wins comparison is handled internally by Apply.
func NewPNCounterReplica(replicaID ReplicaID) *Replica[*PNCounter] {
	return NewReplica[*PNCounter](replicaID, NewPNCounter(), AlwaysMergeClock{})
}

// --- Mutations ---

// Increment adds amount to the positive side and returns the encoded delta.
// The dot provides causality tracking for the ReceivedClock.
//
// Delta format: [1 byte op=0x01][8 bytes replica][8 bytes count][16 bytes dot]
func (c *PNCounter) Increment(replica ReplicaID, amount uint64, dot Dot) []byte {
	e := c.positive[replica]
	e.count += amount
	e.dot = dot
	c.positive[replica] = e
	return encodePNDelta(pnInc, replica, e.count, dot)
}

// Decrement adds amount to the negative side and returns the encoded delta.
//
// Delta format: [1 byte op=0x02][8 bytes replica][8 bytes count][16 bytes dot]
func (c *PNCounter) Decrement(replica ReplicaID, amount uint64, dot Dot) []byte {
	e := c.negative[replica]
	e.count += amount
	e.dot = dot
	c.negative[replica] = e
	return encodePNDelta(pnDec, replica, e.count, dot)
}

// --- Reads ---

// SetPositive sets the positive count for a replica.
func (c *PNCounter) SetPositive(replica ReplicaID, count uint64) {
	e := c.positive[replica]
	e.count = count
	c.positive[replica] = e
}

// SetNegative sets the negative count for a replica.
func (c *PNCounter) SetNegative(replica ReplicaID, count uint64) {
	e := c.negative[replica]
	e.count = count
	c.negative[replica] = e
}

// GetPositive returns the positive count for a replica.
func (c *PNCounter) GetPositive(replica ReplicaID) uint64 {
	return c.positive[replica].count
}

// GetNegative returns the negative count for a replica.
func (c *PNCounter) GetNegative(replica ReplicaID) uint64 {
	return c.negative[replica].count
}

// Int64 returns the counter value: sum(positive) - sum(negative).
func (c *PNCounter) Int64() int64 {
	var pos, neg uint64
	for _, e := range c.positive {
		pos += e.count
	}
	for _, e := range c.negative {
		neg += e.count
	}
	return int64(pos) - int64(neg)
}

// RangePositive calls fn for each replica's positive count.
func (c *PNCounter) RangePositive(fn func(replica ReplicaID, count uint64) bool) {
	for r, e := range c.positive {
		if !fn(r, e.count) {
			return
		}
	}
}

// RangeNegative calls fn for each replica's negative count.
func (c *PNCounter) RangeNegative(fn func(replica ReplicaID, count uint64) bool) {
	for r, e := range c.negative {
		if !fn(r, e.count) {
			return
		}
	}
}

// --- Queryable ---

// EntryMeta returns false — PNCounter uses AlwaysMergeClock so this is
// never called.
func (c *PNCounter) EntryMeta(string) ([]byte, bool) {
	return nil, false
}

// TombstoneMeta always returns false — PNCounter has no tombstones.
func (c *PNCounter) TombstoneMeta(string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [DeltaInfo] from a PNCounter delta.
func (c *PNCounter) ParseDelta(delta []byte) (DeltaInfo, error) {
	if len(delta) < 33 {
		return DeltaInfo{}, ErrShortBuffer
	}
	op := delta[0]
	dot, err := DecodeDot(delta[17:33])
	if err != nil {
		return DeltaInfo{}, err
	}
	return DeltaInfo{
		Op:   op,
		Dots: []Dot{dot},
	}, nil
}

// Apply merges a remote delta using max-wins per side. If the remote count
// exceeds the local count for the same replica and side, the local count is
// updated.
func (c *PNCounter) Apply(delta []byte) error {
	if len(delta) < 33 {
		return ErrShortBuffer
	}
	op := delta[0]
	replica := binary.BigEndian.Uint64(delta[1:9])
	count := binary.BigEndian.Uint64(delta[9:17])
	dot, err := DecodeDot(delta[17:33])
	if err != nil {
		return err
	}

	switch op {
	case pnInc:
		if count > c.positive[replica].count {
			c.positive[replica] = pnEntry{count: count, dot: dot}
		}
	case pnDec:
		if count > c.negative[replica].count {
			c.negative[replica] = pnEntry{count: count, dot: dot}
		}
	default:
		return ErrUnknownOp
	}
	return nil
}

// DeltasSince returns deltas for entries with dots not covered by peerHWM.
func (c *PNCounter) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte
	for replica, e := range c.positive {
		if e.dot.Counter > peerHWM.Get(e.dot.Replica) {
			deltas = append(deltas, encodePNDelta(pnInc, replica, e.count, e.dot))
		}
	}
	for replica, e := range c.negative {
		if e.dot.Counter > peerHWM.Get(e.dot.Replica) {
			deltas = append(deltas, encodePNDelta(pnDec, replica, e.count, e.dot))
		}
	}
	return deltas
}

func encodePNDelta(op byte, replica ReplicaID, count uint64, dot Dot) []byte {
	b := make([]byte, 33)
	b[0] = op
	binary.BigEndian.PutUint64(b[1:9], replica)
	binary.BigEndian.PutUint64(b[9:17], count)
	copy(b[17:33], EncodeDot(dot))
	return b
}
