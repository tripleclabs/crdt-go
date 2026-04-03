package crdt

import "encoding/binary"

// pnCounterState is a positive-negative counter. Two maps: positive and negative
// counts per replica, each tracked with a [Dot] for causality. Value =
// sum(positive) - sum(negative).
//
// pnCounterState implements [mergeable] for use with [replica] and
// [alwaysMergeClock] (max-wins comparison is done in [pnCounterState.Apply]).
type pnCounterState struct {
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

// newPNCounterState returns an initialized PNCounter.
func newPNCounterState() *pnCounterState {
	return &pnCounterState{
		positive: make(map[ReplicaID]pnEntry),
		negative: make(map[ReplicaID]pnEntry),
	}
}

// --- Mutations ---

// Increment adds amount to the positive side and returns the encoded delta.
// The dot provides causality tracking for the ReceivedClock.
//
// Delta format: [1 byte op=0x01][8 bytes replica][8 bytes count][16 bytes dot]
func (c *pnCounterState) Increment(replica ReplicaID, amount uint64, dot Dot) []byte {
	e := c.positive[replica]
	e.count += amount
	e.dot = dot
	c.positive[replica] = e
	return encodePNDelta(pnInc, replica, e.count, dot)
}

// Decrement adds amount to the negative side and returns the encoded delta.
//
// Delta format: [1 byte op=0x02][8 bytes replica][8 bytes count][16 bytes dot]
func (c *pnCounterState) Decrement(replica ReplicaID, amount uint64, dot Dot) []byte {
	e := c.negative[replica]
	e.count += amount
	e.dot = dot
	c.negative[replica] = e
	return encodePNDelta(pnDec, replica, e.count, dot)
}

// --- Reads ---

// SetPositive sets the positive count for a replica.
func (c *pnCounterState) SetPositive(replica ReplicaID, count uint64) {
	e := c.positive[replica]
	e.count = count
	c.positive[replica] = e
}

// SetNegative sets the negative count for a replica.
func (c *pnCounterState) SetNegative(replica ReplicaID, count uint64) {
	e := c.negative[replica]
	e.count = count
	c.negative[replica] = e
}

// GetPositive returns the positive count for a replica.
func (c *pnCounterState) GetPositive(replica ReplicaID) uint64 {
	return c.positive[replica].count
}

// GetNegative returns the negative count for a replica.
func (c *pnCounterState) GetNegative(replica ReplicaID) uint64 {
	return c.negative[replica].count
}

// Int64 returns the counter value: sum(positive) - sum(negative).
func (c *pnCounterState) Int64() int64 {
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
func (c *pnCounterState) RangePositive(fn func(replica ReplicaID, count uint64) bool) {
	for r, e := range c.positive {
		if !fn(r, e.count) {
			return
		}
	}
}

// RangeNegative calls fn for each replica's negative count.
func (c *pnCounterState) RangeNegative(fn func(replica ReplicaID, count uint64) bool) {
	for r, e := range c.negative {
		if !fn(r, e.count) {
			return
		}
	}
}

// --- Queryable ---

// EntryMeta returns false — PNCounter uses AlwaysMergeClock so this is
// never called.
func (c *pnCounterState) EntryMeta(string) ([]byte, bool) {
	return nil, false
}

// TombstoneMeta always returns false — PNCounter has no tombstones.
func (c *pnCounterState) TombstoneMeta(string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [deltaInfo] from a PNCounter delta.
func (c *pnCounterState) ParseDelta(delta []byte) (deltaInfo, error) {
	if len(delta) < 33 {
		return deltaInfo{}, errShortBuffer
	}
	op := delta[0]
	dot, err := decodeDot(delta[17:33])
	if err != nil {
		return deltaInfo{}, err
	}
	return deltaInfo{
		Op:   op,
		Dots: []Dot{dot},
	}, nil
}

// Apply merges a remote delta using max-wins per side. If the remote count
// exceeds the local count for the same replica and side, the local count is
// updated.
func (c *pnCounterState) Apply(delta []byte) error {
	if len(delta) < 33 {
		return errShortBuffer
	}
	op := delta[0]
	replica := binary.BigEndian.Uint64(delta[1:9])
	count := binary.BigEndian.Uint64(delta[9:17])
	dot, err := decodeDot(delta[17:33])
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
		return errUnknownOp
	}
	return nil
}

// DeltasSince returns deltas for entries with dots not covered by peerHWM.
func (c *pnCounterState) DeltasSince(peerHWM VClock) [][]byte {
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
	copy(b[17:33], encodeDot(dot))
	return b
}
