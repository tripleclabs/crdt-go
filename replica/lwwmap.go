package replica

import "github.com/3clabs/crdt"

// LWWMapReplica wraps an [crdt.LWWMap] with clocks and LWW merge logic.
// It stamps dots on mutations, compares dots on incoming deltas, and
// provides anti-entropy via received clock diffing.
type LWWMapReplica[V any] struct {
	Data     *crdt.LWWMap[V]
	Clock    *crdt.LocalClock
	Received *crdt.ReceivedClock
}

// NewLWWMap creates an LWWMapReplica wrapping a new LWWMap.
func NewLWWMap[V any](replicaID crdt.ReplicaID, codec crdt.Codec[V], opts ...crdt.Option) *LWWMapReplica[V] {
	return &LWWMapReplica[V]{
		Data:     crdt.NewLWWMap(codec, opts...),
		Clock:    crdt.NewLocalClock(replicaID),
		Received: crdt.NewReceivedClock(),
	}
}

// Put stores a key-value pair, stamps a dot, and returns the encoded delta
// to send to peers.
//
// Delta format: [op=0x01][varint key len][key][varint val len][val][16 byte dot]
func (r *LWWMapReplica[V]) Put(key string, value V) ([]byte, error) {
	dot := r.Clock.Next()
	r.Received.Record(dot.Replica, dot.Counter)

	if err := r.Data.Put(key, value, dot); err != nil {
		return nil, err
	}

	// Encode delta from the stored bytes.
	valBytes, _, _ := r.Data.GetBytes(key)
	buf := []byte{OpPut}
	buf = appendVarintBytes(buf, []byte(key))
	buf = appendVarintBytes(buf, valBytes)
	buf = append(buf, crdt.EncodeDot(dot)...)
	return buf, nil
}

// Remove tombstones a key, stamps a dot, and returns the encoded delta
// to send to peers.
//
// Delta format: [op=0x02][varint key len][key][16 byte dot]
func (r *LWWMapReplica[V]) Remove(key string) []byte {
	dot := r.Clock.Next()
	r.Received.Record(dot.Replica, dot.Counter)
	r.Data.Remove(key, dot)

	buf := []byte{OpRemove}
	buf = appendVarintBytes(buf, []byte(key))
	buf = append(buf, crdt.EncodeDot(dot)...)
	return buf
}

// ApplyDelta applies an incoming delta from a remote peer. Compares dots
// and writes only if the incoming data wins.
func (r *LWWMapReplica[V]) ApplyDelta(delta []byte) error {
	if len(delta) < 1 {
		return crdt.ErrShortBuffer
	}
	switch delta[0] {
	case OpPut:
		return r.applyPut(delta[1:])
	case OpRemove:
		return r.applyRemove(delta[1:])
	default:
		return crdt.ErrUnknownOp
	}
}

func (r *LWWMapReplica[V]) applyPut(data []byte) error {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	valBytes, off, err := readVarintBytes(data, off)
	if err != nil {
		return err
	}
	if off+16 > len(data) {
		return crdt.ErrShortBuffer
	}
	remoteDot, _ := crdt.DecodeDot(data[off:])
	key := string(keyBytes)

	// Compare against existing entry.
	if _, localDot, ok := r.Data.GetBytes(key); ok {
		if !crdt.DotGT(remoteDot, localDot) {
			r.Received.Record(remoteDot.Replica, remoteDot.Counter)
			return nil // local wins
		}
	}
	// Compare against existing tombstone.
	if localTombDot, ok := r.Data.GetTombstone(key); ok {
		if !crdt.DotGT(remoteDot, localTombDot) {
			r.Received.Record(remoteDot.Replica, remoteDot.Counter)
			return nil // tombstone wins
		}
	}

	r.Data.PutBytes(key, valBytes, remoteDot)
	r.Received.Record(remoteDot.Replica, remoteDot.Counter)
	return nil
}

func (r *LWWMapReplica[V]) applyRemove(data []byte) error {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	if off+16 > len(data) {
		return crdt.ErrShortBuffer
	}
	remoteDot, _ := crdt.DecodeDot(data[off:])
	key := string(keyBytes)

	// Compare against existing entry.
	if _, localDot, ok := r.Data.GetBytes(key); ok {
		if !crdt.DotGT(remoteDot, localDot) {
			r.Received.Record(remoteDot.Replica, remoteDot.Counter)
			return nil // entry wins
		}
	}
	// Compare against existing tombstone.
	if localTombDot, ok := r.Data.GetTombstone(key); ok {
		if !crdt.DotGT(remoteDot, localTombDot) {
			r.Received.Record(remoteDot.Replica, remoteDot.Counter)
			return nil // existing tombstone wins
		}
	}

	r.Data.Remove(key, remoteDot)
	r.Received.Record(remoteDot.Replica, remoteDot.Counter)
	return nil
}

// DeltasSince returns encoded deltas for entries and tombstones with dots
// not covered by peerHWM.
func (r *LWWMapReplica[V]) DeltasSince(peerHWM crdt.VClock) [][]byte {
	var deltas [][]byte

	r.Data.RangeBytes(func(key string, valBytes []byte, dot crdt.Dot) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			buf := []byte{OpPut}
			buf = appendVarintBytes(buf, []byte(key))
			buf = appendVarintBytes(buf, valBytes)
			buf = append(buf, crdt.EncodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	r.Data.RangeTombstones(func(key string, dot crdt.Dot) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			buf := []byte{OpRemove}
			buf = appendVarintBytes(buf, []byte(key))
			buf = append(buf, crdt.EncodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	return deltas
}
