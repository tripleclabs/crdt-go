package replica

import "github.com/3clabs/crdt"

// AWLWWMapReplica wraps an [crdt.AWLWWMap] with clock and add-wins LWW merge.
// The add-wins bias means a concurrent put beats a concurrent remove.
type AWLWWMapReplica[V any] struct {
	Data     *crdt.AWLWWMap[V]
	Codec    crdt.Codec[V]
	Clock    *crdt.LocalClock
	Received *crdt.ReceivedClock
}

// NewAWLWWMap creates an AWLWWMapReplica.
func NewAWLWWMap[V any](replicaID crdt.ReplicaID, codec crdt.Codec[V], opts ...crdt.Option) *AWLWWMapReplica[V] {
	return &AWLWWMapReplica[V]{
		Data:     crdt.NewAWLWWMap(codec, opts...),
		Codec:    codec,
		Clock:    crdt.NewLocalClock(replicaID),
		Received: crdt.NewReceivedClock(),
	}
}

// Put stores a key-value pair, stamps a dot, returns the encoded delta.
// Delta format: [op=0x01][varint key len][key][varint val len][val][16 byte dot]
func (r *AWLWWMapReplica[V]) Put(key string, value V) ([]byte, error) {
	dot := r.Clock.Next()
	r.Received.Record(dot.Replica, dot.Counter)

	if err := r.Data.Put(key, value, dot); err != nil {
		return nil, err
	}

	valBytes, _, _ := r.Data.GetBytes(key)
	buf := []byte{OpPut}
	buf = appendVarintBytes(buf, []byte(key))
	buf = appendVarintBytes(buf, valBytes)
	buf = append(buf, crdt.EncodeDot(dot)...)
	return buf, nil
}

// Remove tombstones a key with causal context. Returns the encoded delta.
// Delta format: [op=0x02][varint key len][key][16 byte dot][encoded vclock context]
func (r *AWLWWMapReplica[V]) Remove(key string) []byte {
	dot := r.Clock.Next()
	r.Received.Record(dot.Replica, dot.Counter)
	context := r.Received.HWM()
	r.Data.Remove(key, dot, context)

	buf := []byte{OpRemove}
	buf = appendVarintBytes(buf, []byte(key))
	buf = append(buf, crdt.EncodeDot(dot)...)
	buf = append(buf, crdt.EncodeVClock(context)...)
	return buf
}

// ApplyDelta applies an incoming delta.
func (r *AWLWWMapReplica[V]) ApplyDelta(delta []byte) error {
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

func (r *AWLWWMapReplica[V]) applyPut(data []byte) error {
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

	// Compare against existing entry — higher dot wins.
	if _, localDot, ok := r.Data.GetBytes(key); ok {
		if !crdt.DotGT(remoteDot, localDot) {
			r.Received.Record(remoteDot.Replica, remoteDot.Counter)
			return nil
		}
	}

	// Compare against existing tombstone — add-wins: if the entry's dot
	// is NOT covered by the tombstone's context, the entry wins.
	if _, tombCtx, ok := r.Data.GetTombstone(key); ok {
		if tombCtx.Get(remoteDot.Replica) >= remoteDot.Counter {
			// Tombstone's context covers this dot — tombstone wins.
			r.Received.Record(remoteDot.Replica, remoteDot.Counter)
			return nil
		}
	}

	r.Data.PutBytes(key, valBytes, remoteDot)
	r.Received.Record(remoteDot.Replica, remoteDot.Counter)
	return nil
}

func (r *AWLWWMapReplica[V]) applyRemove(data []byte) error {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	if off+16 > len(data) {
		return crdt.ErrShortBuffer
	}
	remoteDot, _ := crdt.DecodeDot(data[off:])
	off += 16
	remoteCtx, err := crdt.DecodeVClock(data[off:])
	if err != nil {
		return err
	}
	key := string(keyBytes)

	// Compare against existing entry — add-wins: entry survives if
	// its dot is NOT covered by the tombstone's context.
	if _, entryDot, ok := r.Data.GetBytes(key); ok {
		if remoteCtx.Get(entryDot.Replica) < entryDot.Counter {
			// Entry's dot is not covered — entry wins (add-wins).
			r.Received.Record(remoteDot.Replica, remoteDot.Counter)
			return nil
		}
	}

	// Compare against existing tombstone — higher dot wins.
	if existingDot, _, ok := r.Data.GetTombstone(key); ok {
		if !crdt.DotGT(remoteDot, existingDot) {
			r.Received.Record(remoteDot.Replica, remoteDot.Counter)
			return nil
		}
	}

	r.Data.Remove(key, remoteDot, remoteCtx)
	r.Received.Record(remoteDot.Replica, remoteDot.Counter)
	return nil
}

// DeltasSince returns deltas for entries and tombstones not covered by peerHWM.
func (r *AWLWWMapReplica[V]) DeltasSince(peerHWM crdt.VClock) [][]byte {
	var deltas [][]byte

	r.Data.Range(func(key string, _ V, dot crdt.Dot) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			valBytes, _, _ := r.Data.GetBytes(key)
			buf := []byte{OpPut}
			buf = appendVarintBytes(buf, []byte(key))
			buf = appendVarintBytes(buf, valBytes)
			buf = append(buf, crdt.EncodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	r.Data.RangeTombstones(func(key string, dot crdt.Dot, ctx crdt.VClock) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			buf := []byte{OpRemove}
			buf = appendVarintBytes(buf, []byte(key))
			buf = append(buf, crdt.EncodeDot(dot)...)
			buf = append(buf, crdt.EncodeVClock(ctx)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	return deltas
}
