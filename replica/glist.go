package replica

import "github.com/3clabs/crdt"

// GListReplica wraps a [crdt.GList] with clock. GList is append-only so
// merge is simple: add if not already present (dedup by dot).
type GListReplica[V any] struct {
	Data     *crdt.GList[V]
	Codec    crdt.Codec[V]
	Clock    *crdt.LocalClock
	Received *crdt.ReceivedClock
}

// NewGList creates a GListReplica.
func NewGList[V any](replicaID crdt.ReplicaID, codec crdt.Codec[V], opts ...crdt.Option) *GListReplica[V] {
	return &GListReplica[V]{
		Data:     crdt.NewGList(codec, opts...),
		Codec:    codec,
		Clock:    crdt.NewLocalClock(replicaID),
		Received: crdt.NewReceivedClock(),
	}
}

// Append adds a value, stamps a dot, returns the encoded delta.
// Delta format: [varint val len][val bytes][16 byte dot]
func (r *GListReplica[V]) Append(value V) ([]byte, error) {
	dot := r.Clock.Next()
	r.Received.Record(dot.Replica, dot.Counter)

	valBytes, err := r.Codec.Encode(value)
	if err != nil {
		return nil, err
	}
	r.Data.AppendBytes(valBytes, dot)

	var buf []byte
	buf = appendVarintBytes(buf, valBytes)
	buf = append(buf, crdt.EncodeDot(dot)...)
	return buf, nil
}

// ApplyDelta applies an incoming delta. Adds the item if not already present.
func (r *GListReplica[V]) ApplyDelta(delta []byte) error {
	valBytes, off, err := readVarintBytes(delta, 0)
	if err != nil {
		return err
	}
	if off+16 > len(delta) {
		return crdt.ErrShortBuffer
	}
	remoteDot, _ := crdt.DecodeDot(delta[off:])

	// Dedup — only add if we don't have this dot.
	if !r.Data.Has(remoteDot) {
		r.Data.AppendBytes(valBytes, remoteDot)
	}
	r.Received.Record(remoteDot.Replica, remoteDot.Counter)
	return nil
}

// DeltasSince returns deltas for items with dots not covered by peerHWM.
func (r *GListReplica[V]) DeltasSince(peerHWM crdt.VClock) [][]byte {
	var deltas [][]byte
	r.Data.Range(func(valBytes []byte, dot crdt.Dot) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			var buf []byte
			buf = appendVarintBytes(buf, valBytes)
			buf = append(buf, crdt.EncodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})
	return deltas
}
