package replica

import "github.com/3clabs/crdt"

// LWWRegisterReplica wraps a [crdt.LWWRegister] with clock and LWW merge.
type LWWRegisterReplica[V any] struct {
	Data     *crdt.LWWRegister[V]
	Codec    crdt.Codec[V]
	Clock    *crdt.LocalClock
	Received *crdt.ReceivedClock
}

// NewLWWRegister creates an LWWRegisterReplica.
func NewLWWRegister[V any](replicaID crdt.ReplicaID, codec crdt.Codec[V]) *LWWRegisterReplica[V] {
	return &LWWRegisterReplica[V]{
		Data:     crdt.NewLWWRegister(codec),
		Codec:    codec,
		Clock:    crdt.NewLocalClock(replicaID),
		Received: crdt.NewReceivedClock(),
	}
}

// Set writes a value, stamps a dot, returns the encoded delta.
// Delta format: [varint val len][val bytes][16 byte dot]
func (r *LWWRegisterReplica[V]) Set(value V) ([]byte, error) {
	dot := r.Clock.Next()
	r.Received.Record(dot.Replica, dot.Counter)

	if err := r.Data.Set(value, dot); err != nil {
		return nil, err
	}

	valBytes, _, _ := r.Data.GetBytes()
	var buf []byte
	buf = appendVarintBytes(buf, valBytes)
	buf = append(buf, crdt.EncodeDot(dot)...)
	return buf, nil
}

// ApplyDelta applies an incoming delta. Higher dot wins.
func (r *LWWRegisterReplica[V]) ApplyDelta(delta []byte) error {
	valBytes, off, err := readVarintBytes(delta, 0)
	if err != nil {
		return err
	}
	if off+16 > len(delta) {
		return crdt.ErrShortBuffer
	}
	remoteDot, _ := crdt.DecodeDot(delta[off:])

	_, localDot, hasLocal := r.Data.GetBytes()
	if !hasLocal || crdt.DotGT(remoteDot, localDot) {
		r.Data.SetBytes(valBytes, remoteDot)
	}
	r.Received.Record(remoteDot.Replica, remoteDot.Counter)
	return nil
}

// DeltasSince returns the register as a delta if the peer hasn't seen it.
func (r *LWWRegisterReplica[V]) DeltasSince(peerHWM crdt.VClock) [][]byte {
	valBytes, dot, ok := r.Data.GetBytes()
	if !ok {
		return nil
	}
	if dot.Counter <= peerHWM.Get(dot.Replica) {
		return nil
	}
	var buf []byte
	buf = appendVarintBytes(buf, valBytes)
	buf = append(buf, crdt.EncodeDot(dot)...)
	return [][]byte{buf}
}
