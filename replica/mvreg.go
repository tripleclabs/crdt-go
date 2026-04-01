package replica

import "github.com/3clabs/crdt"

// MVRegisterReplica wraps a [crdt.MVRegister] with clock and multi-value
// merge logic. Concurrent values survive; a write after merge resolves.
type MVRegisterReplica[V any] struct {
	Data     *crdt.MVRegister[V]
	Codec    crdt.Codec[V]
	Clock    *crdt.LocalClock
	Received *crdt.ReceivedClock
}

// NewMVRegister creates an MVRegisterReplica.
func NewMVRegister[V any](replicaID crdt.ReplicaID, codec crdt.Codec[V]) *MVRegisterReplica[V] {
	return &MVRegisterReplica[V]{
		Data:     crdt.NewMVRegister(codec),
		Codec:    codec,
		Clock:    crdt.NewLocalClock(replicaID),
		Received: crdt.NewReceivedClock(),
	}
}

// Write sets a value (clearing all concurrent values), stamps a dot,
// returns the encoded delta. The delta includes the writer's received
// clock so the receiver can prune superseded values.
//
// Delta format: [varint val len][val bytes][16 byte dot][encoded vclock context]
func (r *MVRegisterReplica[V]) Write(value V) ([]byte, error) {
	dot := r.Clock.Next()
	r.Received.Record(dot.Replica, dot.Counter)

	if err := r.Data.Set(value, dot); err != nil {
		return nil, err
	}

	valBytes, _ := r.Codec.Encode(value)
	var buf []byte
	buf = appendVarintBytes(buf, valBytes)
	buf = append(buf, crdt.EncodeDot(dot)...)
	buf = append(buf, crdt.EncodeVClock(r.Received.HWM())...)
	return buf, nil
}

// ApplyDelta applies an incoming delta. Local values whose dots are
// covered by the remote's context are pruned. The remote value is added
// if its dot is not covered by local knowledge.
func (r *MVRegisterReplica[V]) ApplyDelta(delta []byte) error {
	valBytes, off, err := readVarintBytes(delta, 0)
	if err != nil {
		return err
	}
	if off+16 > len(delta) {
		return crdt.ErrShortBuffer
	}
	remoteDot, _ := crdt.DecodeDot(delta[off:])
	off += 16

	// Decode the writer's context (received HWM at time of write).
	remoteCtx, err := crdt.DecodeVClock(delta[off:])
	if err != nil {
		return err
	}

	type entry struct {
		ValBytes []byte
		Dot      crdt.Dot
	}
	var surviving []entry

	// Keep local entries whose dots are NOT covered by the remote context.
	r.Data.RangeEntries(func(localVal []byte, localDot crdt.Dot) bool {
		if remoteCtx.Get(localDot.Replica) >= localDot.Counter {
			return true // covered by remote context — prune
		}
		surviving = append(surviving, entry{localVal, localDot})
		return true
	})

	// Add remote value if not already superseded by a local entry
	// from the same replica with equal or higher counter.
	addRemote := true
	for _, e := range surviving {
		if e.Dot.Replica == remoteDot.Replica && e.Dot.Counter >= remoteDot.Counter {
			addRemote = false
			break
		}
	}
	if addRemote {
		surviving = append(surviving, entry{valBytes, remoteDot})
	}

	entries := make([]struct {
		ValBytes []byte
		Dot      crdt.Dot
	}, len(surviving))
	for i, e := range surviving {
		entries[i].ValBytes = e.ValBytes
		entries[i].Dot = e.Dot
	}
	r.Data.SetEntries(entries)
	r.Received.Record(remoteDot.Replica, remoteDot.Counter)
	return nil
}

// DeltasSince returns deltas for entries with dots not covered by peerHWM.
// Each delta includes the received HWM as context so the receiver can
// prune superseded values.
func (r *MVRegisterReplica[V]) DeltasSince(peerHWM crdt.VClock) [][]byte {
	var deltas [][]byte
	ctx := r.Received.HWM()
	r.Data.RangeEntries(func(valBytes []byte, dot crdt.Dot) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			var buf []byte
			buf = appendVarintBytes(buf, valBytes)
			buf = append(buf, crdt.EncodeDot(dot)...)
			buf = append(buf, crdt.EncodeVClock(ctx)...)
			deltas = append(deltas, buf)
		}
		return true
	})
	return deltas
}
