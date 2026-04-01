package replica

import (
	"encoding/binary"

	"github.com/3clabs/crdt"
)

// PNCounterReplica wraps a [crdt.PNCounter] with clock and max-wins merge.
type PNCounterReplica struct {
	Data     *crdt.PNCounter
	Clock    *crdt.LocalClock
	Received *crdt.ReceivedClock
}

// PNCounter op codes within the delta.
const (
	pnInc byte = 0x01
	pnDec byte = 0x02
)

// NewPNCounter creates a PNCounterReplica.
func NewPNCounter(replicaID crdt.ReplicaID) *PNCounterReplica {
	return &PNCounterReplica{
		Data:     crdt.NewPNCounter(),
		Clock:    crdt.NewLocalClock(replicaID),
		Received: crdt.NewReceivedClock(),
	}
}

// Increment adds amount and returns the encoded delta.
// Delta format: [1 byte op=0x01][8 bytes replica][8 bytes count]
func (r *PNCounterReplica) Increment(amount uint64) []byte {
	rid := r.Clock.Replica()
	newCount := r.Data.GetPositive(rid) + amount
	r.Data.SetPositive(rid, newCount)
	// Use combined pos+neg as the clock counter.
	r.Clock.SetCounter(r.Data.GetPositive(rid) + r.Data.GetNegative(rid))
	r.Received.Record(rid, r.Clock.Counter())
	return encodePNDelta(pnInc, rid, newCount)
}

// Decrement adds amount to the negative side and returns the encoded delta.
// Delta format: [1 byte op=0x02][8 bytes replica][8 bytes count]
func (r *PNCounterReplica) Decrement(amount uint64) []byte {
	rid := r.Clock.Replica()
	newCount := r.Data.GetNegative(rid) + amount
	r.Data.SetNegative(rid, newCount)
	r.Clock.SetCounter(r.Data.GetPositive(rid) + r.Data.GetNegative(rid))
	r.Received.Record(rid, r.Clock.Counter())
	return encodePNDelta(pnDec, rid, newCount)
}

// ApplyDelta applies an incoming delta. Max-wins per replica per side.
func (r *PNCounterReplica) ApplyDelta(delta []byte) error {
	if len(delta) < 17 {
		return crdt.ErrShortBuffer
	}
	op := delta[0]
	replica := binary.BigEndian.Uint64(delta[1:9])
	count := binary.BigEndian.Uint64(delta[9:17])

	switch op {
	case pnInc:
		if count > r.Data.GetPositive(replica) {
			r.Data.SetPositive(replica, count)
		}
	case pnDec:
		if count > r.Data.GetNegative(replica) {
			r.Data.SetNegative(replica, count)
		}
	default:
		return crdt.ErrUnknownOp
	}
	r.Received.Record(replica, count)
	return nil
}

// DeltasSince returns deltas for replicas with counts above peerHWM.
func (r *PNCounterReplica) DeltasSince(peerHWM crdt.VClock) [][]byte {
	var deltas [][]byte
	r.Data.RangePositive(func(replica crdt.ReplicaID, count uint64) bool {
		if count > peerHWM.Get(replica) {
			deltas = append(deltas, encodePNDelta(pnInc, replica, count))
		}
		return true
	})
	r.Data.RangeNegative(func(replica crdt.ReplicaID, count uint64) bool {
		if count > peerHWM.Get(replica) {
			deltas = append(deltas, encodePNDelta(pnDec, replica, count))
		}
		return true
	})
	return deltas
}

func encodePNDelta(op byte, replica crdt.ReplicaID, count uint64) []byte {
	b := make([]byte, 17)
	b[0] = op
	binary.BigEndian.PutUint64(b[1:9], replica)
	binary.BigEndian.PutUint64(b[9:17], count)
	return b
}
