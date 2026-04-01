package replica

import (
	"encoding/binary"

	"github.com/3clabs/crdt"
)

// GCounterReplica wraps a [crdt.GCounter] with clock and max-wins merge.
type GCounterReplica struct {
	Data     *crdt.GCounter
	Clock    *crdt.LocalClock
	Received *crdt.ReceivedClock
}

// NewGCounter creates a GCounterReplica.
func NewGCounter(replicaID crdt.ReplicaID) *GCounterReplica {
	return &GCounterReplica{
		Data:     crdt.NewGCounter(),
		Clock:    crdt.NewLocalClock(replicaID),
		Received: crdt.NewReceivedClock(),
	}
}

// Increment adds amount and returns the encoded delta.
//
// Delta format: [8 bytes replica][8 bytes count]
func (r *GCounterReplica) Increment(amount uint64) []byte {
	rid := r.Clock.Replica()
	newCount := r.Data.Get(rid) + amount
	r.Data.Set(rid, newCount)
	r.Clock.SetCounter(newCount)
	r.Received.Record(rid, newCount)

	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], rid)
	binary.BigEndian.PutUint64(buf[8:16], newCount)
	return buf
}

// ApplyDelta applies an incoming delta. Max-wins per replica.
func (r *GCounterReplica) ApplyDelta(delta []byte) error {
	if len(delta) < 16 {
		return crdt.ErrShortBuffer
	}
	replica := binary.BigEndian.Uint64(delta[0:8])
	count := binary.BigEndian.Uint64(delta[8:16])

	if count > r.Data.Get(replica) {
		r.Data.Set(replica, count)
	}
	r.Received.Record(replica, count)
	return nil
}

// DeltasSince returns deltas for replicas with counts above peerHWM.
func (r *GCounterReplica) DeltasSince(peerHWM crdt.VClock) [][]byte {
	var deltas [][]byte
	r.Data.Range(func(replica crdt.ReplicaID, count uint64) bool {
		if count > peerHWM.Get(replica) {
			buf := make([]byte, 16)
			binary.BigEndian.PutUint64(buf[0:8], replica)
			binary.BigEndian.PutUint64(buf[8:16], count)
			deltas = append(deltas, buf)
		}
		return true
	})
	return deltas
}
