package crdt

import (
	"context"
	"encoding/binary"
)

// WriteConcern controls how many peers must acknowledge a delta before
// [durableReplica.Propagate] returns successfully.
type WriteConcern int

const (
	// WLocal returns immediately after local apply. Deltas are still sent
	// to peers, but Propagate does not wait for acknowledgements.
	WLocal WriteConcern = iota

	// WMajority waits for acks from a strict majority of the cluster
	// (⌊n/2⌋+1 where n includes the local node, which always counts as
	// having acked).
	WMajority

	// WAll waits for acks from every peer in the topology snapshot taken
	// at the time of the Propagate call.
	WAll
)

// TopologyProvider supplies the current set of peer replica IDs. The
// returned slice must NOT include the local replica. Implementations
// must be safe for concurrent calls.
type TopologyProvider interface {
	Peers() []ReplicaID
}

// TransportMessage is the opaque unit moved between peers. The transport
// should not inspect its contents — just deliver it to the receiving
// replica's OnReceive handler.
type TransportMessage struct {
	from  ReplicaID
	dot   Dot
	delta []byte
	ack   bool
}

// From returns the sender's replica ID.
func (m TransportMessage) From() ReplicaID { return m.from }

// Marshal encodes the message into bytes for transmission over the wire.
// The sender's ID is not included — the transport knows who sent it.
func (m TransportMessage) Marshal() []byte {
	// Format: [1 byte flags][16 byte dot][delta bytes]
	// flags bit 0: ack requested
	flags := byte(0)
	if m.ack {
		flags |= 1
	}
	buf := make([]byte, 1+16+len(m.delta))
	buf[0] = flags
	binary.BigEndian.PutUint64(buf[1:9], m.dot.Replica)
	binary.BigEndian.PutUint64(buf[9:17], m.dot.Counter)
	copy(buf[17:], m.delta)
	return buf
}

// UnmarshalTransportMessage decodes a message from wire bytes. The
// transport provides the sender's replica ID from its network layer.
func UnmarshalTransportMessage(from ReplicaID, data []byte) (TransportMessage, bool) {
	if len(data) < 17 {
		return TransportMessage{}, false
	}
	flags := data[0]
	return TransportMessage{
		from:  from,
		dot:   Dot{Replica: binary.BigEndian.Uint64(data[1:9]), Counter: binary.BigEndian.Uint64(data[9:17])},
		delta: data[17:],
		ack:   flags&1 != 0,
	}, true
}

// Transport moves messages between peers. The replica registers a receive
// handler via OnReceive at construction time; the transport calls it
// when a message arrives from a remote peer.
type Transport interface {
	// Send transmits a message to a specific peer. When the message
	// requests an ack, the transport returns a channel that closes when
	// the peer acknowledges receipt. Otherwise the returned channel is
	// nil. Send should not block on ack arrival.
	Send(ctx context.Context, peer ReplicaID, msg TransportMessage) (<-chan struct{}, error)

	// OnReceive registers a handler that the transport must call when a
	// message arrives from a remote peer. Called once at setup time.
	OnReceive(fn func(msg TransportMessage))
}

// WithWriteConcern sets the write concern level. Default is [WLocal].
func WithWriteConcern(wc WriteConcern) Option {
	return func(o *options) { o.concern = wc }
}
