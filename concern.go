package crdt

import "context"

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

// From returns the sender's replica ID. Set by the transport on receive.
func (m TransportMessage) From() ReplicaID { return m.from }

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
