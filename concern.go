package crdt

import (
	"context"
	"time"
)

// WriteConcern controls how many peers must acknowledge a delta before
// [DurableReplica.Propagate] returns successfully.
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

// Transport sends deltas to specific peers. Implement this interface
// for fire-and-forget replication without write concerns.
type Transport interface {
	// Send transmits a delta to a specific peer. The dot identifies the
	// causal event so the receiver can acknowledge it. Implementations
	// should respect ctx cancellation.
	Send(ctx context.Context, peer ReplicaID, dot Dot, delta []byte) error
}

// AckTransport extends [Transport] with acknowledgement delivery,
// enabling write-concern-aware propagation. A single concrete type
// can satisfy both Transport and AckTransport.
type AckTransport interface {
	Transport

	// OnAck registers a callback that the transport must invoke when an
	// ack is received from a peer for a given dot. Called once at setup
	// time by [NewDurableReplica].
	OnAck(fn func(peer ReplicaID, dot Dot))
}

// StaticTopology is a [TopologyProvider] with a fixed set of peers.
type StaticTopology struct {
	peers []ReplicaID
}

// NewStaticTopology returns a [TopologyProvider] with the given peer
// IDs. The slice is copied; later modifications have no effect.
func NewStaticTopology(peers ...ReplicaID) *StaticTopology {
	cp := make([]ReplicaID, len(peers))
	copy(cp, peers)
	return &StaticTopology{peers: cp}
}

// Peers returns a copy of the static peer list.
func (s *StaticTopology) Peers() []ReplicaID {
	out := make([]ReplicaID, len(s.peers))
	copy(out, s.peers)
	return out
}

// DurableOption configures a [DurableReplica] at construction time.
type DurableOption func(*durableOptions)

type durableOptions struct {
	concern WriteConcern
	timeout time.Duration
}

func applyDurableOptions(opts []DurableOption) durableOptions {
	o := durableOptions{
		concern: WLocal,
		timeout: 5 * time.Second,
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// WithWriteConcern sets the write concern level. Default is [WLocal].
func WithWriteConcern(wc WriteConcern) DurableOption {
	return func(o *durableOptions) { o.concern = wc }
}

// WithPropagateTimeout sets the default timeout for [DurableReplica.Propagate]
// when the caller's context has no deadline. Default is 5 seconds.
func WithPropagateTimeout(d time.Duration) DurableOption {
	return func(o *durableOptions) { o.timeout = d }
}
