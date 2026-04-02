package crdt

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrQuorumTimeout is returned by [DurableReplica.Propagate] when the
// write concern quorum is not reached before the context expires. The
// write is still applied locally and will propagate via anti-entropy.
var ErrQuorumTimeout = errors.New("crdt: quorum not reached within timeout")

// ErrClosed is returned by [DurableReplica.Propagate] when the
// DurableReplica has been closed.
var ErrClosed = errors.New("crdt: durable replica closed")

// DurableReplica wraps a [Replica] with write-concern-aware propagation.
// The caller performs mutations on the embedded Replica as usual, then
// calls [Propagate] to broadcast the resulting delta and optionally wait
// for quorum acknowledgements.
//
// The embedded *Replica is NOT safe for concurrent use — the caller must
// serialise mutations and reads as before. Propagate itself is safe to
// call concurrently (the ack tracking is synchronised internally).
type DurableReplica[M Mergeable] struct {
	*Replica[M]

	topology  TopologyProvider
	transport AckTransport
	concern   WriteConcern
	timeout   time.Duration

	mu      sync.Mutex
	pending map[Dot]*pendingWrite
	closed  bool
}

// pendingWrite tracks acknowledgements for a single in-flight write.
type pendingWrite struct {
	needed int                    // peer acks required for quorum
	acked  map[ReplicaID]struct{} // peers that have acked so far
	done   chan struct{}           // closed when quorum reached
}

// NewDurableReplica creates a write-concern-aware wrapper around an
// existing Replica. The topology provider supplies the current peer set
// and the transport handles delta delivery and ack reception.
func NewDurableReplica[M Mergeable](
	inner *Replica[M],
	topology TopologyProvider,
	transport AckTransport,
	opts ...DurableOption,
) *DurableReplica[M] {
	o := applyDurableOptions(opts)
	dr := &DurableReplica[M]{
		Replica:   inner,
		topology:  topology,
		transport: transport,
		concern:   o.concern,
		timeout:   o.timeout,
		pending:   make(map[Dot]*pendingWrite),
	}
	transport.OnAck(dr.handleAck)
	return dr
}

// Propagate broadcasts a delta to all peers in the current topology and
// blocks until the configured write concern is satisfied or the context
// expires.
//
// For [WLocal], the delta is sent to all peers but Propagate returns
// immediately. For [WMajority] and [WAll], Propagate blocks until the
// required number of peers acknowledge receipt.
//
// If the context expires before quorum is reached, Propagate returns
// [ErrQuorumTimeout]. The write remains applied locally and will
// propagate via anti-entropy.
//
// The dot must be the dot returned by NextDot() for this mutation.
func (dr *DurableReplica[M]) Propagate(ctx context.Context, dot Dot, delta []byte) error {
	peers := dr.topology.Peers() // snapshot membership

	if dr.concern == WLocal || len(peers) == 0 {
		// Fire-and-forget: send to all peers, return immediately.
		for _, peer := range peers {
			_ = dr.transport.Send(ctx, peer, dot, delta)
		}
		return nil
	}

	needed := dr.quorumSize(len(peers))
	if needed <= 0 {
		for _, peer := range peers {
			_ = dr.transport.Send(ctx, peer, dot, delta)
		}
		return nil
	}

	pw := &pendingWrite{
		needed: needed,
		acked:  make(map[ReplicaID]struct{}),
		done:   make(chan struct{}),
	}

	// Register the pending write BEFORE sending so that acks arriving
	// synchronously within Send (or very fast transports) are not lost.
	dr.mu.Lock()
	if dr.closed {
		dr.mu.Unlock()
		return ErrClosed
	}
	dr.pending[dot] = pw
	dr.mu.Unlock()

	// Send to all peers. Errors are best-effort; anti-entropy covers
	// delivery failures.
	for _, peer := range peers {
		_ = dr.transport.Send(ctx, peer, dot, delta)
	}

	// Apply default timeout if the context has no deadline.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dr.timeout)
		defer cancel()
	}

	select {
	case <-pw.done:
		dr.mu.Lock()
		if dr.closed {
			delete(dr.pending, dot)
			dr.mu.Unlock()
			return ErrClosed
		}
		delete(dr.pending, dot)
		dr.mu.Unlock()
		return nil
	case <-ctx.Done():
		dr.mu.Lock()
		delete(dr.pending, dot)
		dr.mu.Unlock()
		return ErrQuorumTimeout
	}
}

// handleAck is registered with the transport via OnAck. It resolves
// pending writes as peers acknowledge receipt.
func (dr *DurableReplica[M]) handleAck(peer ReplicaID, dot Dot) {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	pw, ok := dr.pending[dot]
	if !ok {
		return // stale or unknown ack
	}

	pw.acked[peer] = struct{}{}
	if len(pw.acked) >= pw.needed {
		select {
		case <-pw.done:
			// already closed
		default:
			close(pw.done)
		}
	}
}

// quorumSize returns how many peer acks are needed (not counting local).
// The local node always counts as having acked.
func (dr *DurableReplica[M]) quorumSize(peerCount int) int {
	total := peerCount + 1 // peers + local
	switch dr.concern {
	case WMajority:
		majority := total/2 + 1
		fromPeers := majority - 1
		if fromPeers > peerCount {
			fromPeers = peerCount
		}
		return fromPeers
	case WAll:
		return peerCount
	default:
		return 0
	}
}

// Close cleans up all pending writes. Any blocked Propagate calls will
// observe their done channel closing. Callers should cancel their
// contexts or use Close during graceful shutdown.
func (dr *DurableReplica[M]) Close() {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	dr.closed = true
	for dot, pw := range dr.pending {
		select {
		case <-pw.done:
		default:
			close(pw.done)
		}
		delete(dr.pending, dot)
	}
}
