package crdt

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// ReplicaID uniquely identifies a replica (node or process) in a distributed
// system. Each replica that can independently mutate a CRDT must have its own
// ReplicaID. The type is uint64 for deterministic ordering (used in tie-breaking)
// and compact representation in maps and on the wire.
//
// Consumers are responsible for generating replica IDs — for example by hashing
// a node hostname, drawing from a sequence, or using a random uint64.
type ReplicaID = uint64

// ErrQuorumTimeout is returned by [WriteResult.Wait] when the write concern
// quorum is not reached before the context expires. The write is still applied
// locally and will propagate via anti-entropy.
var ErrQuorumTimeout = errors.New("crdt: quorum not reached within timeout")

// Message type prefixes for transport framing.
const (
	msgDelta    byte = 0x00
	msgAEDigest byte = 0x01
)

// defaultAEInterval is the default anti-entropy interval when a transport is
// configured and no explicit interval is set.
const defaultAEInterval = 1 * time.Second

// replica is the core replication unit. It owns CRDT state, transport,
// topology, write concern, anti-entropy, and concurrency control.
// All access is mutex-protected — safe for concurrent use.
type replica[M mergeable] struct {
	mu       sync.Mutex
	data     M
	strategy clock
	local    *localClock
	received *receivedClock
	merkle   *merkleMap

	transport  Transport
	topology   TopologyProvider
	concern    WriteConcern
	aeInterval time.Duration
	aeStop     chan struct{}
}

// newReplica creates a replica with the given storage and clock strategy.
// If options include a transport, OnReceive is wired and anti-entropy starts.
func newReplica[M mergeable](replicaID ReplicaID, data M, strategy clock, opts ...Option) *replica[M] {
	o := applyOptions(opts)
	aeInterval := o.aeInterval
	if aeInterval == 0 && o.transport != nil {
		aeInterval = defaultAEInterval
	}
	r := &replica[M]{
		data:       data,
		strategy:   strategy,
		local:      newLocalClock(replicaID),
		received:   newReceivedClock(),
		merkle:     newMerkleMap(),
		transport:  o.transport,
		topology:   o.topology,
		concern:    o.concern,
		aeInterval: aeInterval,
	}
	if o.transport != nil {
		o.transport.OnReceive(func(msg TransportMessage) {
			r.handleMessage(msg.from, msg.dot, msg.delta)
		})
		if aeInterval > 0 {
			r.aeStop = make(chan struct{})
			go r.runAntiEntropy()
		}
	}
	return r
}

// Close stops the anti-entropy goroutine. Safe to call multiple times.
func (r *replica[M]) Close() {
	if r.aeStop != nil {
		select {
		case <-r.aeStop:
			// already closed
		default:
			close(r.aeStop)
		}
	}
}

// handleMessage is registered with the transport via OnReceive. It
// dispatches based on the message type prefix.
func (r *replica[M]) handleMessage(from ReplicaID, dot Dot, msg []byte) {
	if len(msg) == 0 {
		return
	}
	switch msg[0] {
	case msgDelta:
		r.applyDelta(from, dot, msg[1:])
	case msgAEDigest:
		r.handleSyncDigest(from, msg[1:])
	}
}

// nextDot stamps and returns the next dot for local mutations.
// Caller must hold mu.
func (r *replica[M]) nextDot() Dot {
	d := r.local.Next()
	r.received.Record(d.Replica, d.Counter)
	return d
}

// trackKey updates the MerkleMap for a key after a state change.
// Caller must hold mu.
func (r *replica[M]) trackKey(key string) {
	if key == "" {
		key = "\x00" // sentinel for non-keyed types (registers)
	}
	if meta, ok := r.data.EntryMeta(key); ok {
		r.merkle.Put(key, meta)
	} else if meta, ok := r.data.TombstoneMeta(key); ok {
		r.merkle.Put(key, meta)
	} else {
		r.merkle.Delete(key)
	}
}

// applyDelta applies an incoming delta from a remote peer. Acquires mu.
func (r *replica[M]) applyDelta(from ReplicaID, dot Dot, delta []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, err := r.data.ParseDelta(delta)
	if err != nil {
		return err
	}

	if r.strategy.Allows(r.data, info) {
		if err := r.data.Apply(delta); err != nil {
			return err
		}
	}

	for _, d := range info.Dots {
		r.received.Record(d.Replica, d.Counter)
	}

	r.trackKey(info.Key)
	return nil
}

// tagDelta prepends the delta message type prefix.
func tagDelta(delta []byte) []byte {
	tagged := make([]byte, 1+len(delta))
	tagged[0] = msgDelta
	copy(tagged[1:], delta)
	return tagged
}

// propagate sends a tagged delta to all peers in parallel and returns a
// WriteResult for optional quorum waiting. Does NOT hold mu.
func (r *replica[M]) propagate(ctx context.Context, dot Dot, delta []byte) *WriteResult {
	if r.transport == nil || r.topology == nil {
		return &WriteResult{}
	}
	peers := r.topology.Peers()
	if len(peers) == 0 {
		return &WriteResult{}
	}
	tagged := tagDelta(delta)
	ack := r.concern > WLocal
	chs := make([]<-chan struct{}, len(peers))
	var wg sync.WaitGroup
	for i, peer := range peers {
		wg.Add(1)
		go func(i int, peer ReplicaID) {
			defer wg.Done()
			ch, _ := r.transport.Send(ctx, peer, TransportMessage{dot: dot, delta: tagged, ack: ack})
			chs[i] = ch
		}(i, peer)
	}
	wg.Wait()
	var acks []<-chan struct{}
	for _, ch := range chs {
		if ch != nil {
			acks = append(acks, ch)
		}
	}
	return &WriteResult{acks: acks, needed: r.quorumSize(len(peers))}
}

// broadcast sends a tagged delta to all peers fire-and-forget (no acks).
// Used for context-only removes (ORSet, ORMap). Does NOT hold mu.
func (r *replica[M]) broadcast(ctx context.Context, dot Dot, delta []byte) {
	if r.transport == nil || r.topology == nil {
		return
	}
	tagged := tagDelta(delta)
	peers := r.topology.Peers()
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer ReplicaID) {
			defer wg.Done()
			r.transport.Send(ctx, peer, TransportMessage{dot: dot, delta: tagged})
		}(peer)
	}
	wg.Wait()
}

// --- Anti-entropy ---

// runAntiEntropy periodically sends state digests to all peers.
func (r *replica[M]) runAntiEntropy() {
	ticker := time.NewTicker(r.aeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.aeStop:
			return
		case <-ticker.C:
			r.sendDigests()
		}
	}
}

// sendDigests sends a sync digest to each peer.
func (r *replica[M]) sendDigests() {
	if r.topology == nil {
		return
	}
	peers := r.topology.Peers()
	if len(peers) == 0 {
		return
	}

	r.mu.Lock()
	hash := r.merkle.Hash()
	hwm := r.received.HWM()
	r.mu.Unlock()

	msg := encodeAEDigest(hash, hwm)
	ctx := context.Background()
	for _, peer := range peers {
		r.transport.Send(ctx, peer, TransportMessage{delta: msg})
	}
}

// handleSyncDigest processes an incoming anti-entropy digest from a peer.
func (r *replica[M]) handleSyncDigest(from ReplicaID, payload []byte) {
	peerHash, peerHWM, err := decodeAEDigest(payload)
	if err != nil {
		return
	}

	r.mu.Lock()
	localHash := r.merkle.Hash()
	if localHash == peerHash {
		r.mu.Unlock()
		return
	}
	deltas := r.data.DeltasSince(peerHWM)
	r.mu.Unlock()

	ctx := context.Background()
	for _, d := range deltas {
		r.transport.Send(ctx, from, TransportMessage{delta: tagDelta(d)})
	}
}

// encodeAEDigest encodes [msgAEDigest][8-byte hash][VClock].
func encodeAEDigest(hash uint64, hwm VClock) []byte {
	vcBytes := encodeVClock(hwm)
	buf := make([]byte, 1+8+len(vcBytes))
	buf[0] = msgAEDigest
	binary.BigEndian.PutUint64(buf[1:9], hash)
	copy(buf[9:], vcBytes)
	return buf
}

// decodeAEDigest decodes the payload after the type prefix.
func decodeAEDigest(payload []byte) (uint64, VClock, error) {
	if len(payload) < 8 {
		return 0, nil, errors.New("crdt: short AE digest")
	}
	hash := binary.BigEndian.Uint64(payload[:8])
	hwm, err := decodeVClock(payload[8:])
	if err != nil {
		return 0, nil, err
	}
	return hash, hwm, nil
}

// --- Quorum ---

// quorumSize returns how many peer acks are needed (not counting local).
func (r *replica[M]) quorumSize(peerCount int) int {
	total := peerCount + 1
	switch r.concern {
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

// WriteResult tracks an in-flight write. Call [Wait] to block until
// the configured write concern is satisfied.
type WriteResult struct {
	acks   []<-chan struct{}
	needed int
}

// Wait blocks until the write concern quorum is reached or ctx expires.
// Returns nil immediately for WLocal or local-only replicas.
func (w *WriteResult) Wait(ctx context.Context) error {
	if w == nil || w.needed == 0 {
		return nil
	}
	got := 0
	for _, ch := range w.acks {
		select {
		case <-ch:
			got++
			if got >= w.needed {
				return nil
			}
		case <-ctx.Done():
			return ErrQuorumTimeout
		}
	}
	return nil
}
