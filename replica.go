package crdt

// ReplicaID uniquely identifies a replica (node or process) in a distributed
// system. Each replica that can independently mutate a CRDT must have its own
// ReplicaID. The type is uint64 for deterministic ordering (used in tie-breaking)
// and compact representation in maps and on the wire.
//
// Consumers are responsible for generating replica IDs — for example by hashing
// a node hostname, drawing from a sequence, or using a random uint64.
type ReplicaID = uint64

// Replica is the generic replication wrapper for any CRDT storage type that
// implements [Mergeable]. It composes three orthogonal concerns:
//
//   - Storage + merge semantics (the M type parameter)
//   - Clock semantics (the [Clock] implementation)
//   - Causal bookkeeping ([LocalClock] + [ReceivedClock])
//
// Users call domain-specific mutation methods on Data (e.g., Data.Put,
// Data.Add) and replication methods on Replica (ApplyDelta, DeltasSince).
type Replica[M Mergeable] struct {
	// Data is the CRDT storage type. Use it for domain operations (Put,
	// Get, Add, Remove, etc.) and for reading state.
	Data M

	// Strategy is the clock/domination rule used to decide whether an
	// incoming delta should be applied.
	Strategy Clock

	// Local stamps dots for local mutations.
	Local *LocalClock

	// Received tracks which remote dots have been processed.
	Received *ReceivedClock
}

// NewReplica creates a Replica with the given storage, clock strategy, and
// replica ID. Prefer the type-specific factory functions (NewLWWMap, NewORSet,
// etc.) which select the correct clock strategy automatically.
func NewReplica[M Mergeable](replicaID ReplicaID, data M, strategy Clock) *Replica[M] {
	return &Replica[M]{
		Data:     data,
		Strategy: strategy,
		Local:    NewLocalClock(replicaID),
		Received: NewReceivedClock(),
	}
}

// ApplyDelta applies an incoming delta from a remote peer. The clock strategy
// decides whether the delta dominates local state; if so, the delta is applied.
// Causal dots are always recorded in the ReceivedClock regardless of the
// clock's decision.
func (r *Replica[M]) ApplyDelta(delta []byte) error {
	info, err := r.Data.ParseDelta(delta)
	if err != nil {
		return err
	}

	if r.Strategy.Allows(r.Data, info) {
		if err := r.Data.Apply(delta); err != nil {
			return err
		}
	}

	for _, d := range info.Dots {
		r.Received.Record(d.Replica, d.Counter)
	}
	return nil
}

// DeltasSince returns encoded deltas for state not covered by peerHWM.
func (r *Replica[M]) DeltasSince(peerHWM VClock) [][]byte {
	return r.Data.DeltasSince(peerHWM)
}

// NextDot stamps and returns the next dot for local mutations. The dot is
// also recorded in the ReceivedClock.
func (r *Replica[M]) NextDot() Dot {
	d := r.Local.Next()
	r.Received.Record(d.Replica, d.Counter)
	return d
}

// HWM returns the received high-water mark vector clock, for use in
// anti-entropy (pass to a peer's DeltasSince) or as causal context for
// remove operations.
func (r *Replica[M]) HWM() VClock {
	return r.Received.HWM()
}
