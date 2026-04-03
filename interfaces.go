package crdt

// deltaInfo is the type-agnostic causal summary of a delta, extracted by a
// CRDT storage type via [mergeable.ParseDelta]. It carries enough information
// for a [clock] to decide domination and for [replica] to update the
// [receivedClock].
type deltaInfo struct {
	// Op is the operation code (e.g., OpPut, OpRemove).
	Op byte
	// Key is the target key or element identifier.
	Key string
	// Meta is the remote causal metadata (encoded dot, dotmap, or count).
	Meta []byte
	// Context is the remote causal context for add-wins removes. Nil for
	// operations that don't carry context.
	Context []byte
	// Dots contains all causal dots in this delta, used to update the
	// ReceivedClock regardless of whether the delta is applied.
	Dots []Dot
}

// queryable lets a [clock] inspect local CRDT state for comparison against
// an incoming delta. Backend-based types delegate to
// [Backend.GetEntry]/[Backend.GetTombstone]; non-Backend types encode their
// struct fields as bytes.
type queryable interface {
	// EntryMeta returns the causal metadata for the entry at key.
	// Returns ok=false if no entry exists.
	EntryMeta(key string) (meta []byte, ok bool)

	// TombstoneMeta returns the causal metadata for the tombstone at key.
	// Returns ok=false if no tombstone exists.
	TombstoneMeta(key string) (meta []byte, ok bool)
}

// clock determines whether a remote delta dominates local state. Each
// implementation encapsulates a domination rule (LWW, add-wins, max-wins,
// etc.). The [replica] calls Allows before applying any delta.
type clock interface {
	// Allows reports whether the remote delta described by info should be
	// applied, given the local state accessible via local.
	Allows(local queryable, info deltaInfo) bool
}

// mergeable is implemented by each CRDT storage type. It combines
// [queryable] (for clock comparison) with delta parsing, application, and
// anti-entropy encoding.
type mergeable interface {
	queryable

	// ParseDelta extracts a [DeltaInfo] from raw delta bytes without
	// modifying state.
	ParseDelta(delta []byte) (deltaInfo, error)

	// Apply unconditionally merges a delta into local state. The caller
	// must ensure [Clock.Allows] returned true before calling Apply.
	Apply(delta []byte) error

	// DeltasSince returns encoded deltas for state not covered by peerHWM.
	DeltasSince(peerHWM VClock) [][]byte
}
