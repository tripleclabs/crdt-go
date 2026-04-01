package crdt

import "encoding"

// State is the interface implemented by all CRDT types. It provides the
// common operations needed for delta synchronization, persistence, and
// generic plumbing (sync engines, storage layers).
//
// Mutation methods are NOT part of this interface because their signatures
// differ per CRDT type. Consumers who know the concrete type call mutation
// methods directly (e.g., [GCounter.Increment], [ORSet.Add]).
type State interface {
	// Value returns the user-visible value of the CRDT. The concrete type
	// depends on the CRDT:
	//   - GCounter, PNCounter → int64
	//   - LWWRegister → any
	//   - MVRegister → []any
	//   - ORSet → []any
	//   - GList → []any
	//   - ORMap, LWWMap, AWLWWMap, DeltaMap → map[string]any
	Value() any

	// VClock returns the vector clock representing this CRDT's causal history.
	VClock() VClock

	// Merge merges a remote state or delta into this CRDT and returns a new
	// merged state. The receiver is not modified. The other State must be of
	// the same concrete CRDT type.
	Merge(other State) State

	// CRDTType returns the type identifier for this CRDT, used for
	// serialization dispatch and generic type handling.
	CRDTType() TypeID

	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}
