package crdt

// TypeID identifies a CRDT type for serialization dispatch and factory
// functions. Each CRDT type has a unique TypeID constant.
type TypeID uint8

const (
	// TypeGCounter identifies a [GCounter].
	TypeGCounter TypeID = iota + 1
	// TypePNCounter identifies a [PNCounter].
	TypePNCounter
	// TypeORSet identifies an [ORSet].
	TypeORSet
	// TypeLWWRegister identifies a [LWWRegister].
	TypeLWWRegister
	// TypeMVRegister identifies an [MVRegister].
	TypeMVRegister
	// TypeORMap identifies an [ORMap].
	TypeORMap
	// TypeLWWMap identifies a [LWWMap].
	TypeLWWMap
	// TypeAWLWWMap identifies an [AWLWWMap].
	TypeAWLWWMap
	// TypeGList identifies a [GList].
	TypeGList
	// TypeDeltaMap identifies a [DeltaMap].
	TypeDeltaMap
)

// String returns the human-readable name of the TypeID.
func (t TypeID) String() string {
	switch t {
	case TypeGCounter:
		return "GCounter"
	case TypePNCounter:
		return "PNCounter"
	case TypeORSet:
		return "ORSet"
	case TypeLWWRegister:
		return "LWWRegister"
	case TypeMVRegister:
		return "MVRegister"
	case TypeORMap:
		return "ORMap"
	case TypeLWWMap:
		return "LWWMap"
	case TypeAWLWWMap:
		return "AWLWWMap"
	case TypeGList:
		return "GList"
	case TypeDeltaMap:
		return "DeltaMap"
	default:
		return "Unknown"
	}
}

// Delta represents a minimal state change produced by a CRDT mutation.
// It wraps a [State] that contains only the data that changed. Deltas are
// themselves valid States and can be passed to [State.Merge].
type Delta struct {
	// Type identifies the CRDT type for deserialization dispatch.
	Type TypeID
	// State is the delta payload — a partial CRDT state containing only
	// the changed data.
	State State
}
