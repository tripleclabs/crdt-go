package crdt

// AlwaysMergeClock implements [Clock] by always allowing the delta. All
// merge logic (dotmap combination, context-based pruning, dedup) is handled
// by the storage type's [Mergeable.Apply] method.
//
// Used by: ORSet, ORMap, MVRegister, GList.
type AlwaysMergeClock struct{}

func (AlwaysMergeClock) Allows(Queryable, DeltaInfo) bool {
	return true
}
