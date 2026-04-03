package crdt

// alwaysMergeClock implements [clock] by always allowing the delta. All
// merge logic (dotmap combination, context-based pruning, dedup) is handled
// by the storage type's [mergeable.Apply] method.
//
// Used by: ORSet, ORMap, MVRegister, GList.
type alwaysMergeClock struct{}

func (alwaysMergeClock) Allows(queryable, deltaInfo) bool {
	return true
}
