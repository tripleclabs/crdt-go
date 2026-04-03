package crdt

// lwwClock implements [clock] with last-write-wins semantics. A remote delta
// is allowed if its dot is greater than the local entry's dot (and, if a
// tombstone exists, greater than the tombstone's dot). Dot comparison uses
// [DotGT]: higher counter wins, lower replica ID breaks ties.
//
// Used by: LWWMap, LWWRegister.
type lwwClock struct{}

func (lwwClock) Allows(local queryable, info deltaInfo) bool {
	remoteDot, err := decodeDot(info.Meta)
	if err != nil {
		return false
	}

	if entryMeta, ok := local.EntryMeta(info.Key); ok {
		localDot, err := decodeDot(entryMeta)
		if err == nil && !DotGT(remoteDot, localDot) {
			return false
		}
	}

	if tombMeta, ok := local.TombstoneMeta(info.Key); ok {
		localDot, err := decodeDot(tombMeta)
		if err == nil && !DotGT(remoteDot, localDot) {
			return false
		}
	}

	return true
}
