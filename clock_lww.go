package crdt

// LWWClock implements [Clock] with last-write-wins semantics. A remote delta
// is allowed if its dot is greater than the local entry's dot (and, if a
// tombstone exists, greater than the tombstone's dot). Dot comparison uses
// [DotGT]: higher counter wins, lower replica ID breaks ties.
//
// Used by: LWWMap, LWWRegister.
type LWWClock struct{}

func (LWWClock) Allows(local Queryable, info DeltaInfo) bool {
	remoteDot, err := DecodeDot(info.Meta)
	if err != nil {
		return false
	}

	if entryMeta, ok := local.EntryMeta(info.Key); ok {
		localDot, _ := DecodeDot(entryMeta)
		if !DotGT(remoteDot, localDot) {
			return false
		}
	}

	if tombMeta, ok := local.TombstoneMeta(info.Key); ok {
		localDot, _ := DecodeDot(tombMeta)
		if !DotGT(remoteDot, localDot) {
			return false
		}
	}

	return true
}
