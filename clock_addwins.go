package crdt

// AddWinsClock implements [Clock] with add-wins semantics. A concurrent put
// beats a concurrent remove. Tombstones carry causal context (a [VClock])
// so the clock can determine whether an entry's dot is "covered" by the
// removal.
//
//   - Put vs entry: LWW ([DotGT])
//   - Put vs tombstone: entry wins if its dot is NOT covered by the
//     tombstone's context
//   - Remove vs entry: entry survives if its dot is NOT covered by the
//     remove's context
//   - Remove vs tombstone: LWW ([DotGT])
//
// Used by: AWLWWMap.
type AddWinsClock struct{}

func (AddWinsClock) Allows(local Queryable, info DeltaInfo) bool {
	remoteDot, err := DecodeDot(info.Meta)
	if err != nil {
		return false
	}

	switch info.Op {
	case OpPut:
		// Against existing entry: LWW.
		if entryMeta, ok := local.EntryMeta(info.Key); ok {
			localDot, err := DecodeDot(entryMeta)
			if err == nil && !DotGT(remoteDot, localDot) {
				return false
			}
			// If err != nil, local meta is corrupted — treat as absent and allow.
		}
		// Against existing tombstone: add-wins — the entry loses only if
		// the tombstone's context covers the entry's dot.
		if tombMeta, ok := local.TombstoneMeta(info.Key); ok {
			// AWLWWMap tombstone meta: [16-byte dot][encoded vclock].
			if len(tombMeta) > 16 {
				tombCtx, err := DecodeVClock(tombMeta[16:])
				if err == nil && tombCtx.Get(remoteDot.Replica) >= remoteDot.Counter {
					return false
				}
				// If err != nil, tombstone context is corrupted — treat as absent and allow.
			}
		}
		return true

	case OpRemove:
		// Against existing entry: add-wins — entry survives if its dot
		// is NOT covered by the remote's context.
		if entryMeta, ok := local.EntryMeta(info.Key); ok {
			entryDot, err := DecodeDot(entryMeta)
			if err == nil && info.Context != nil {
				remoteCtx, err := DecodeVClock(info.Context)
				if err == nil && remoteCtx.Get(entryDot.Replica) < entryDot.Counter {
					return false // entry not covered, add wins
				}
				// If err != nil, remote context is corrupted — allow the delta.
			}
			// If err != nil, local entry meta is corrupted — treat as absent and allow.
		}
		// Against existing tombstone: LWW.
		if tombMeta, ok := local.TombstoneMeta(info.Key); ok {
			tombDot, err := DecodeDot(tombMeta)
			if err == nil && !DotGT(remoteDot, tombDot) {
				return false
			}
			// If err != nil, tombstone meta is corrupted — treat as absent and allow.
		}
		return true
	}

	return false
}
