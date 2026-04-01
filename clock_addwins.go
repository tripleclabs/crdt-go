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
			localDot, _ := DecodeDot(entryMeta)
			if !DotGT(remoteDot, localDot) {
				return false
			}
		}
		// Against existing tombstone: add-wins — the entry loses only if
		// the tombstone's context covers the entry's dot.
		if tombMeta, ok := local.TombstoneMeta(info.Key); ok {
			// AWLWWMap tombstone meta: [16-byte dot][encoded vclock].
			if len(tombMeta) > 16 {
				tombCtx, _ := DecodeVClock(tombMeta[16:])
				if tombCtx.Get(remoteDot.Replica) >= remoteDot.Counter {
					return false
				}
			}
		}
		return true

	case OpRemove:
		// Against existing entry: add-wins — entry survives if its dot
		// is NOT covered by the remote's context.
		if entryMeta, ok := local.EntryMeta(info.Key); ok {
			entryDot, _ := DecodeDot(entryMeta)
			if info.Context != nil {
				remoteCtx, _ := DecodeVClock(info.Context)
				if remoteCtx.Get(entryDot.Replica) < entryDot.Counter {
					return false // entry not covered, add wins
				}
			}
		}
		// Against existing tombstone: LWW.
		if tombMeta, ok := local.TombstoneMeta(info.Key); ok {
			tombDot, _ := DecodeDot(tombMeta)
			if !DotGT(remoteDot, tombDot) {
				return false
			}
		}
		return true
	}

	return false
}
