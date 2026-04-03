package crdt

import "encoding/binary"

// maxWinsClock implements [clock] with max-wins semantics. A remote delta
// is allowed if its count (encoded as 8 bytes big-endian in Meta) exceeds
// the local count for the same key. If no local state exists, the delta is
// always allowed.
//
// Used by: GCounter, PNCounter.
type maxWinsClock struct{}

func (maxWinsClock) Allows(local queryable, info deltaInfo) bool {
	if len(info.Meta) < 8 {
		return false
	}
	remoteCount := binary.BigEndian.Uint64(info.Meta)

	entryMeta, ok := local.EntryMeta(info.Key)
	if !ok {
		return true
	}
	if len(entryMeta) < 8 {
		return true
	}
	localCount := binary.BigEndian.Uint64(entryMeta)
	return remoteCount > localCount
}
