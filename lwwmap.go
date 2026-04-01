package crdt

// LWWMap is a last-write-wins map CRDT. Each key has an associated dot
// (replica, counter) that determines which write wins on merge. Removals
// are tracked via tombstones with their own dots. A put always beats a
// concurrent remove for the same key (put clears the tombstone).
//
// The zero value is not usable; create instances with [NewLWWMap].
type LWWMap struct {
	replica    ReplicaID
	entries    map[string]lwwMapEntry
	tombstones map[string]Dot
	vclock     VClock
}

type lwwMapEntry struct {
	Value any
	Dot   Dot
}

// NewLWWMap returns a new LWWMap owned by the given replica.
func NewLWWMap(replica ReplicaID) *LWWMap {
	return &LWWMap{
		replica:    replica,
		entries:    make(map[string]lwwMapEntry),
		tombstones: make(map[string]Dot),
		vclock:     NewVClock(),
	}
}

// Put sets a key-value pair, removing any tombstone for the key. Returns
// the new state with a [Delta]. The receiver is not modified.
func (m *LWWMap) Put(key string, value any) (*LWWMap, *Delta) {
	newVC := m.vclock.Increment(m.replica)
	dot := Dot{Replica: m.replica, Counter: newVC.Get(m.replica)}

	newEntries := m.cloneEntries()
	newEntries[key] = lwwMapEntry{Value: value, Dot: dot}
	newTombstones := cloneDotMapString(m.tombstones)
	delete(newTombstones, key)

	next := &LWWMap{
		replica:    m.replica,
		entries:    newEntries,
		tombstones: newTombstones,
		vclock:     newVC,
	}

	delta := &LWWMap{
		replica:    m.replica,
		entries:    map[string]lwwMapEntry{key: {Value: value, Dot: dot}},
		tombstones: make(map[string]Dot),
		vclock:     VClock{m.replica: dot.Counter},
	}
	return next, &Delta{Type: TypeLWWMap, State: delta}
}

// Remove marks a key as deleted with a tombstone. Returns the new state
// with a [Delta]. The receiver is not modified.
func (m *LWWMap) Remove(key string) (*LWWMap, *Delta) {
	newVC := m.vclock.Increment(m.replica)
	dot := Dot{Replica: m.replica, Counter: newVC.Get(m.replica)}

	newEntries := m.cloneEntries()
	delete(newEntries, key)
	newTombstones := cloneDotMapString(m.tombstones)
	newTombstones[key] = dot

	next := &LWWMap{
		replica:    m.replica,
		entries:    newEntries,
		tombstones: newTombstones,
		vclock:     newVC,
	}

	delta := &LWWMap{
		replica:    m.replica,
		entries:    make(map[string]lwwMapEntry),
		tombstones: map[string]Dot{key: dot},
		vclock:     VClock{m.replica: dot.Counter},
	}
	return next, &Delta{Type: TypeLWWMap, State: delta}
}

// Get returns the value for key and whether it exists (not tombstoned).
func (m *LWWMap) Get(key string) (any, bool) {
	e, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	return e.Value, true
}

// Value returns the map contents as a map[string]any (excluding tombstoned keys).
func (m *LWWMap) Value() any {
	return m.Map()
}

// Map returns the map contents as a typed map[string]any.
func (m *LWWMap) Map() map[string]any {
	out := make(map[string]any, len(m.entries))
	for k, e := range m.entries {
		out[k] = e.Value
	}
	return out
}

// Len returns the number of live (non-tombstoned) entries.
func (m *LWWMap) Len() int {
	return len(m.entries)
}

// VClock returns the vector clock for this map.
func (m *LWWMap) VClock() VClock {
	return m.vclock.Clone()
}

// Merge merges a remote LWWMap state and returns the result. For each key,
// the entry or tombstone with the higher dot (per [DotGT]) wins. The
// receiver is not modified.
func (m *LWWMap) Merge(other State) State {
	o := other.(*LWWMap)
	mergedVC := m.vclock.Merge(o.vclock)
	mergedEntries := make(map[string]lwwMapEntry)
	mergedTombstones := make(map[string]Dot)

	// Collect all keys from both sides.
	allKeys := make(map[string]struct{})
	for k := range m.entries {
		allKeys[k] = struct{}{}
	}
	for k := range m.tombstones {
		allKeys[k] = struct{}{}
	}
	for k := range o.entries {
		allKeys[k] = struct{}{}
	}
	for k := range o.tombstones {
		allKeys[k] = struct{}{}
	}

	for k := range allKeys {
		// Find the best entry dot and best tombstone dot across both.
		var bestEntry lwwMapEntry
		var bestEntryDot Dot
		var hasBestEntry bool

		if le, ok := m.entries[k]; ok {
			bestEntry = le
			bestEntryDot = le.Dot
			hasBestEntry = true
		}
		if re, ok := o.entries[k]; ok {
			if !hasBestEntry || DotGT(re.Dot, bestEntryDot) {
				bestEntry = re
				bestEntryDot = re.Dot
				hasBestEntry = true
			}
		}

		var bestTombstone Dot
		var hasBestTombstone bool
		if lt, ok := m.tombstones[k]; ok {
			bestTombstone = lt
			hasBestTombstone = true
		}
		if rt, ok := o.tombstones[k]; ok {
			if !hasBestTombstone || DotGT(rt, bestTombstone) {
				bestTombstone = rt
				hasBestTombstone = true
			}
		}

		// Entry wins over tombstone if entry dot > tombstone dot.
		if hasBestEntry && hasBestTombstone {
			if DotGT(bestEntryDot, bestTombstone) {
				mergedEntries[k] = bestEntry
			} else {
				mergedTombstones[k] = bestTombstone
			}
		} else if hasBestEntry {
			mergedEntries[k] = bestEntry
		} else if hasBestTombstone {
			mergedTombstones[k] = bestTombstone
		}
	}

	return &LWWMap{
		replica:    m.replica,
		entries:    mergedEntries,
		tombstones: mergedTombstones,
		vclock:     mergedVC,
	}
}

// CRDTType returns [TypeLWWMap].
func (m *LWWMap) CRDTType() TypeID {
	return TypeLWWMap
}

// MarshalBinary encodes the LWWMap into a binary format.
func (m *LWWMap) MarshalBinary() ([]byte, error) {
	return gobEncode(m.replica, m.entries, m.tombstones, map[ReplicaID]uint64(m.vclock))
}

// UnmarshalBinary decodes a LWWMap from binary format.
func (m *LWWMap) UnmarshalBinary(data []byte) error {
	var vc map[ReplicaID]uint64
	if err := gobDecode(data, &m.replica, &m.entries, &m.tombstones, &vc); err != nil {
		return err
	}
	m.vclock = VClock(vc)
	return nil
}

func (m *LWWMap) cloneEntries() map[string]lwwMapEntry {
	out := make(map[string]lwwMapEntry, len(m.entries))
	for k, e := range m.entries {
		out[k] = e
	}
	return out
}

func cloneDotMapString(m map[string]Dot) map[string]Dot {
	out := make(map[string]Dot, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
