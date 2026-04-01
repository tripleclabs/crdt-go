package crdt

// AWLWWMap is an add-wins last-write-wins map CRDT. It extends [LWWMap]
// semantics with an add-wins bias: when a put and remove occur concurrently,
// the put wins. This is achieved by recording a causal context (vector clock
// snapshot) in each tombstone. A put dominates a tombstone only if the put's
// dot is NOT covered by the tombstone's context.
//
// The zero value is not usable; create instances with [NewAWLWWMap].
type AWLWWMap struct {
	replica    ReplicaID
	entries    map[string]lwwMapEntry // reuses {Value, Dot}
	tombstones map[string]awTombstone
	vclock     VClock
}

type awTombstone struct {
	Dot     Dot
	Context VClock // vclock snapshot at time of removal
}

// NewAWLWWMap returns a new AWLWWMap owned by the given replica.
func NewAWLWWMap(replica ReplicaID) *AWLWWMap {
	return &AWLWWMap{
		replica:    replica,
		entries:    make(map[string]lwwMapEntry),
		tombstones: make(map[string]awTombstone),
		vclock:     NewVClock(),
	}
}

// Put sets a key-value pair and returns the new state with a [Delta].
// Any tombstone for the key is cleared. The receiver is not modified.
func (m *AWLWWMap) Put(key string, value any) (*AWLWWMap, *Delta) {
	newVC := m.vclock.Increment(m.replica)
	dot := Dot{Replica: m.replica, Counter: newVC.Get(m.replica)}

	newEntries := cloneAWEntries(m.entries)
	newEntries[key] = lwwMapEntry{Value: value, Dot: dot}
	newTombstones := cloneAWTombstones(m.tombstones)
	delete(newTombstones, key)

	next := &AWLWWMap{
		replica:    m.replica,
		entries:    newEntries,
		tombstones: newTombstones,
		vclock:     newVC,
	}

	delta := &AWLWWMap{
		replica:    m.replica,
		entries:    map[string]lwwMapEntry{key: {Value: value, Dot: dot}},
		tombstones: make(map[string]awTombstone),
		vclock:     VClock{m.replica: dot.Counter},
	}
	return next, &Delta{Type: TypeAWLWWMap, State: delta}
}

// Remove marks a key as deleted with a tombstone that includes the current
// causal context. Returns the new state with a [Delta]. The receiver is not
// modified.
func (m *AWLWWMap) Remove(key string) (*AWLWWMap, *Delta) {
	newVC := m.vclock.Increment(m.replica)
	dot := Dot{Replica: m.replica, Counter: newVC.Get(m.replica)}

	newEntries := cloneAWEntries(m.entries)
	delete(newEntries, key)
	newTombstones := cloneAWTombstones(m.tombstones)
	newTombstones[key] = awTombstone{Dot: dot, Context: m.vclock.Clone()}

	next := &AWLWWMap{
		replica:    m.replica,
		entries:    newEntries,
		tombstones: newTombstones,
		vclock:     newVC,
	}

	delta := &AWLWWMap{
		replica:    m.replica,
		entries:    make(map[string]lwwMapEntry),
		tombstones: map[string]awTombstone{key: {Dot: dot, Context: m.vclock.Clone()}},
		vclock:     VClock{m.replica: dot.Counter},
	}
	return next, &Delta{Type: TypeAWLWWMap, State: delta}
}

// Get returns the value for key and whether it exists (not tombstoned).
func (m *AWLWWMap) Get(key string) (any, bool) {
	e, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	return e.Value, true
}

// Value returns the map contents as a map[string]any.
func (m *AWLWWMap) Value() any {
	return m.Map()
}

// Map returns the map contents as a typed map[string]any.
func (m *AWLWWMap) Map() map[string]any {
	out := make(map[string]any, len(m.entries))
	for k, e := range m.entries {
		out[k] = e.Value
	}
	return out
}

// Len returns the number of live entries.
func (m *AWLWWMap) Len() int {
	return len(m.entries)
}

// VClock returns the vector clock for this map.
func (m *AWLWWMap) VClock() VClock {
	return m.vclock.Clone()
}

// Merge merges a remote AWLWWMap state and returns the result. Add-wins
// semantics: an entry dominates a tombstone if the entry's dot is NOT
// covered by the tombstone's causal context. The receiver is not modified.
func (m *AWLWWMap) Merge(other State) State {
	o := other.(*AWLWWMap)
	mergedVC := m.vclock.Merge(o.vclock)
	mergedEntries := make(map[string]lwwMapEntry)
	mergedTombstones := make(map[string]awTombstone)

	// Collect all keys.
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
		// Collect best entry from both sides.
		var bestEntry lwwMapEntry
		var hasBestEntry bool
		if le, ok := m.entries[k]; ok {
			bestEntry = le
			hasBestEntry = true
		}
		if re, ok := o.entries[k]; ok {
			if !hasBestEntry || DotGT(re.Dot, bestEntry.Dot) {
				bestEntry = re
				hasBestEntry = true
			}
		}

		// Collect best tombstone from both sides.
		var bestTombstone awTombstone
		var hasBestTombstone bool
		if lt, ok := m.tombstones[k]; ok {
			bestTombstone = lt
			hasBestTombstone = true
		}
		if rt, ok := o.tombstones[k]; ok {
			if !hasBestTombstone || DotGT(rt.Dot, bestTombstone.Dot) {
				bestTombstone = rt
				hasBestTombstone = true
			} else if hasBestTombstone {
				// Merge contexts for same-level tombstones.
				bestTombstone.Context = bestTombstone.Context.Merge(rt.Context)
			}
		}

		if hasBestEntry && hasBestTombstone {
			// Add-wins: entry dominates if its dot is NOT covered by
			// the tombstone's causal context.
			if dominatedByContext(bestEntry.Dot, bestTombstone.Context) {
				mergedTombstones[k] = bestTombstone
			} else {
				mergedEntries[k] = bestEntry
			}
		} else if hasBestEntry {
			mergedEntries[k] = bestEntry
		} else if hasBestTombstone {
			mergedTombstones[k] = bestTombstone
		}
	}

	return &AWLWWMap{
		replica:    m.replica,
		entries:    mergedEntries,
		tombstones: mergedTombstones,
		vclock:     mergedVC,
	}
}

// dominatedByContext reports whether the dot is covered by the causal context
// — i.e., the context has seen this dot's replica at or beyond this counter.
func dominatedByContext(d Dot, ctx VClock) bool {
	return ctx.Get(d.Replica) >= d.Counter
}

// CRDTType returns [TypeAWLWWMap].
func (m *AWLWWMap) CRDTType() TypeID {
	return TypeAWLWWMap
}

// MarshalBinary encodes the AWLWWMap into a binary format.
func (m *AWLWWMap) MarshalBinary() ([]byte, error) {
	return gobEncode(m.replica, m.entries, m.tombstones, map[ReplicaID]uint64(m.vclock))
}

// UnmarshalBinary decodes an AWLWWMap from binary format.
func (m *AWLWWMap) UnmarshalBinary(data []byte) error {
	var vc map[ReplicaID]uint64
	if err := gobDecode(data, &m.replica, &m.entries, &m.tombstones, &vc); err != nil {
		return err
	}
	m.vclock = VClock(vc)
	return nil
}

func cloneAWEntries(m map[string]lwwMapEntry) map[string]lwwMapEntry {
	out := make(map[string]lwwMapEntry, len(m))
	for k, e := range m {
		out[k] = e
	}
	return out
}

func cloneAWTombstones(m map[string]awTombstone) map[string]awTombstone {
	out := make(map[string]awTombstone, len(m))
	for k, t := range m {
		out[k] = awTombstone{Dot: t.Dot, Context: t.Context.Clone()}
	}
	return out
}
