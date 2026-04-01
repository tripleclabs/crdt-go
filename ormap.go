package crdt

// ORMap is an observed-remove map CRDT with add-wins semantics. When a key
// is concurrently added and removed, the add wins. Each key's value is
// tracked alongside a [DotMap] recording which replicas contributed the entry.
//
// The zero value is not usable; create instances with [NewORMap].
type ORMap struct {
	replica ReplicaID
	entries map[string]orMapEntry
	vclock  VClock
}

type orMapEntry struct {
	Value any
	Dots  DotMap
}

// NewORMap returns a new ORMap owned by the given replica.
func NewORMap(replica ReplicaID) *ORMap {
	return &ORMap{
		replica: replica,
		entries: make(map[string]orMapEntry),
		vclock:  NewVClock(),
	}
}

// Put sets a key-value pair and returns the new state with a [Delta].
// The receiver is not modified.
func (m *ORMap) Put(key string, value any) (*ORMap, *Delta) {
	newVC := m.vclock.Increment(m.replica)
	newDot := newVC.Get(m.replica)

	newEntries := m.cloneEntries()
	dots := DotMap{m.replica: newDot}
	if existing, ok := newEntries[key]; ok {
		dots = CombineDots(existing.Dots, dots)
	}
	newEntries[key] = orMapEntry{Value: value, Dots: dots}

	next := &ORMap{replica: m.replica, entries: newEntries, vclock: newVC}

	deltaEntries := map[string]orMapEntry{
		key: {Value: value, Dots: DotMap{m.replica: newDot}},
	}
	delta := &ORMap{
		replica: m.replica,
		entries: deltaEntries,
		vclock:  VClock{m.replica: newDot},
	}
	return next, &Delta{Type: TypeORMap, State: delta}
}

// Remove removes a key and returns the new state with a [Delta]. Only dots
// observed by this replica are removed — concurrent adds from other replicas
// survive (add-wins). The receiver is not modified.
func (m *ORMap) Remove(key string) (*ORMap, *Delta) {
	newEntries := m.cloneEntries()
	delete(newEntries, key)

	next := &ORMap{
		replica: m.replica,
		entries: newEntries,
		vclock:  m.vclock.Clone(),
	}

	delta := &orMapRemoveDelta{
		replica: m.replica,
		key:     key,
		vclock:  m.vclock.Clone(),
	}
	return next, &Delta{Type: TypeORMap, State: delta}
}

// Get returns the value for key and whether it exists.
func (m *ORMap) Get(key string) (any, bool) {
	e, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	return e.Value, true
}

// Value returns the map contents as a map[string]any.
func (m *ORMap) Value() any {
	return m.Map()
}

// Map returns the map contents as a typed map[string]any.
func (m *ORMap) Map() map[string]any {
	out := make(map[string]any, len(m.entries))
	for k, e := range m.entries {
		out[k] = e.Value
	}
	return out
}

// Len returns the number of entries.
func (m *ORMap) Len() int {
	return len(m.entries)
}

// VClock returns the vector clock for this map.
func (m *ORMap) VClock() VClock {
	return m.vclock.Clone()
}

// Merge merges a remote ORMap state and returns the result. Add-wins
// semantics are applied: keys present in either map survive if they have
// unseen dots. The receiver is not modified.
func (m *ORMap) Merge(other State) State {
	if rd, ok := other.(*orMapRemoveDelta); ok {
		return m.mergeRemoveDelta(rd)
	}

	o := other.(*ORMap)
	mergedVC := m.vclock.Merge(o.vclock)
	mergedEntries := make(map[string]orMapEntry)

	// Start with all local entries.
	for k, e := range m.entries {
		mergedEntries[k] = orMapEntry{Value: e.Value, Dots: CloneDotMap(e.Dots)}
	}

	// Merge in remote entries.
	for k, remoteE := range o.entries {
		if localE, ok := mergedEntries[k]; ok {
			// Both have the key — merge dots, keep the value from the higher dot.
			combinedDots := CombineDots(localE.Dots, remoteE.Dots)
			winner := localE.Value
			localMax := MaxDot(localE.Dots)
			remoteMax := MaxDot(remoteE.Dots)
			if DotGT(remoteMax, localMax) {
				winner = remoteE.Value
			}
			mergedEntries[k] = orMapEntry{Value: winner, Dots: combinedDots}
		} else {
			// Key only in remote — keep if it has unseen dots.
			dm := make(DotMap)
			for r, c := range remoteE.Dots {
				if c > m.vclock.Get(r) {
					dm[r] = c
				}
			}
			if len(dm) > 0 {
				mergedEntries[k] = orMapEntry{Value: remoteE.Value, Dots: dm}
			}
		}
	}

	// Remove local entries whose dots are fully dominated by remote vclock
	// and not present in remote (remote removed them).
	for k, localE := range mergedEntries {
		if _, inRemote := o.entries[k]; !inRemote {
			surviving := make(DotMap)
			for r, c := range localE.Dots {
				if c > o.vclock.Get(r) {
					surviving[r] = c
				}
			}
			if len(surviving) > 0 {
				mergedEntries[k] = orMapEntry{Value: localE.Value, Dots: surviving}
			} else {
				delete(mergedEntries, k)
			}
		}
	}

	return &ORMap{replica: m.replica, entries: mergedEntries, vclock: mergedVC}
}

func (m *ORMap) mergeRemoveDelta(rd *orMapRemoveDelta) State {
	newEntries := m.cloneEntries()
	if e, ok := newEntries[rd.key]; ok {
		surviving := make(DotMap)
		for r, c := range e.Dots {
			if c > rd.vclock.Get(r) {
				surviving[r] = c
			}
		}
		if len(surviving) > 0 {
			newEntries[rd.key] = orMapEntry{Value: e.Value, Dots: surviving}
		} else {
			delete(newEntries, rd.key)
		}
	}
	return &ORMap{
		replica: m.replica,
		entries: newEntries,
		vclock:  m.vclock.Merge(rd.vclock),
	}
}

// CRDTType returns [TypeORMap].
func (m *ORMap) CRDTType() TypeID {
	return TypeORMap
}

// MarshalBinary encodes the ORMap into a binary format.
func (m *ORMap) MarshalBinary() ([]byte, error) {
	return gobEncode(m.replica, m.entries, map[ReplicaID]uint64(m.vclock))
}

// UnmarshalBinary decodes an ORMap from binary format.
func (m *ORMap) UnmarshalBinary(data []byte) error {
	var vc map[ReplicaID]uint64
	if err := gobDecode(data, &m.replica, &m.entries, &vc); err != nil {
		return err
	}
	m.vclock = VClock(vc)
	return nil
}

func (m *ORMap) cloneEntries() map[string]orMapEntry {
	out := make(map[string]orMapEntry, len(m.entries))
	for k, e := range m.entries {
		out[k] = orMapEntry{Value: e.Value, Dots: CloneDotMap(e.Dots)}
	}
	return out
}

// orMapRemoveDelta is an internal delta for remove operations.
type orMapRemoveDelta struct {
	replica ReplicaID
	key     string
	vclock  VClock
}

func (d *orMapRemoveDelta) Value() any          { return nil }
func (d *orMapRemoveDelta) VClock() VClock      { return d.vclock }
func (d *orMapRemoveDelta) CRDTType() TypeID    { return TypeORMap }
func (d *orMapRemoveDelta) Merge(_ State) State { return d }
func (d *orMapRemoveDelta) MarshalBinary() ([]byte, error) {
	return gobEncode(true, d.replica, d.key, map[ReplicaID]uint64(d.vclock))
}
func (d *orMapRemoveDelta) UnmarshalBinary(data []byte) error {
	var isRemove bool
	var vc map[ReplicaID]uint64
	if err := gobDecode(data, &isRemove, &d.replica, &d.key, &vc); err != nil {
		return err
	}
	d.vclock = VClock(vc)
	return nil
}
