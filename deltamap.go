package crdt

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// DeltaMap is a typed nested CRDT map. Each key holds a CRDT value of a
// specified type (e.g., a GCounter, ORSet, or even another DeltaMap). Keys
// are tracked with dot maps for add-wins semantics, and tombstones record
// deletions.
//
// DeltaMap enables document-like structures where each field is independently
// mergeable. For example, a user profile might have a GCounter for views,
// an LWWRegister for the display name, and an ORSet for tags.
//
// The zero value is not usable; create instances with [NewDeltaMap].
type DeltaMap struct {
	replica    ReplicaID
	entries    map[string]deltaMapEntry
	tombstones map[string]Dot
	vclock     VClock
}

type deltaMapEntry struct {
	Type  TypeID
	Value State
	Dots  DotMap
}

// NewDeltaMap returns a new DeltaMap owned by the given replica.
func NewDeltaMap(replica ReplicaID) *DeltaMap {
	return &DeltaMap{
		replica:    replica,
		entries:    make(map[string]deltaMapEntry),
		tombstones: make(map[string]Dot),
		vclock:     NewVClock(),
	}
}

// Put stores a typed CRDT value under key. The value is wrapped in the
// appropriate CRDT type based on typeID. Returns the new state with a [Delta].
// The receiver is not modified.
func (m *DeltaMap) Put(key string, typeID TypeID, value any) (*DeltaMap, *Delta) {
	newVC := m.vclock.Increment(m.replica)
	dot := newVC.Get(m.replica)

	state := wrapValue(m.replica, typeID, value)

	newEntries := m.cloneEntries()
	dots := DotMap{m.replica: dot}
	if existing, ok := newEntries[key]; ok {
		dots = CombineDots(existing.Dots, dots)
	}
	newEntries[key] = deltaMapEntry{Type: typeID, Value: state, Dots: dots}
	newTombstones := cloneDotMapString(m.tombstones)
	delete(newTombstones, key)

	next := &DeltaMap{
		replica:    m.replica,
		entries:    newEntries,
		tombstones: newTombstones,
		vclock:     newVC,
	}

	deltaEntry := deltaMapEntry{
		Type:  typeID,
		Value: state,
		Dots:  DotMap{m.replica: dot},
	}
	delta := &DeltaMap{
		replica:    m.replica,
		entries:    map[string]deltaMapEntry{key: deltaEntry},
		tombstones: make(map[string]Dot),
		vclock:     VClock{m.replica: dot},
	}
	return next, &Delta{Type: TypeDeltaMap, State: delta}
}

// Remove deletes a key with a tombstone. Returns the new state with a [Delta].
// The receiver is not modified.
func (m *DeltaMap) Remove(key string) (*DeltaMap, *Delta) {
	newVC := m.vclock.Increment(m.replica)
	dot := Dot{Replica: m.replica, Counter: newVC.Get(m.replica)}

	newEntries := m.cloneEntries()
	delete(newEntries, key)
	newTombstones := cloneDotMapString(m.tombstones)
	newTombstones[key] = dot

	next := &DeltaMap{
		replica:    m.replica,
		entries:    newEntries,
		tombstones: newTombstones,
		vclock:     newVC,
	}

	delta := &DeltaMap{
		replica:    m.replica,
		entries:    make(map[string]deltaMapEntry),
		tombstones: map[string]Dot{key: dot},
		vclock:     VClock{m.replica: dot.Counter},
	}
	return next, &Delta{Type: TypeDeltaMap, State: delta}
}

// Get returns the CRDT [State] for key, or nil and false if the key doesn't
// exist or is tombstoned.
func (m *DeltaMap) Get(key string) (State, bool) {
	e, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	return e.Value, true
}

// Value returns the map contents as a map[string]any, where each value is
// the result of calling Value() on the nested CRDT.
func (m *DeltaMap) Value() any {
	out := make(map[string]any, len(m.entries))
	for k, e := range m.entries {
		out[k] = e.Value.Value()
	}
	return out
}

// Len returns the number of live entries.
func (m *DeltaMap) Len() int {
	return len(m.entries)
}

// VClock returns the vector clock for this map.
func (m *DeltaMap) VClock() VClock {
	return m.vclock.Clone()
}

// Merge merges a remote DeltaMap state and returns the result. For entries
// present in both, the nested CRDT values are themselves merged. Add-wins
// semantics apply to keys: entries with unseen dots survive concurrent
// removes. The receiver is not modified.
func (m *DeltaMap) Merge(other State) State {
	o := other.(*DeltaMap)
	mergedVC := m.vclock.Merge(o.vclock)
	mergedEntries := make(map[string]deltaMapEntry)
	mergedTombstones := make(map[string]Dot)

	// Start with local entries.
	for k, e := range m.entries {
		mergedEntries[k] = deltaMapEntry{
			Type:  e.Type,
			Value: e.Value,
			Dots:  CloneDotMap(e.Dots),
		}
	}

	// Merge remote entries.
	for k, remoteE := range o.entries {
		if localE, ok := mergedEntries[k]; ok {
			// Both have it — merge the nested CRDTs and combine dots.
			mergedValue := localE.Value.Merge(remoteE.Value)
			mergedEntries[k] = deltaMapEntry{
				Type:  localE.Type,
				Value: mergedValue,
				Dots:  CombineDots(localE.Dots, remoteE.Dots),
			}
		} else {
			// Only in remote — keep if has unseen dots.
			dm := make(DotMap)
			for r, c := range remoteE.Dots {
				if c > m.vclock.Get(r) {
					dm[r] = c
				}
			}
			if len(dm) > 0 {
				mergedEntries[k] = deltaMapEntry{
					Type:  remoteE.Type,
					Value: remoteE.Value,
					Dots:  dm,
				}
			}
		}
	}

	// Remove local entries dominated by remote vclock and absent from remote.
	for k, localE := range mergedEntries {
		if _, inRemote := o.entries[k]; !inRemote {
			surviving := make(DotMap)
			for r, c := range localE.Dots {
				if c > o.vclock.Get(r) {
					surviving[r] = c
				}
			}
			if len(surviving) > 0 {
				me := mergedEntries[k]
				me.Dots = surviving
				mergedEntries[k] = me
			} else {
				delete(mergedEntries, k)
			}
		}
	}

	// Merge tombstones — keep the higher dot per key.
	for k, lt := range m.tombstones {
		mergedTombstones[k] = lt
	}
	for k, rt := range o.tombstones {
		if existing, ok := mergedTombstones[k]; !ok || DotGT(rt, existing) {
			mergedTombstones[k] = rt
		}
	}

	// Reconcile entries vs tombstones.
	for k, ts := range mergedTombstones {
		if e, ok := mergedEntries[k]; ok {
			// Entry survives if it has dots NOT dominated by tombstone.
			surviving := make(DotMap)
			for r, c := range e.Dots {
				if c > ts.Counter || r != ts.Replica {
					// More nuanced: dot survives if it's unseen by the
					// tombstone's perspective. Use the simpler check: entry
					// dot counter > tombstone counter for same replica.
					if !DotMember(DotMap{ts.Replica: ts.Counter}, Dot{r, c}) || c > ts.Counter {
						surviving[r] = c
					}
				}
			}
			// Simpler: check if any dot is strictly newer than the tombstone.
			hasUnseen := false
			for r, c := range e.Dots {
				_ = r
				if c > ts.Counter {
					hasUnseen = true
					break
				}
			}
			if hasUnseen {
				// Keep entry, remove tombstone.
				delete(mergedTombstones, k)
			} else {
				// Tombstone wins.
				delete(mergedEntries, k)
			}
		}
	}

	return &DeltaMap{
		replica:    m.replica,
		entries:    mergedEntries,
		tombstones: mergedTombstones,
		vclock:     mergedVC,
	}
}

// CRDTType returns [TypeDeltaMap].
func (m *DeltaMap) CRDTType() TypeID {
	return TypeDeltaMap
}

// MarshalBinary encodes the DeltaMap into a binary format.
func (m *DeltaMap) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(m.replica); err != nil {
		return nil, err
	}

	// Encode entries count, then each entry.
	if err := enc.Encode(len(m.entries)); err != nil {
		return nil, err
	}
	for k, e := range m.entries {
		if err := enc.Encode(k); err != nil {
			return nil, err
		}
		if err := enc.Encode(e.Type); err != nil {
			return nil, err
		}
		data, err := e.Value.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if err := enc.Encode(data); err != nil {
			return nil, err
		}
		if err := enc.Encode(e.Dots); err != nil {
			return nil, err
		}
	}

	if err := enc.Encode(m.tombstones); err != nil {
		return nil, err
	}
	if err := enc.Encode(map[ReplicaID]uint64(m.vclock)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary decodes a DeltaMap from binary format.
func (m *DeltaMap) UnmarshalBinary(data []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&m.replica); err != nil {
		return err
	}

	var count int
	if err := dec.Decode(&count); err != nil {
		return err
	}
	m.entries = make(map[string]deltaMapEntry, count)
	for i := 0; i < count; i++ {
		var k string
		if err := dec.Decode(&k); err != nil {
			return err
		}
		var typeID TypeID
		if err := dec.Decode(&typeID); err != nil {
			return err
		}
		var raw []byte
		if err := dec.Decode(&raw); err != nil {
			return err
		}
		state, err := unmarshalByType(typeID, raw)
		if err != nil {
			return err
		}
		var dots DotMap
		if err := dec.Decode(&dots); err != nil {
			return err
		}
		m.entries[k] = deltaMapEntry{Type: typeID, Value: state, Dots: dots}
	}

	if err := dec.Decode(&m.tombstones); err != nil {
		return err
	}
	var vc map[ReplicaID]uint64
	if err := dec.Decode(&vc); err != nil {
		return err
	}
	m.vclock = VClock(vc)
	return nil
}

func (m *DeltaMap) cloneEntries() map[string]deltaMapEntry {
	out := make(map[string]deltaMapEntry, len(m.entries))
	for k, e := range m.entries {
		out[k] = deltaMapEntry{
			Type:  e.Type,
			Value: e.Value,
			Dots:  CloneDotMap(e.Dots),
		}
	}
	return out
}

// wrapValue creates a CRDT State of the given type initialized with value.
func wrapValue(replica ReplicaID, typeID TypeID, value any) State {
	switch typeID {
	case TypeGCounter:
		c := NewGCounter(replica)
		if v, ok := value.(uint64); ok && v > 0 {
			c, _ = c.Increment(v)
		}
		return c
	case TypePNCounter:
		return NewPNCounter(replica)
	case TypeLWWRegister:
		r := NewLWWRegister(replica)
		if value != nil {
			r, _ = r.Set(value)
		}
		return r
	case TypeMVRegister:
		r := NewMVRegister(replica)
		if value != nil {
			r, _ = r.Write(value)
		}
		return r
	case TypeORSet:
		return NewORSet(replica)
	case TypeGList:
		return NewGList(replica)
	case TypeORMap:
		return NewORMap(replica)
	case TypeLWWMap:
		return NewLWWMap(replica)
	case TypeAWLWWMap:
		return NewAWLWWMap(replica)
	case TypeDeltaMap:
		return NewDeltaMap(replica)
	default:
		return NewLWWRegister(replica)
	}
}

// unmarshalByType creates a CRDT of the given type and unmarshals data into it.
func unmarshalByType(typeID TypeID, data []byte) (State, error) {
	var s State
	switch typeID {
	case TypeGCounter:
		s = &GCounter{}
	case TypePNCounter:
		s = &PNCounter{}
	case TypeLWWRegister:
		s = &LWWRegister{}
	case TypeMVRegister:
		s = &MVRegister{}
	case TypeORSet:
		s = &ORSet{}
	case TypeGList:
		s = &GList{}
	case TypeORMap:
		s = &ORMap{}
	case TypeLWWMap:
		s = &LWWMap{}
	case TypeAWLWWMap:
		s = &AWLWWMap{}
	case TypeDeltaMap:
		s = &DeltaMap{}
	default:
		return nil, fmt.Errorf("crdt: unknown type ID %d", typeID)
	}
	if err := s.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return s, nil
}

// UnmarshalState creates a CRDT of the given type and decodes data into it.
// This is the top-level factory for persistence-friendly deserialization.
func UnmarshalState(typeID TypeID, data []byte) (State, error) {
	return unmarshalByType(typeID, data)
}
