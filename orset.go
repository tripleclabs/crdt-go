package crdt

// ORSet is an observed-remove set CRDT with add-wins semantics. When a
// concurrent add and remove occur for the same element, the add wins.
// Each element is tracked by a [DotMap] recording which replicas added it
// and when.
//
// Element values must be comparable (usable as Go map keys). The zero value
// is not usable; create instances with [NewORSet].
type ORSet struct {
	replica  ReplicaID
	elements map[any]DotMap // value → {replica → counter}
	vclock   VClock
}

// NewORSet returns a new ORSet owned by the given replica.
func NewORSet(replica ReplicaID) *ORSet {
	return &ORSet{
		replica:  replica,
		elements: make(map[any]DotMap),
		vclock:   NewVClock(),
	}
}

// Add adds a value to the set and returns the new state with a [Delta].
// If the value already exists, a new dot is still recorded (strengthening
// the add). The receiver is not modified.
func (s *ORSet) Add(value any) (*ORSet, *Delta) {
	newVC := s.vclock.Increment(s.replica)
	newDot := newVC.Get(s.replica)

	newElements := s.cloneElements()
	if existing, ok := newElements[value]; ok {
		dm := CloneDotMap(existing)
		dm[s.replica] = newDot
		newElements[value] = dm
	} else {
		newElements[value] = DotMap{s.replica: newDot}
	}

	next := &ORSet{replica: s.replica, elements: newElements, vclock: newVC}

	deltaElements := map[any]DotMap{value: {s.replica: newDot}}
	delta := &ORSet{
		replica:  s.replica,
		elements: deltaElements,
		vclock:   VClock{s.replica: newDot},
	}
	return next, &Delta{Type: TypeORSet, State: delta}
}

// Remove removes a value from the set and returns the new state with a
// [Delta]. The remove only affects dots that this replica has observed —
// concurrent adds from other replicas will survive (add-wins). The receiver
// is not modified.
func (s *ORSet) Remove(value any) (*ORSet, *Delta) {
	newElements := s.cloneElements()
	delete(newElements, value)

	next := &ORSet{
		replica:  s.replica,
		elements: newElements,
		vclock:   s.vclock.Clone(),
	}

	// The remove delta carries an empty set for this value with the current
	// vclock, so the merge recipient knows to remove dots it has already seen.
	delta := &orSetRemoveDelta{
		replica: s.replica,
		value:   value,
		vclock:  s.vclock.Clone(),
	}
	return next, &Delta{Type: TypeORSet, State: delta}
}

// Contains reports whether value is in the set.
func (s *ORSet) Contains(value any) bool {
	_, ok := s.elements[value]
	return ok
}

// Value returns the set elements as a []any in no particular order.
func (s *ORSet) Value() any {
	return s.Elements()
}

// Elements returns the set elements as a typed []any slice.
func (s *ORSet) Elements() []any {
	out := make([]any, 0, len(s.elements))
	for v := range s.elements {
		out = append(out, v)
	}
	return out
}

// Len returns the number of elements in the set.
func (s *ORSet) Len() int {
	return len(s.elements)
}

// VClock returns the vector clock for this set.
func (s *ORSet) VClock() VClock {
	return s.vclock.Clone()
}

// Merge merges a remote ORSet state (or delta) and returns the result.
// Add-wins semantics: for each element present in either set, the union of
// dots is kept. The receiver is not modified.
//
// If the other State is an internal remove delta, the merge removes observed
// dots while preserving concurrent adds.
func (s *ORSet) Merge(other State) State {
	// Handle remove delta.
	if rd, ok := other.(*orSetRemoveDelta); ok {
		return s.mergeRemoveDelta(rd)
	}

	o := other.(*ORSet)
	mergedVC := s.vclock.Merge(o.vclock)
	mergedElements := make(map[any]DotMap)

	// Add all elements from local.
	for v, dm := range s.elements {
		mergedElements[v] = CloneDotMap(dm)
	}

	// Merge in elements from remote.
	for v, remoteDM := range o.elements {
		if localDM, ok := mergedElements[v]; ok {
			mergedElements[v] = CombineDots(localDM, remoteDM)
		} else {
			// Add-wins: element exists in remote but not local.
			// Keep it if it has dots not yet seen by our vclock.
			dm := make(DotMap)
			for r, c := range remoteDM {
				if c > s.vclock.Get(r) {
					dm[r] = c
				}
			}
			// Also keep dots that we've seen (they might have been
			// concurrently re-added after we processed a remove).
			// Actually for add-wins ORSet: if element is in remote,
			// keep all dots. The correct approach: keep dots from remote
			// that are either unseen by local OR also in local's element.
			// Since local doesn't have this element, keep only unseen dots.
			if len(dm) > 0 {
				mergedElements[v] = dm
			}
		}
	}

	// Remove elements from local that are fully dominated by remote's vclock
	// but not present in remote (meaning remote removed them).
	for v, localDM := range mergedElements {
		if _, inRemote := o.elements[v]; !inRemote {
			// Keep only dots that remote hasn't seen.
			surviving := make(DotMap)
			for r, c := range localDM {
				if c > o.vclock.Get(r) {
					surviving[r] = c
				}
			}
			if len(surviving) > 0 {
				mergedElements[v] = surviving
			} else {
				delete(mergedElements, v)
			}
		}
	}

	return &ORSet{
		replica:  s.replica,
		elements: mergedElements,
		vclock:   mergedVC,
	}
}

func (s *ORSet) mergeRemoveDelta(rd *orSetRemoveDelta) State {
	newElements := s.cloneElements()
	if dm, ok := newElements[rd.value]; ok {
		// Remove dots that the remover has seen.
		surviving := make(DotMap)
		for r, c := range dm {
			if c > rd.vclock.Get(r) {
				surviving[r] = c
			}
		}
		if len(surviving) > 0 {
			newElements[rd.value] = surviving
		} else {
			delete(newElements, rd.value)
		}
	}

	return &ORSet{
		replica:  s.replica,
		elements: newElements,
		vclock:   s.vclock.Merge(rd.vclock),
	}
}

// CRDTType returns [TypeORSet].
func (s *ORSet) CRDTType() TypeID {
	return TypeORSet
}

// MarshalBinary encodes the ORSet into a binary format.
func (s *ORSet) MarshalBinary() ([]byte, error) {
	return gobEncode(s.replica, s.elements, map[ReplicaID]uint64(s.vclock))
}

// UnmarshalBinary decodes an ORSet from binary format.
func (s *ORSet) UnmarshalBinary(data []byte) error {
	var vc map[ReplicaID]uint64
	if err := gobDecode(data, &s.replica, &s.elements, &vc); err != nil {
		return err
	}
	s.vclock = VClock(vc)
	return nil
}

func (s *ORSet) cloneElements() map[any]DotMap {
	out := make(map[any]DotMap, len(s.elements))
	for v, dm := range s.elements {
		out[v] = CloneDotMap(dm)
	}
	return out
}

// orSetRemoveDelta is an internal delta type representing a remove operation.
// It carries the removed value and the vclock at the time of removal, allowing
// the merge recipient to remove only dots it has already seen.
type orSetRemoveDelta struct {
	replica ReplicaID
	value   any
	vclock  VClock
}

func (d *orSetRemoveDelta) Value() any          { return nil }
func (d *orSetRemoveDelta) VClock() VClock      { return d.vclock }
func (d *orSetRemoveDelta) CRDTType() TypeID    { return TypeORSet }
func (d *orSetRemoveDelta) Merge(_ State) State { return d }
func (d *orSetRemoveDelta) MarshalBinary() ([]byte, error) {
	return gobEncode(true, d.replica, &d.value, map[ReplicaID]uint64(d.vclock))
}
func (d *orSetRemoveDelta) UnmarshalBinary(data []byte) error {
	var isRemove bool
	var vc map[ReplicaID]uint64
	if err := gobDecode(data, &isRemove, &d.replica, &d.value, &vc); err != nil {
		return err
	}
	d.vclock = VClock(vc)
	return nil
}
