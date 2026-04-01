package crdt

// MVRegister is a multi-value register CRDT that preserves all concurrent
// writes. When two replicas write different values without first synchronizing,
// both values are kept and exposed via [MVRegister.Values]. A subsequent write
// clears all previously observed values and replaces them with the new one.
//
// The zero value is not usable; create instances with [NewMVRegister].
type MVRegister struct {
	replica ReplicaID
	values  map[any]Dot // value → dot that wrote it
	vclock  VClock
}

// NewMVRegister returns a new MVRegister owned by the given replica.
func NewMVRegister(replica ReplicaID) *MVRegister {
	return &MVRegister{
		replica: replica,
		values:  make(map[any]Dot),
		vclock:  NewVClock(),
	}
}

// Write sets a new value, replacing all previously observed values, and
// returns the new state with a [Delta]. The receiver is not modified.
func (r *MVRegister) Write(value any) (*MVRegister, *Delta) {
	newVC := r.vclock.Increment(r.replica)
	newDot := Dot{Replica: r.replica, Counter: newVC.Get(r.replica)}

	next := &MVRegister{
		replica: r.replica,
		values:  map[any]Dot{value: newDot},
		vclock:  newVC,
	}
	delta := &MVRegister{
		replica: r.replica,
		values:  map[any]Dot{value: newDot},
		vclock:  newVC.Clone(),
	}
	return next, &Delta{Type: TypeMVRegister, State: delta}
}

// Value returns the current values as a []any. If there are no concurrent
// writes, this slice has one element. Multiple elements indicate unresolved
// concurrent writes.
func (r *MVRegister) Value() any {
	return r.Values()
}

// Values returns the current values as a typed []any slice.
func (r *MVRegister) Values() []any {
	out := make([]any, 0, len(r.values))
	for v := range r.values {
		out = append(out, v)
	}
	return out
}

// VClock returns the vector clock for this register.
func (r *MVRegister) VClock() VClock {
	return r.vclock.Clone()
}

// Merge merges a remote MVRegister state and returns the result. Values
// whose dots are causally dominated by the other state's vclock are discarded.
// Non-dominated (concurrent) values from both sides are kept. The receiver
// is not modified.
func (r *MVRegister) Merge(other State) State {
	o := other.(*MVRegister)
	mergedVC := r.vclock.Merge(o.vclock)
	mergedValues := make(map[any]Dot)

	// Keep local values not dominated by remote vclock OR present in remote.
	for v, d := range r.values {
		if d.Counter > o.vclock.Get(d.Replica) {
			// Unseen by remote — concurrent, keep it.
			if existing, ok := mergedValues[v]; !ok || DotGT(d, existing) {
				mergedValues[v] = d
			}
		} else if rd, ok := o.values[v]; ok {
			// Same value exists in both — keep the higher dot.
			best := d
			if DotGT(rd, d) {
				best = rd
			}
			if existing, ok := mergedValues[v]; !ok || DotGT(best, existing) {
				mergedValues[v] = best
			}
		}
	}

	// Keep remote values not dominated by local vclock.
	for v, d := range o.values {
		if d.Counter > r.vclock.Get(d.Replica) {
			if existing, ok := mergedValues[v]; !ok || DotGT(d, existing) {
				mergedValues[v] = d
			}
		}
	}

	// If no values survived, keep the value with the highest dot.
	if len(mergedValues) == 0 && (len(r.values) > 0 || len(o.values) > 0) {
		var bestVal any
		var bestDot Dot
		for v, d := range r.values {
			if bestDot == (Dot{}) || DotGT(d, bestDot) {
				bestVal = v
				bestDot = d
			}
		}
		for v, d := range o.values {
			if bestDot == (Dot{}) || DotGT(d, bestDot) {
				bestVal = v
				bestDot = d
			}
		}
		if bestDot != (Dot{}) {
			mergedValues[bestVal] = bestDot
		}
	}

	return &MVRegister{
		replica: r.replica,
		values:  mergedValues,
		vclock:  mergedVC,
	}
}

// CRDTType returns [TypeMVRegister].
func (r *MVRegister) CRDTType() TypeID {
	return TypeMVRegister
}

// MarshalBinary encodes the MVRegister into a binary format.
func (r *MVRegister) MarshalBinary() ([]byte, error) {
	return gobEncode(r.replica, r.values, map[ReplicaID]uint64(r.vclock))
}

// UnmarshalBinary decodes an MVRegister from binary format.
func (r *MVRegister) UnmarshalBinary(data []byte) error {
	var vc map[ReplicaID]uint64
	if err := gobDecode(data, &r.replica, &r.values, &vc); err != nil {
		return err
	}
	r.vclock = VClock(vc)
	return nil
}
