package crdt

// LWWRegister is a last-write-wins register CRDT. It holds a single value,
// and concurrent writes are resolved deterministically: the write with the
// higher counter wins. When counters are equal, the lower replica ID wins.
//
// The zero value is not usable; create instances with [NewLWWRegister].
type LWWRegister struct {
	replica ReplicaID
	value   any
	dot     Dot
	vclock  VClock
}

// NewLWWRegister returns a new LWWRegister owned by the given replica.
func NewLWWRegister(replica ReplicaID) *LWWRegister {
	return &LWWRegister{
		replica: replica,
		vclock:  NewVClock(),
	}
}

// Set writes a new value to the register and returns the new state with a
// [Delta]. The receiver is not modified.
func (r *LWWRegister) Set(value any) (*LWWRegister, *Delta) {
	newVC := r.vclock.Increment(r.replica)
	newDot := Dot{Replica: r.replica, Counter: newVC.Get(r.replica)}

	next := &LWWRegister{
		replica: r.replica,
		value:   value,
		dot:     newDot,
		vclock:  newVC,
	}
	delta := &LWWRegister{
		replica: r.replica,
		value:   value,
		dot:     newDot,
		vclock:  newVC.Clone(),
	}
	return next, &Delta{Type: TypeLWWRegister, State: delta}
}

// Get returns the current value and whether it has been set. Returns nil and
// false if the register has never been written to.
func (r *LWWRegister) Get() (any, bool) {
	if r.dot == (Dot{}) {
		return nil, false
	}
	return r.value, true
}

// Value returns the current register value (nil if never set).
func (r *LWWRegister) Value() any {
	return r.value
}

// VClock returns the vector clock for this register.
func (r *LWWRegister) VClock() VClock {
	return r.vclock.Clone()
}

// Merge merges a remote LWWRegister state and returns the result. The value
// with the greater dot (per [DotGT] semantics) is kept. The vector clocks
// are merged. The receiver is not modified.
func (r *LWWRegister) Merge(other State) State {
	o := other.(*LWWRegister)
	mergedVC := r.vclock.Merge(o.vclock)

	winner := r
	if o.dot != (Dot{}) && (r.dot == (Dot{}) || DotGT(o.dot, r.dot)) {
		winner = o
	}

	return &LWWRegister{
		replica: r.replica,
		value:   winner.value,
		dot:     winner.dot,
		vclock:  mergedVC,
	}
}

// CRDTType returns [TypeLWWRegister].
func (r *LWWRegister) CRDTType() TypeID {
	return TypeLWWRegister
}

// MarshalBinary encodes the LWWRegister into a binary format.
func (r *LWWRegister) MarshalBinary() ([]byte, error) {
	return gobEncode(r.replica, &r.value, r.dot, map[ReplicaID]uint64(r.vclock))
}

// UnmarshalBinary decodes a LWWRegister from binary format.
func (r *LWWRegister) UnmarshalBinary(data []byte) error {
	var vc map[ReplicaID]uint64
	if err := gobDecode(data, &r.replica, &r.value, &r.dot, &vc); err != nil {
		return err
	}
	r.vclock = VClock(vc)
	return nil
}
