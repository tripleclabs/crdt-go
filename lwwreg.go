package crdt

// LWWRegister stores a single value with a [Dot]. The dot records when
// and by whom the value was written.
//
// This is pure storage — no clocks, no merge logic.
type LWWRegister[V any] struct {
	codec    Codec[V]
	valBytes []byte
	dot      Dot
	hasVal   bool
}

// NewLWWRegister returns an initialized LWWRegister.
func NewLWWRegister[V any](codec Codec[V]) *LWWRegister[V] {
	return &LWWRegister[V]{codec: codec}
}

// Set stores a value with the given dot.
func (r *LWWRegister[V]) Set(value V, dot Dot) error {
	b, err := r.codec.Encode(value)
	if err != nil {
		return err
	}
	r.valBytes = b
	r.dot = dot
	r.hasVal = true
	return nil
}

// SetBytes stores pre-encoded value bytes with the given dot. Useful when
// applying deltas where the value is already encoded.
func (r *LWWRegister[V]) SetBytes(valBytes []byte, dot Dot) {
	r.valBytes = valBytes
	r.dot = dot
	r.hasVal = true
}

// Get returns the current value, its dot, and whether a value has been set.
func (r *LWWRegister[V]) Get() (V, Dot, bool) {
	var zero V
	if !r.hasVal {
		return zero, Dot{}, false
	}
	v, err := r.codec.Decode(r.valBytes)
	if err != nil {
		return zero, Dot{}, false
	}
	return v, r.dot, true
}

// GetBytes returns the raw encoded value, its dot, and whether set.
func (r *LWWRegister[V]) GetBytes() ([]byte, Dot, bool) {
	if !r.hasVal {
		return nil, Dot{}, false
	}
	return r.valBytes, r.dot, true
}

// HasValue reports whether a value has been set.
func (r *LWWRegister[V]) HasValue() bool { return r.hasVal }
