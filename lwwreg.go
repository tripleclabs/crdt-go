package crdt

// lwwRegisterState stores a single value with a [Dot]. The dot records when
// and by whom the value was written.
//
// lwwRegisterState implements [mergeable] for use with [replica] and [lwwClock].
type lwwRegisterState[V any] struct {
	codec    Codec[V]
	valBytes []byte
	dot      Dot
	hasVal   bool
}

// newLWWRegisterState returns an initialized LWWRegister.
func newLWWRegisterState[V any](codec Codec[V]) *lwwRegisterState[V] {
	requireCodec(codec)
	return &lwwRegisterState[V]{codec: codec}
}

// --- Mutations ---

// Set stores a value with the given dot and returns the encoded delta.
//
// Delta format: [varint val len][val bytes][16 byte dot]
func (r *lwwRegisterState[V]) Set(value V, dot Dot) ([]byte, error) {
	b, err := r.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	r.valBytes = b
	r.dot = dot
	r.hasVal = true

	var buf []byte
	buf = appendVarintBytes(buf, b)
	buf = append(buf, encodeDot(dot)...)
	return buf, nil
}

// SetBytes stores pre-encoded value bytes with the given dot.
func (r *lwwRegisterState[V]) SetBytes(valBytes []byte, dot Dot) {
	r.valBytes = valBytes
	r.dot = dot
	r.hasVal = true
}

// --- Reads ---

// Get returns the current value, its dot, and whether a value has been set.
func (r *lwwRegisterState[V]) Get() (V, Dot, bool) {
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
func (r *lwwRegisterState[V]) GetBytes() ([]byte, Dot, bool) {
	if !r.hasVal {
		return nil, Dot{}, false
	}
	return r.valBytes, r.dot, true
}

// HasValue reports whether a value has been set.
func (r *lwwRegisterState[V]) HasValue() bool { return r.hasVal }

// --- Queryable ---

// EntryMeta returns the encoded dot of the register's current value.
// The key is ignored (registers are single-valued).
func (r *lwwRegisterState[V]) EntryMeta(string) ([]byte, bool) {
	if !r.hasVal {
		return nil, false
	}
	return encodeDot(r.dot), true
}

// TombstoneMeta always returns false — LWWRegister has no tombstones.
func (r *lwwRegisterState[V]) TombstoneMeta(string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [deltaInfo] from an LWWRegister delta.
func (r *lwwRegisterState[V]) ParseDelta(delta []byte) (deltaInfo, error) {
	_, off, err := readVarintBytes(delta, 0)
	if err != nil {
		return deltaInfo{}, err
	}
	if off+16 > len(delta) {
		return deltaInfo{}, errShortBuffer
	}
	dot, err := decodeDot(delta[off:])
	if err != nil {
		return deltaInfo{}, err
	}
	return deltaInfo{
		Key:  "",
		Meta: delta[off : off+16],
		Dots: []Dot{dot},
	}, nil
}

// Apply unconditionally sets the register to the delta's value and dot.
func (r *lwwRegisterState[V]) Apply(delta []byte) error {
	valBytes, off, err := readVarintBytes(delta, 0)
	if err != nil {
		return err
	}
	if off+16 > len(delta) {
		return errShortBuffer
	}
	remoteDot, err := decodeDot(delta[off:])
	if err != nil {
		return err
	}
	r.SetBytes(valBytes, remoteDot)
	return nil
}

// DeltasSince returns the register as a delta if the peer hasn't seen it.
func (r *lwwRegisterState[V]) DeltasSince(peerHWM VClock) [][]byte {
	if !r.hasVal {
		return nil
	}
	if r.dot.Counter <= peerHWM.Get(r.dot.Replica) {
		return nil
	}
	var buf []byte
	buf = appendVarintBytes(buf, r.valBytes)
	buf = append(buf, encodeDot(r.dot)...)
	return [][]byte{buf}
}
