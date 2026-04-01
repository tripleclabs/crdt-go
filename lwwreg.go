package crdt

// LWWRegister stores a single value with a [Dot]. The dot records when
// and by whom the value was written.
//
// LWWRegister implements [Mergeable] for use with [Replica] and [LWWClock].
type LWWRegister[V any] struct {
	codec    Codec[V]
	valBytes []byte
	dot      Dot
	hasVal   bool
}

// NewLWWRegister returns an initialized LWWRegister.
func NewLWWRegister[V any](codec Codec[V]) *LWWRegister[V] {
	requireCodec(codec)
	return &LWWRegister[V]{codec: codec}
}

// NewLWWRegisterReplica creates a [Replica] wrapping an [LWWRegister] with [LWWClock].
func NewLWWRegisterReplica[V any](replicaID ReplicaID, codec Codec[V]) *Replica[*LWWRegister[V]] {
	return NewReplica[*LWWRegister[V]](replicaID, NewLWWRegister(codec), LWWClock{})
}

// --- Mutations ---

// Set stores a value with the given dot and returns the encoded delta.
//
// Delta format: [varint val len][val bytes][16 byte dot]
func (r *LWWRegister[V]) Set(value V, dot Dot) ([]byte, error) {
	b, err := r.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	r.valBytes = b
	r.dot = dot
	r.hasVal = true

	var buf []byte
	buf = AppendVarintBytes(buf, b)
	buf = append(buf, EncodeDot(dot)...)
	return buf, nil
}

// SetBytes stores pre-encoded value bytes with the given dot.
func (r *LWWRegister[V]) SetBytes(valBytes []byte, dot Dot) {
	r.valBytes = valBytes
	r.dot = dot
	r.hasVal = true
}

// --- Reads ---

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

// --- Queryable ---

// EntryMeta returns the encoded dot of the register's current value.
// The key is ignored (registers are single-valued).
func (r *LWWRegister[V]) EntryMeta(string) ([]byte, bool) {
	if !r.hasVal {
		return nil, false
	}
	return EncodeDot(r.dot), true
}

// TombstoneMeta always returns false — LWWRegister has no tombstones.
func (r *LWWRegister[V]) TombstoneMeta(string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [DeltaInfo] from an LWWRegister delta.
func (r *LWWRegister[V]) ParseDelta(delta []byte) (DeltaInfo, error) {
	_, off, err := ReadVarintBytes(delta, 0)
	if err != nil {
		return DeltaInfo{}, err
	}
	if off+16 > len(delta) {
		return DeltaInfo{}, ErrShortBuffer
	}
	dot, err := DecodeDot(delta[off:])
	if err != nil {
		return DeltaInfo{}, err
	}
	return DeltaInfo{
		Key:  "",
		Meta: delta[off : off+16],
		Dots: []Dot{dot},
	}, nil
}

// Apply unconditionally sets the register to the delta's value and dot.
func (r *LWWRegister[V]) Apply(delta []byte) error {
	valBytes, off, err := ReadVarintBytes(delta, 0)
	if err != nil {
		return err
	}
	if off+16 > len(delta) {
		return ErrShortBuffer
	}
	remoteDot, err := DecodeDot(delta[off:])
	if err != nil {
		return err
	}
	r.SetBytes(valBytes, remoteDot)
	return nil
}

// DeltasSince returns the register as a delta if the peer hasn't seen it.
func (r *LWWRegister[V]) DeltasSince(peerHWM VClock) [][]byte {
	if !r.hasVal {
		return nil
	}
	if r.dot.Counter <= peerHWM.Get(r.dot.Replica) {
		return nil
	}
	var buf []byte
	buf = AppendVarintBytes(buf, r.valBytes)
	buf = append(buf, EncodeDot(r.dot)...)
	return [][]byte{buf}
}
