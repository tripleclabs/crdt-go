package crdt

// AWLWWMap stores key → (value, [Dot]) entries and key → (Dot, context VClock)
// tombstones, backed by a [Backend]. The tombstone context is the causal
// snapshot at time of removal — used by the replica layer for add-wins logic.
//
// This is pure storage — no clocks, no merge logic, no delta encoding.
type AWLWWMap[V any] struct {
	codec   Codec[V]
	backend Backend
}

// NewAWLWWMap returns an initialized AWLWWMap.
func NewAWLWWMap[V any](codec Codec[V], opts ...Option) *AWLWWMap[V] {
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = NewMemoryBackend()
	}
	return &AWLWWMap[V]{codec: codec, backend: b}
}

// Put stores a key-value pair with the given dot. Removes any tombstone.
func (m *AWLWWMap[V]) Put(key string, value V, dot Dot) error {
	valBytes, err := m.codec.Encode(value)
	if err != nil {
		return err
	}
	m.backend.PutEntry(key, valBytes, EncodeDot(dot))
	m.backend.DeleteTombstone(key)
	return nil
}

// PutBytes stores pre-encoded value bytes.
func (m *AWLWWMap[V]) PutBytes(key string, valBytes []byte, dot Dot) {
	m.backend.PutEntry(key, valBytes, EncodeDot(dot))
	m.backend.DeleteTombstone(key)
}

// Remove tombstones a key with the given dot and causal context.
func (m *AWLWWMap[V]) Remove(key string, dot Dot, context VClock) {
	m.backend.DeleteEntry(key)
	m.backend.PutTombstone(key, encodeAWTombstone(dot, context))
}

// Get returns the value, its dot, and whether the key exists.
func (m *AWLWWMap[V]) Get(key string) (V, Dot, bool) {
	var zero V
	valBytes, metaBytes, ok := m.backend.GetEntry(key)
	if !ok {
		return zero, Dot{}, false
	}
	v, err := m.codec.Decode(valBytes)
	if err != nil {
		return zero, Dot{}, false
	}
	dot, _ := DecodeDot(metaBytes)
	return v, dot, true
}

// GetBytes returns raw value bytes, dot, and whether exists.
func (m *AWLWWMap[V]) GetBytes(key string) ([]byte, Dot, bool) {
	valBytes, metaBytes, ok := m.backend.GetEntry(key)
	if !ok {
		return nil, Dot{}, false
	}
	dot, _ := DecodeDot(metaBytes)
	return valBytes, dot, true
}

// GetTombstone returns the dot and causal context for a tombstoned key.
func (m *AWLWWMap[V]) GetTombstone(key string) (Dot, VClock, bool) {
	metaBytes, ok := m.backend.GetTombstone(key)
	if !ok {
		return Dot{}, nil, false
	}
	dot, ctx := decodeAWTombstone(metaBytes)
	return dot, ctx, true
}

// Range calls fn for each live entry.
func (m *AWLWWMap[V]) Range(fn func(key string, value V, dot Dot) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		v, err := m.codec.Decode(valBytes)
		if err != nil {
			return true
		}
		dot, _ := DecodeDot(metaBytes)
		return fn(key, v, dot)
	})
}

// RangeTombstones calls fn for each tombstone.
func (m *AWLWWMap[V]) RangeTombstones(fn func(key string, dot Dot, context VClock) bool) {
	m.backend.RangeTombstones(func(key string, metaBytes []byte) bool {
		dot, ctx := decodeAWTombstone(metaBytes)
		return fn(key, dot, ctx)
	})
}

// Len returns the number of live entries.
func (m *AWLWWMap[V]) Len() int { return m.backend.EntryLen() }

func encodeAWTombstone(d Dot, ctx VClock) []byte {
	dotBytes := EncodeDot(d)
	ctxBytes := EncodeVClock(ctx)
	out := make([]byte, len(dotBytes)+len(ctxBytes))
	copy(out, dotBytes)
	copy(out[len(dotBytes):], ctxBytes)
	return out
}

func decodeAWTombstone(b []byte) (Dot, VClock) {
	d, _ := DecodeDot(b)
	ctx, _ := DecodeVClock(b[16:])
	return d, ctx
}
