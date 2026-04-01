package crdt

// LWWMap stores key → (value, [Dot]) entries and key → [Dot] tombstones,
// backed by a [Backend]. Values are encoded via the provided [Codec].
//
// This is pure storage — no clocks, no merge logic, no delta encoding.
type LWWMap[V any] struct {
	codec   Codec[V]
	backend Backend
}

// NewLWWMap returns an initialized LWWMap.
func NewLWWMap[V any](codec Codec[V], opts ...Option) *LWWMap[V] {
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = NewMemoryBackend()
	}
	return &LWWMap[V]{codec: codec, backend: b}
}

// Put stores a key-value pair with the given dot. Removes any tombstone
// for the key.
func (m *LWWMap[V]) Put(key string, value V, dot Dot) error {
	valBytes, err := m.codec.Encode(value)
	if err != nil {
		return err
	}
	m.backend.PutEntry(key, valBytes, EncodeDot(dot))
	m.backend.DeleteTombstone(key)
	return nil
}

// PutBytes stores pre-encoded value bytes with the given dot.
func (m *LWWMap[V]) PutBytes(key string, valBytes []byte, dot Dot) {
	m.backend.PutEntry(key, valBytes, EncodeDot(dot))
	m.backend.DeleteTombstone(key)
}

// Remove tombstones a key with the given dot. Removes any entry for the key.
func (m *LWWMap[V]) Remove(key string, dot Dot) {
	m.backend.DeleteEntry(key)
	m.backend.PutTombstone(key, EncodeDot(dot))
}

// Get returns the value, its dot, and whether the key exists.
func (m *LWWMap[V]) Get(key string) (V, Dot, bool) {
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

// GetBytes returns the raw value bytes, dot, and whether the key exists.
func (m *LWWMap[V]) GetBytes(key string) ([]byte, Dot, bool) {
	valBytes, metaBytes, ok := m.backend.GetEntry(key)
	if !ok {
		return nil, Dot{}, false
	}
	dot, _ := DecodeDot(metaBytes)
	return valBytes, dot, true
}

// GetTombstone returns the dot for a tombstoned key.
func (m *LWWMap[V]) GetTombstone(key string) (Dot, bool) {
	metaBytes, ok := m.backend.GetTombstone(key)
	if !ok {
		return Dot{}, false
	}
	dot, _ := DecodeDot(metaBytes)
	return dot, true
}

// Range calls fn for each live entry.
func (m *LWWMap[V]) Range(fn func(key string, value V, dot Dot) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		v, err := m.codec.Decode(valBytes)
		if err != nil {
			return true // skip decode errors
		}
		dot, _ := DecodeDot(metaBytes)
		return fn(key, v, dot)
	})
}

// RangeBytes calls fn for each live entry with raw value bytes.
func (m *LWWMap[V]) RangeBytes(fn func(key string, valBytes []byte, dot Dot) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		dot, _ := DecodeDot(metaBytes)
		return fn(key, valBytes, dot)
	})
}

// RangeTombstones calls fn for each tombstone.
func (m *LWWMap[V]) RangeTombstones(fn func(key string, dot Dot) bool) {
	m.backend.RangeTombstones(func(key string, metaBytes []byte) bool {
		dot, _ := DecodeDot(metaBytes)
		return fn(key, dot)
	})
}

// Len returns the number of live entries.
func (m *LWWMap[V]) Len() int { return m.backend.EntryLen() }

// TombstoneLen returns the number of tombstones.
func (m *LWWMap[V]) TombstoneLen() int { return m.backend.TombstoneLen() }
