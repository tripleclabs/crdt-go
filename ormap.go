package crdt

// ORMap stores key → (value, [DotMap]) entries, backed by a [Backend].
// Each key's DotMap tracks which replicas contributed the entry.
//
// This is pure storage — no clocks, no merge logic, no delta encoding.
type ORMap[V any] struct {
	codec   Codec[V]
	backend Backend
}

// NewORMap returns an initialized ORMap.
func NewORMap[V any](codec Codec[V], opts ...Option) *ORMap[V] {
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = NewMemoryBackend()
	}
	return &ORMap[V]{codec: codec, backend: b}
}

// Put stores a key-value pair with the given dotmap.
func (m *ORMap[V]) Put(key string, value V, dots DotMap) error {
	valBytes, err := m.codec.Encode(value)
	if err != nil {
		return err
	}
	m.backend.PutEntry(key, valBytes, EncodeDotMap(dots))
	return nil
}

// PutBytes stores pre-encoded value bytes with the given dotmap.
func (m *ORMap[V]) PutBytes(key string, valBytes []byte, dots DotMap) {
	m.backend.PutEntry(key, valBytes, EncodeDotMap(dots))
}

// Remove removes an entry.
func (m *ORMap[V]) Remove(key string) {
	m.backend.DeleteEntry(key)
}

// Get returns the value, its dotmap, and whether the key exists.
func (m *ORMap[V]) Get(key string) (V, DotMap, bool) {
	var zero V
	valBytes, metaBytes, ok := m.backend.GetEntry(key)
	if !ok {
		return zero, nil, false
	}
	v, err := m.codec.Decode(valBytes)
	if err != nil {
		return zero, nil, false
	}
	dm, _ := DecodeDotMap(metaBytes)
	return v, dm, true
}

// GetBytes returns raw value bytes, dotmap, and whether the key exists.
func (m *ORMap[V]) GetBytes(key string) ([]byte, DotMap, bool) {
	valBytes, metaBytes, ok := m.backend.GetEntry(key)
	if !ok {
		return nil, nil, false
	}
	dm, _ := DecodeDotMap(metaBytes)
	return valBytes, dm, true
}

// Range calls fn for each entry.
func (m *ORMap[V]) Range(fn func(key string, value V, dots DotMap) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		v, err := m.codec.Decode(valBytes)
		if err != nil {
			return true
		}
		dm, _ := DecodeDotMap(metaBytes)
		return fn(key, v, dm)
	})
}

// RangeBytes calls fn for each entry with raw value bytes.
func (m *ORMap[V]) RangeBytes(fn func(key string, valBytes []byte, dots DotMap) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		dm, _ := DecodeDotMap(metaBytes)
		return fn(key, valBytes, dm)
	})
}

// Len returns the number of entries.
func (m *ORMap[V]) Len() int { return m.backend.EntryLen() }
