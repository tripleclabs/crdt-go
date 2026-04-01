package crdt

// LWWMap stores key → (value, [Dot]) entries and key → [Dot] tombstones,
// backed by a [Backend]. Values are encoded via the provided [Codec].
//
// LWWMap implements [Mergeable] for use with [Replica] and [LWWClock].
type LWWMap[V any] struct {
	codec   Codec[V]
	backend Backend
}

// NewLWWMap returns an initialized LWWMap. Use [NewLWWMapReplica] to create
// a fully wired Replica.
func NewLWWMap[V any](codec Codec[V], opts ...Option) *LWWMap[V] {
	requireCodec(codec)
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = NewMemoryBackend()
	}
	return &LWWMap[V]{codec: codec, backend: b}
}

// NewLWWMapReplica creates a [Replica] wrapping an [LWWMap] with [LWWClock].
func NewLWWMapReplica[V any](replicaID ReplicaID, codec Codec[V], opts ...Option) *Replica[*LWWMap[V]] {
	return NewReplica[*LWWMap[V]](replicaID, NewLWWMap(codec, opts...), LWWClock{})
}

// --- Mutations (return delta bytes) ---

// Put stores a key-value pair with the given dot. Removes any tombstone for
// the key. Returns the encoded delta to send to peers.
//
// Delta format: [op=0x01][varint key len][key][varint val len][val][16 byte dot]
func (m *LWWMap[V]) Put(key string, value V, dot Dot) ([]byte, error) {
	valBytes, err := m.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	m.backend.PutEntry(key, valBytes, EncodeDot(dot))
	m.backend.DeleteTombstone(key)

	buf := []byte{OpPut}
	buf = AppendVarintBytes(buf, []byte(key))
	buf = AppendVarintBytes(buf, valBytes)
	buf = append(buf, EncodeDot(dot)...)
	return buf, nil
}

// PutBytes stores pre-encoded value bytes with the given dot.
func (m *LWWMap[V]) PutBytes(key string, valBytes []byte, dot Dot) {
	m.backend.PutEntry(key, valBytes, EncodeDot(dot))
	m.backend.DeleteTombstone(key)
}

// Remove tombstones a key with the given dot. Removes any entry for the key.
// Returns the encoded delta to send to peers.
//
// Delta format: [op=0x02][varint key len][key][16 byte dot]
func (m *LWWMap[V]) Remove(key string, dot Dot) []byte {
	m.backend.DeleteEntry(key)
	m.backend.PutTombstone(key, EncodeDot(dot))

	buf := []byte{OpRemove}
	buf = AppendVarintBytes(buf, []byte(key))
	buf = append(buf, EncodeDot(dot)...)
	return buf
}

// --- Reads ---

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
	dot, err := DecodeDot(metaBytes)
	if err != nil {
		return zero, Dot{}, false
	}
	return v, dot, true
}

// GetBytes returns the raw value bytes, dot, and whether the key exists.
func (m *LWWMap[V]) GetBytes(key string) ([]byte, Dot, bool) {
	valBytes, metaBytes, ok := m.backend.GetEntry(key)
	if !ok {
		return nil, Dot{}, false
	}
	dot, err := DecodeDot(metaBytes)
	if err != nil {
		return nil, Dot{}, false
	}
	return valBytes, dot, true
}

// GetTombstone returns the dot for a tombstoned key.
func (m *LWWMap[V]) GetTombstone(key string) (Dot, bool) {
	metaBytes, ok := m.backend.GetTombstone(key)
	if !ok {
		return Dot{}, false
	}
	dot, err := DecodeDot(metaBytes)
	if err != nil {
		return Dot{}, false
	}
	return dot, true
}

// Range calls fn for each live entry.
func (m *LWWMap[V]) Range(fn func(key string, value V, dot Dot) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		v, err := m.codec.Decode(valBytes)
		if err != nil {
			return true // skip decode errors
		}
		dot, err := DecodeDot(metaBytes)
		if err != nil {
			return true // skip corrupted entry
		}
		return fn(key, v, dot)
	})
}

// RangeBytes calls fn for each live entry with raw value bytes.
func (m *LWWMap[V]) RangeBytes(fn func(key string, valBytes []byte, dot Dot) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		dot, err := DecodeDot(metaBytes)
		if err != nil {
			return true // skip corrupted entry
		}
		return fn(key, valBytes, dot)
	})
}

// RangeTombstones calls fn for each tombstone.
func (m *LWWMap[V]) RangeTombstones(fn func(key string, dot Dot) bool) {
	m.backend.RangeTombstones(func(key string, metaBytes []byte) bool {
		dot, err := DecodeDot(metaBytes)
		if err != nil {
			return true // skip corrupted entry
		}
		return fn(key, dot)
	})
}

// Len returns the number of live entries.
func (m *LWWMap[V]) Len() int { return m.backend.EntryLen() }

// TombstoneLen returns the number of tombstones.
func (m *LWWMap[V]) TombstoneLen() int { return m.backend.TombstoneLen() }

// --- Queryable ---

// EntryMeta returns the encoded dot for the entry at key.
func (m *LWWMap[V]) EntryMeta(key string) ([]byte, bool) {
	_, meta, ok := m.backend.GetEntry(key)
	return meta, ok
}

// TombstoneMeta returns the encoded dot for the tombstone at key.
func (m *LWWMap[V]) TombstoneMeta(key string) ([]byte, bool) {
	return m.backend.GetTombstone(key)
}

// --- Mergeable ---

// ParseDelta extracts a [DeltaInfo] from an encoded LWWMap delta.
func (m *LWWMap[V]) ParseDelta(delta []byte) (DeltaInfo, error) {
	if len(delta) < 1 {
		return DeltaInfo{}, ErrShortBuffer
	}
	op := delta[0]
	switch op {
	case OpPut:
		return m.parsePutDelta(delta[1:])
	case OpRemove:
		return m.parseRemoveDelta(delta[1:])
	default:
		return DeltaInfo{}, ErrUnknownOp
	}
}

func (m *LWWMap[V]) parsePutDelta(data []byte) (DeltaInfo, error) {
	keyBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return DeltaInfo{}, err
	}
	_, off, err = ReadVarintBytes(data, off)
	if err != nil {
		return DeltaInfo{}, err
	}
	if off+16 > len(data) {
		return DeltaInfo{}, ErrShortBuffer
	}
	dot, err := DecodeDot(data[off:])
	if err != nil {
		return DeltaInfo{}, err
	}
	return DeltaInfo{
		Op:   OpPut,
		Key:  string(keyBytes),
		Meta: data[off : off+16],
		Dots: []Dot{dot},
	}, nil
}

func (m *LWWMap[V]) parseRemoveDelta(data []byte) (DeltaInfo, error) {
	keyBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return DeltaInfo{}, err
	}
	if off+16 > len(data) {
		return DeltaInfo{}, ErrShortBuffer
	}
	dot, err := DecodeDot(data[off:])
	if err != nil {
		return DeltaInfo{}, err
	}
	return DeltaInfo{
		Op:   OpRemove,
		Key:  string(keyBytes),
		Meta: data[off : off+16],
		Dots: []Dot{dot},
	}, nil
}

// Apply unconditionally merges a delta into the LWWMap. The caller must
// ensure the clock has already approved the delta.
func (m *LWWMap[V]) Apply(delta []byte) error {
	if len(delta) < 1 {
		return ErrShortBuffer
	}
	switch delta[0] {
	case OpPut:
		return m.applyPut(delta[1:])
	case OpRemove:
		return m.applyRemove(delta[1:])
	default:
		return ErrUnknownOp
	}
}

func (m *LWWMap[V]) applyPut(data []byte) error {
	keyBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return err
	}
	valBytes, off, err := ReadVarintBytes(data, off)
	if err != nil {
		return err
	}
	if off+16 > len(data) {
		return ErrShortBuffer
	}
	remoteDot, err := DecodeDot(data[off:])
	if err != nil {
		return err
	}
	m.PutBytes(string(keyBytes), valBytes, remoteDot)
	return nil
}

func (m *LWWMap[V]) applyRemove(data []byte) error {
	keyBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return err
	}
	if off+16 > len(data) {
		return ErrShortBuffer
	}
	remoteDot, err := DecodeDot(data[off:])
	if err != nil {
		return err
	}
	m.Remove(string(keyBytes), remoteDot)
	return nil
}

// DeltasSince returns encoded deltas for entries and tombstones with dots
// not covered by peerHWM.
func (m *LWWMap[V]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte

	m.RangeBytes(func(key string, valBytes []byte, dot Dot) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			buf := []byte{OpPut}
			buf = AppendVarintBytes(buf, []byte(key))
			buf = AppendVarintBytes(buf, valBytes)
			buf = append(buf, EncodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	m.RangeTombstones(func(key string, dot Dot) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			buf := []byte{OpRemove}
			buf = AppendVarintBytes(buf, []byte(key))
			buf = append(buf, EncodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	return deltas
}
