package crdt

// lwwMapState stores key → (value, [Dot]) entries and key → [Dot] tombstones,
// backed by a [Backend]. Values are encoded via the provided [Codec].
//
// lwwMapState implements [mergeable] for use with [replica] and [lwwClock].
type lwwMapState[V any] struct {
	codec   Codec[V]
	backend Backend
}

// newLWWMapState returns an initialized LWWMap. Use [newLWWMapReplica] to create
// a fully wired Replica.
func newLWWMapState[V any](codec Codec[V], opts ...Option) *lwwMapState[V] {
	requireCodec(codec)
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = newMemoryBackend()
	}
	return &lwwMapState[V]{codec: codec, backend: b}
}

// --- Mutations (return delta bytes) ---

// Put stores a key-value pair with the given dot. Removes any tombstone for
// the key. Returns the encoded delta to send to peers.
//
// Delta format: [op=0x01][varint key len][key][varint val len][val][16 byte dot]
func (m *lwwMapState[V]) Put(key string, value V, dot Dot) ([]byte, error) {
	valBytes, err := m.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	m.backend.PutEntry(key, valBytes, encodeDot(dot))
	m.backend.DeleteTombstone(key)

	buf := []byte{opPut}
	buf = appendVarintBytes(buf, []byte(key))
	buf = appendVarintBytes(buf, valBytes)
	buf = append(buf, encodeDot(dot)...)
	return buf, nil
}

// PutBytes stores pre-encoded value bytes with the given dot.
func (m *lwwMapState[V]) PutBytes(key string, valBytes []byte, dot Dot) {
	m.backend.PutEntry(key, valBytes, encodeDot(dot))
	m.backend.DeleteTombstone(key)
}

// Remove tombstones a key with the given dot. Removes any entry for the key.
// Returns the encoded delta to send to peers.
//
// Delta format: [op=0x02][varint key len][key][16 byte dot]
func (m *lwwMapState[V]) Remove(key string, dot Dot) []byte {
	m.backend.DeleteEntry(key)
	m.backend.PutTombstone(key, encodeDot(dot))

	buf := []byte{opRemove}
	buf = appendVarintBytes(buf, []byte(key))
	buf = append(buf, encodeDot(dot)...)
	return buf
}

// --- Reads ---

// Get returns the value, its dot, and whether the key exists.
func (m *lwwMapState[V]) Get(key string) (V, Dot, bool) {
	var zero V
	valBytes, metaBytes, ok := m.backend.GetEntry(key)
	if !ok {
		return zero, Dot{}, false
	}
	v, err := m.codec.Decode(valBytes)
	if err != nil {
		return zero, Dot{}, false
	}
	dot, err := decodeDot(metaBytes)
	if err != nil {
		return zero, Dot{}, false
	}
	return v, dot, true
}

// GetBytes returns the raw value bytes, dot, and whether the key exists.
func (m *lwwMapState[V]) GetBytes(key string) ([]byte, Dot, bool) {
	valBytes, metaBytes, ok := m.backend.GetEntry(key)
	if !ok {
		return nil, Dot{}, false
	}
	dot, err := decodeDot(metaBytes)
	if err != nil {
		return nil, Dot{}, false
	}
	return valBytes, dot, true
}

// GetTombstone returns the dot for a tombstoned key.
func (m *lwwMapState[V]) GetTombstone(key string) (Dot, bool) {
	metaBytes, ok := m.backend.GetTombstone(key)
	if !ok {
		return Dot{}, false
	}
	dot, err := decodeDot(metaBytes)
	if err != nil {
		return Dot{}, false
	}
	return dot, true
}

// Range calls fn for each live entry.
func (m *lwwMapState[V]) Range(fn func(key string, value V, dot Dot) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		v, err := m.codec.Decode(valBytes)
		if err != nil {
			return true // skip decode errors
		}
		dot, err := decodeDot(metaBytes)
		if err != nil {
			return true // skip corrupted entry
		}
		return fn(key, v, dot)
	})
}

// RangeBytes calls fn for each live entry with raw value bytes.
func (m *lwwMapState[V]) RangeBytes(fn func(key string, valBytes []byte, dot Dot) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		dot, err := decodeDot(metaBytes)
		if err != nil {
			return true // skip corrupted entry
		}
		return fn(key, valBytes, dot)
	})
}

// RangeTombstones calls fn for each tombstone.
func (m *lwwMapState[V]) RangeTombstones(fn func(key string, dot Dot) bool) {
	m.backend.RangeTombstones(func(key string, metaBytes []byte) bool {
		dot, err := decodeDot(metaBytes)
		if err != nil {
			return true // skip corrupted entry
		}
		return fn(key, dot)
	})
}

// Len returns the number of live entries.
func (m *lwwMapState[V]) Len() int { return m.backend.EntryLen() }

// TombstoneLen returns the number of tombstones.
func (m *lwwMapState[V]) TombstoneLen() int { return m.backend.TombstoneLen() }

// --- Queryable ---

// EntryMeta returns the encoded dot for the entry at key.
func (m *lwwMapState[V]) EntryMeta(key string) ([]byte, bool) {
	_, meta, ok := m.backend.GetEntry(key)
	return meta, ok
}

// TombstoneMeta returns the encoded dot for the tombstone at key.
func (m *lwwMapState[V]) TombstoneMeta(key string) ([]byte, bool) {
	return m.backend.GetTombstone(key)
}

// --- Mergeable ---

// ParseDelta extracts a [deltaInfo] from an encoded LWWMap delta.
func (m *lwwMapState[V]) ParseDelta(delta []byte) (deltaInfo, error) {
	if len(delta) < 1 {
		return deltaInfo{}, errShortBuffer
	}
	op := delta[0]
	switch op {
	case opPut:
		return m.parsePutDelta(delta[1:])
	case opRemove:
		return m.parseRemoveDelta(delta[1:])
	default:
		return deltaInfo{}, errUnknownOp
	}
}

func (m *lwwMapState[V]) parsePutDelta(data []byte) (deltaInfo, error) {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return deltaInfo{}, err
	}
	_, off, err = readVarintBytes(data, off)
	if err != nil {
		return deltaInfo{}, err
	}
	if off+16 > len(data) {
		return deltaInfo{}, errShortBuffer
	}
	dot, err := decodeDot(data[off:])
	if err != nil {
		return deltaInfo{}, err
	}
	return deltaInfo{
		Op:   opPut,
		Key:  string(keyBytes),
		Meta: data[off : off+16],
		Dots: []Dot{dot},
	}, nil
}

func (m *lwwMapState[V]) parseRemoveDelta(data []byte) (deltaInfo, error) {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return deltaInfo{}, err
	}
	if off+16 > len(data) {
		return deltaInfo{}, errShortBuffer
	}
	dot, err := decodeDot(data[off:])
	if err != nil {
		return deltaInfo{}, err
	}
	return deltaInfo{
		Op:   opRemove,
		Key:  string(keyBytes),
		Meta: data[off : off+16],
		Dots: []Dot{dot},
	}, nil
}

// Apply unconditionally merges a delta into the LWWMap. The caller must
// ensure the clock has already approved the delta.
func (m *lwwMapState[V]) Apply(delta []byte) error {
	if len(delta) < 1 {
		return errShortBuffer
	}
	switch delta[0] {
	case opPut:
		return m.applyPut(delta[1:])
	case opRemove:
		return m.applyRemove(delta[1:])
	default:
		return errUnknownOp
	}
}

func (m *lwwMapState[V]) applyPut(data []byte) error {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	valBytes, off, err := readVarintBytes(data, off)
	if err != nil {
		return err
	}
	if off+16 > len(data) {
		return errShortBuffer
	}
	remoteDot, err := decodeDot(data[off:])
	if err != nil {
		return err
	}
	m.PutBytes(string(keyBytes), valBytes, remoteDot)
	return nil
}

func (m *lwwMapState[V]) applyRemove(data []byte) error {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	if off+16 > len(data) {
		return errShortBuffer
	}
	remoteDot, err := decodeDot(data[off:])
	if err != nil {
		return err
	}
	m.Remove(string(keyBytes), remoteDot)
	return nil
}

// DeltasSince returns encoded deltas for entries and tombstones with dots
// not covered by peerHWM.
func (m *lwwMapState[V]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte

	m.RangeBytes(func(key string, valBytes []byte, dot Dot) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			buf := []byte{opPut}
			buf = appendVarintBytes(buf, []byte(key))
			buf = appendVarintBytes(buf, valBytes)
			buf = append(buf, encodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	m.RangeTombstones(func(key string, dot Dot) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			buf := []byte{opRemove}
			buf = appendVarintBytes(buf, []byte(key))
			buf = append(buf, encodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	return deltas
}
