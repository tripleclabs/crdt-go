package crdt

// awLWWMapState stores key → (value, [Dot]) entries and key → (Dot, context VClock)
// tombstones, backed by a [Backend]. The tombstone context is the causal
// snapshot at time of removal — used by the replica layer for add-wins logic.
//
// awLWWMapState implements [mergeable] for use with [replica] and [addWinsClock].
type awLWWMapState[V any] struct {
	codec   Codec[V]
	backend Backend
}

// newAWLWWMapState returns an initialized AWLWWMap. Use [newAWLWWMapReplica] to
// create a fully wired Replica.
func newAWLWWMapState[V any](codec Codec[V], opts ...Option) *awLWWMapState[V] {
	requireCodec(codec)
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = newMemoryBackend()
	}
	return &awLWWMapState[V]{codec: codec, backend: b}
}

// --- Mutations (return delta bytes) ---

// Put stores a key-value pair with the given dot. Removes any tombstone for
// the key. Returns the encoded delta to send to peers.
//
// Delta format: [op=0x01][varint key len][key][varint val len][val][16 byte dot]
func (m *awLWWMapState[V]) Put(key string, value V, dot Dot) ([]byte, error) {
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
func (m *awLWWMapState[V]) PutBytes(key string, valBytes []byte, dot Dot) {
	m.backend.PutEntry(key, valBytes, encodeDot(dot))
	m.backend.DeleteTombstone(key)
}

// Remove tombstones a key with the given dot and causal context. Removes any
// entry for the key. Returns the encoded delta to send to peers.
//
// Delta format: [op=0x02][varint key len][key][16 byte dot][encoded vclock]
func (m *awLWWMapState[V]) Remove(key string, dot Dot, context VClock) []byte {
	m.backend.DeleteEntry(key)
	m.backend.PutTombstone(key, encodeAWTombstone(dot, context))

	buf := []byte{opRemove}
	buf = appendVarintBytes(buf, []byte(key))
	buf = append(buf, encodeDot(dot)...)
	buf = append(buf, encodeVClock(context)...)
	return buf
}

// Get returns the value, its dot, and whether the key exists.
func (m *awLWWMapState[V]) Get(key string) (V, Dot, bool) {
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

// GetBytes returns raw value bytes, dot, and whether exists.
func (m *awLWWMapState[V]) GetBytes(key string) ([]byte, Dot, bool) {
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

// GetTombstone returns the dot and causal context for a tombstoned key.
func (m *awLWWMapState[V]) GetTombstone(key string) (Dot, VClock, bool) {
	metaBytes, ok := m.backend.GetTombstone(key)
	if !ok {
		return Dot{}, nil, false
	}
	dot, ctx, err := decodeAWTombstone(metaBytes)
	if err != nil {
		return Dot{}, nil, false
	}
	return dot, ctx, true
}

// Range calls fn for each live entry.
func (m *awLWWMapState[V]) Range(fn func(key string, value V, dot Dot) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		v, err := m.codec.Decode(valBytes)
		if err != nil {
			return true
		}
		dot, err := decodeDot(metaBytes)
		if err != nil {
			return true
		}
		return fn(key, v, dot)
	})
}

// RangeTombstones calls fn for each tombstone.
func (m *awLWWMapState[V]) RangeTombstones(fn func(key string, dot Dot, context VClock) bool) {
	m.backend.RangeTombstones(func(key string, metaBytes []byte) bool {
		dot, ctx, err := decodeAWTombstone(metaBytes)
		if err != nil {
			return true
		}
		return fn(key, dot, ctx)
	})
}

// Len returns the number of live entries.
func (m *awLWWMapState[V]) Len() int { return m.backend.EntryLen() }

// TombstoneLen returns the number of tombstones.
func (m *awLWWMapState[V]) TombstoneLen() int { return m.backend.TombstoneLen() }

// --- Queryable ---

// EntryMeta returns the encoded dot for the entry at key.
func (m *awLWWMapState[V]) EntryMeta(key string) ([]byte, bool) {
	_, meta, ok := m.backend.GetEntry(key)
	return meta, ok
}

// TombstoneMeta returns the full tombstone meta (dot + vclock concatenated).
func (m *awLWWMapState[V]) TombstoneMeta(key string) ([]byte, bool) {
	return m.backend.GetTombstone(key)
}

// --- Mergeable ---

// ParseDelta extracts a [deltaInfo] from an encoded AWLWWMap delta.
func (m *awLWWMapState[V]) ParseDelta(delta []byte) (deltaInfo, error) {
	if len(delta) < 1 {
		return deltaInfo{}, errShortBuffer
	}
	switch delta[0] {
	case opPut:
		return m.parsePutDelta(delta[1:])
	case opRemove:
		return m.parseRemoveDelta(delta[1:])
	default:
		return deltaInfo{}, errUnknownOp
	}
}

func (m *awLWWMapState[V]) parsePutDelta(data []byte) (deltaInfo, error) {
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

func (m *awLWWMapState[V]) parseRemoveDelta(data []byte) (deltaInfo, error) {
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
	// Context is everything after the 16-byte dot.
	context := data[off+16:]
	return deltaInfo{
		Op:      opRemove,
		Key:     string(keyBytes),
		Meta:    data[off : off+16],
		Context: context,
		Dots:    []Dot{dot},
	}, nil
}

// Apply unconditionally merges a delta into the AWLWWMap. The caller must
// ensure the clock has already approved the delta.
func (m *awLWWMapState[V]) Apply(delta []byte) error {
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

func (m *awLWWMapState[V]) applyPut(data []byte) error {
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

func (m *awLWWMapState[V]) applyRemove(data []byte) error {
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
	off += 16
	remoteCtx, err := decodeVClock(data[off:])
	if err != nil {
		return err
	}
	m.Remove(string(keyBytes), remoteDot, remoteCtx)
	return nil
}

// DeltasSince returns encoded deltas for entries and tombstones with dots
// not covered by peerHWM.
func (m *awLWWMapState[V]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte

	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		dot, err := decodeDot(metaBytes)
		if err != nil {
			return true
		}
		if dot.Counter > peerHWM.Get(dot.Replica) {
			buf := []byte{opPut}
			buf = appendVarintBytes(buf, []byte(key))
			buf = appendVarintBytes(buf, valBytes)
			buf = append(buf, encodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	m.RangeTombstones(func(key string, dot Dot, ctx VClock) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			buf := []byte{opRemove}
			buf = appendVarintBytes(buf, []byte(key))
			buf = append(buf, encodeDot(dot)...)
			buf = append(buf, encodeVClock(ctx)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	return deltas
}

func encodeAWTombstone(d Dot, ctx VClock) []byte {
	dotBytes := encodeDot(d)
	ctxBytes := encodeVClock(ctx)
	out := make([]byte, len(dotBytes)+len(ctxBytes))
	copy(out, dotBytes)
	copy(out[len(dotBytes):], ctxBytes)
	return out
}

func decodeAWTombstone(b []byte) (Dot, VClock, error) {
	d, err := decodeDot(b)
	if err != nil {
		return Dot{}, nil, err
	}
	ctx, err := decodeVClock(b[16:])
	if err != nil {
		return Dot{}, nil, err
	}
	return d, ctx, nil
}
