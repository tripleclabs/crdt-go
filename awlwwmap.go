package crdt

// AWLWWMap stores key → (value, [Dot]) entries and key → (Dot, context VClock)
// tombstones, backed by a [Backend]. The tombstone context is the causal
// snapshot at time of removal — used by the replica layer for add-wins logic.
//
// AWLWWMap implements [Mergeable] for use with [Replica] and [AddWinsClock].
type AWLWWMap[V any] struct {
	codec   Codec[V]
	backend Backend
}

// NewAWLWWMap returns an initialized AWLWWMap. Use [NewAWLWWMapReplica] to
// create a fully wired Replica.
func NewAWLWWMap[V any](codec Codec[V], opts ...Option) *AWLWWMap[V] {
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = NewMemoryBackend()
	}
	return &AWLWWMap[V]{codec: codec, backend: b}
}

// NewAWLWWMapReplica creates a [Replica] wrapping an [AWLWWMap] with [AddWinsClock].
func NewAWLWWMapReplica[V any](replicaID ReplicaID, codec Codec[V], opts ...Option) *Replica[*AWLWWMap[V]] {
	return NewReplica[*AWLWWMap[V]](replicaID, NewAWLWWMap(codec, opts...), AddWinsClock{})
}

// --- Mutations (return delta bytes) ---

// Put stores a key-value pair with the given dot. Removes any tombstone for
// the key. Returns the encoded delta to send to peers.
//
// Delta format: [op=0x01][varint key len][key][varint val len][val][16 byte dot]
func (m *AWLWWMap[V]) Put(key string, value V, dot Dot) ([]byte, error) {
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
func (m *AWLWWMap[V]) PutBytes(key string, valBytes []byte, dot Dot) {
	m.backend.PutEntry(key, valBytes, EncodeDot(dot))
	m.backend.DeleteTombstone(key)
}

// Remove tombstones a key with the given dot and causal context. Removes any
// entry for the key. Returns the encoded delta to send to peers.
//
// Delta format: [op=0x02][varint key len][key][16 byte dot][encoded vclock]
func (m *AWLWWMap[V]) Remove(key string, dot Dot, context VClock) []byte {
	m.backend.DeleteEntry(key)
	m.backend.PutTombstone(key, encodeAWTombstone(dot, context))

	buf := []byte{OpRemove}
	buf = AppendVarintBytes(buf, []byte(key))
	buf = append(buf, EncodeDot(dot)...)
	buf = append(buf, EncodeVClock(context)...)
	return buf
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

// TombstoneLen returns the number of tombstones.
func (m *AWLWWMap[V]) TombstoneLen() int { return m.backend.TombstoneLen() }

// --- Queryable ---

// EntryMeta returns the encoded dot for the entry at key.
func (m *AWLWWMap[V]) EntryMeta(key string) ([]byte, bool) {
	_, meta, ok := m.backend.GetEntry(key)
	return meta, ok
}

// TombstoneMeta returns the full tombstone meta (dot + vclock concatenated).
func (m *AWLWWMap[V]) TombstoneMeta(key string) ([]byte, bool) {
	return m.backend.GetTombstone(key)
}

// --- Mergeable ---

// ParseDelta extracts a [DeltaInfo] from an encoded AWLWWMap delta.
func (m *AWLWWMap[V]) ParseDelta(delta []byte) (DeltaInfo, error) {
	if len(delta) < 1 {
		return DeltaInfo{}, ErrShortBuffer
	}
	switch delta[0] {
	case OpPut:
		return m.parsePutDelta(delta[1:])
	case OpRemove:
		return m.parseRemoveDelta(delta[1:])
	default:
		return DeltaInfo{}, ErrUnknownOp
	}
}

func (m *AWLWWMap[V]) parsePutDelta(data []byte) (DeltaInfo, error) {
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
	dot, _ := DecodeDot(data[off:])
	return DeltaInfo{
		Op:   OpPut,
		Key:  string(keyBytes),
		Meta: data[off : off+16],
		Dots: []Dot{dot},
	}, nil
}

func (m *AWLWWMap[V]) parseRemoveDelta(data []byte) (DeltaInfo, error) {
	keyBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return DeltaInfo{}, err
	}
	if off+16 > len(data) {
		return DeltaInfo{}, ErrShortBuffer
	}
	dot, _ := DecodeDot(data[off:])
	// Context is everything after the 16-byte dot.
	context := data[off+16:]
	return DeltaInfo{
		Op:      OpRemove,
		Key:     string(keyBytes),
		Meta:    data[off : off+16],
		Context: context,
		Dots:    []Dot{dot},
	}, nil
}

// Apply unconditionally merges a delta into the AWLWWMap. The caller must
// ensure the clock has already approved the delta.
func (m *AWLWWMap[V]) Apply(delta []byte) error {
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

func (m *AWLWWMap[V]) applyPut(data []byte) error {
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
	remoteDot, _ := DecodeDot(data[off:])
	m.PutBytes(string(keyBytes), valBytes, remoteDot)
	return nil
}

func (m *AWLWWMap[V]) applyRemove(data []byte) error {
	keyBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return err
	}
	if off+16 > len(data) {
		return ErrShortBuffer
	}
	remoteDot, _ := DecodeDot(data[off:])
	off += 16
	remoteCtx, err := DecodeVClock(data[off:])
	if err != nil {
		return err
	}
	m.Remove(string(keyBytes), remoteDot, remoteCtx)
	return nil
}

// DeltasSince returns encoded deltas for entries and tombstones with dots
// not covered by peerHWM.
func (m *AWLWWMap[V]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte

	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		dot, _ := DecodeDot(metaBytes)
		if dot.Counter > peerHWM.Get(dot.Replica) {
			buf := []byte{OpPut}
			buf = AppendVarintBytes(buf, []byte(key))
			buf = AppendVarintBytes(buf, valBytes)
			buf = append(buf, EncodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	m.RangeTombstones(func(key string, dot Dot, ctx VClock) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			buf := []byte{OpRemove}
			buf = AppendVarintBytes(buf, []byte(key))
			buf = append(buf, EncodeDot(dot)...)
			buf = append(buf, EncodeVClock(ctx)...)
			deltas = append(deltas, buf)
		}
		return true
	})

	return deltas
}

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
