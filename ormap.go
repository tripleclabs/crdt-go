package crdt

// ORMap stores key → (value, [DotMap]) entries, backed by a [Backend].
// Each key's DotMap tracks which replicas contributed the entry.
//
// ORMap implements [Mergeable] for use with [Replica] and [AlwaysMergeClock].
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

// NewORMapReplica creates a [Replica] wrapping an [ORMap] with [AlwaysMergeClock].
func NewORMapReplica[V any](replicaID ReplicaID, codec Codec[V], opts ...Option) *Replica[*ORMap[V]] {
	return NewReplica[*ORMap[V]](replicaID, NewORMap(codec, opts...), AlwaysMergeClock{})
}

// --- Mutations (return delta bytes) ---

// Put stores a key-value pair with the given dot. Combines the new dot with
// any existing dots for the key. Returns the encoded delta to send to peers.
//
// Delta format: [op=0x01][varint key len][key][varint val len][val][encoded dotmap]
// The delta dotmap carries only the new dot.
func (m *ORMap[V]) Put(key string, value V, dot Dot) ([]byte, error) {
	valBytes, err := m.codec.Encode(value)
	if err != nil {
		return nil, err
	}

	// Combine new dot with existing dots.
	dots := DotMap{dot.Replica: dot.Counter}
	if _, existing, ok := m.GetBytes(key); ok {
		for rep, c := range existing {
			dots[rep] = c
		}
		dots[dot.Replica] = dot.Counter
	}

	m.backend.PutEntry(key, valBytes, EncodeDotMap(dots))

	// Delta carries only the new dot.
	deltaDots := DotMap{dot.Replica: dot.Counter}
	buf := []byte{OpPut}
	buf = AppendVarintBytes(buf, []byte(key))
	buf = AppendVarintBytes(buf, valBytes)
	buf = append(buf, EncodeDotMap(deltaDots)...)
	return buf, nil
}

// PutBytes stores pre-encoded value bytes with the given dotmap.
func (m *ORMap[V]) PutBytes(key string, valBytes []byte, dots DotMap) {
	m.backend.PutEntry(key, valBytes, EncodeDotMap(dots))
}

// Remove removes a key, returns the encoded delta. The delta carries the
// provided causal context (typically the replica's HWM).
//
// Delta format: [op=0x02][varint key len][key][encoded vclock]
func (m *ORMap[V]) Remove(key string, ctx VClock) []byte {
	m.backend.DeleteEntry(key)

	buf := []byte{OpRemove}
	buf = AppendVarintBytes(buf, []byte(key))
	buf = append(buf, EncodeVClock(ctx)...)
	return buf
}

// --- Reads ---

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

// --- Queryable ---

// EntryMeta returns the encoded dotmap metadata for the entry at key.
func (m *ORMap[V]) EntryMeta(key string) ([]byte, bool) {
	_, meta, ok := m.backend.GetEntry(key)
	return meta, ok
}

// TombstoneMeta always returns false — ORMap does not use tombstones.
func (m *ORMap[V]) TombstoneMeta(key string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [DeltaInfo] from an encoded ORMap delta.
func (m *ORMap[V]) ParseDelta(delta []byte) (DeltaInfo, error) {
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

func (m *ORMap[V]) parsePutDelta(data []byte) (DeltaInfo, error) {
	keyBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return DeltaInfo{}, err
	}
	_, off, err = ReadVarintBytes(data, off)
	if err != nil {
		return DeltaInfo{}, err
	}
	remoteDots, err := DecodeDotMap(data[off:])
	if err != nil {
		return DeltaInfo{}, err
	}

	var dots []Dot
	for rep, counter := range remoteDots {
		dots = append(dots, Dot{Replica: rep, Counter: counter})
	}

	return DeltaInfo{
		Op:   OpPut,
		Key:  string(keyBytes),
		Meta: data[off:],
		Dots: dots,
	}, nil
}

func (m *ORMap[V]) parseRemoveDelta(data []byte) (DeltaInfo, error) {
	keyBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return DeltaInfo{}, err
	}
	vc, err := DecodeVClock(data[off:])
	if err != nil {
		return DeltaInfo{}, err
	}

	// Remove deltas carry a vclock context but no dots that should update
	// the received clock — the remove itself does not generate new dots.
	_ = vc
	return DeltaInfo{
		Op:      OpRemove,
		Key:     string(keyBytes),
		Context: data[off:],
	}, nil
}

// Apply unconditionally merges a delta into the ORMap. The caller must
// ensure the clock has already approved the delta.
func (m *ORMap[V]) Apply(delta []byte) error {
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

func (m *ORMap[V]) applyPut(data []byte) error {
	keyBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return err
	}
	valBytes, off, err := ReadVarintBytes(data, off)
	if err != nil {
		return err
	}
	remoteDots, err := DecodeDotMap(data[off:])
	if err != nil {
		return err
	}
	key := string(keyBytes)

	// Combine with existing dots. If key exists, merge dots and keep
	// the value from the higher max-dot.
	if localVal, localDots, ok := m.GetBytes(key); ok {
		combined := CombineDots(localDots, remoteDots)
		winner := localVal
		if DotGT(MaxDot(remoteDots), MaxDot(localDots)) {
			winner = valBytes
		}
		m.PutBytes(key, winner, combined)
	} else {
		m.PutBytes(key, valBytes, remoteDots)
	}
	return nil
}

func (m *ORMap[V]) applyRemove(data []byte) error {
	keyBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return err
	}
	removeVC, err := DecodeVClock(data[off:])
	if err != nil {
		return err
	}
	key := string(keyBytes)

	_, localDots, ok := m.GetBytes(key)
	if !ok {
		return nil
	}

	surviving := make(DotMap)
	for rep, counter := range localDots {
		if counter > removeVC.Get(rep) {
			surviving[rep] = counter
		}
	}

	if len(surviving) > 0 {
		val, _, _ := m.GetBytes(key)
		m.PutBytes(key, val, surviving)
	} else {
		m.backend.DeleteEntry(key)
	}
	return nil
}

// DeltasSince returns encoded deltas for entries with dots not covered by peerHWM.
func (m *ORMap[V]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte
	m.RangeBytes(func(key string, valBytes []byte, dots DotMap) bool {
		for rep, counter := range dots {
			if counter > peerHWM.Get(rep) {
				buf := []byte{OpPut}
				buf = AppendVarintBytes(buf, []byte(key))
				buf = AppendVarintBytes(buf, valBytes)
				buf = append(buf, EncodeDotMap(dots)...)
				deltas = append(deltas, buf)
				break
			}
		}
		return true
	})
	return deltas
}
