package crdt

// orMapState stores key → (value, [DotMap]) entries, backed by a [Backend].
// Each key's DotMap tracks which replicas contributed the entry.
//
// orMapState implements [mergeable] for use with [replica] and [alwaysMergeClock].
type orMapState[V any] struct {
	codec   Codec[V]
	backend Backend
}

// newORMapState returns an initialized ORMap.
func newORMapState[V any](codec Codec[V], opts ...Option) *orMapState[V] {
	requireCodec(codec)
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = newMemoryBackend()
	}
	return &orMapState[V]{codec: codec, backend: b}
}

// --- Mutations (return delta bytes) ---

// Put stores a key-value pair with the given dot. Combines the new dot with
// any existing dots for the key. Returns the encoded delta to send to peers.
//
// Delta format: [op=0x01][varint key len][key][varint val len][val][encoded dotmap]
// The delta dotmap carries only the new dot.
func (m *orMapState[V]) Put(key string, value V, dot Dot) ([]byte, error) {
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

	m.backend.PutEntry(key, valBytes, encodeDotMap(dots))

	// Delta carries only the new dot.
	deltaDots := DotMap{dot.Replica: dot.Counter}
	buf := []byte{opPut}
	buf = appendVarintBytes(buf, []byte(key))
	buf = appendVarintBytes(buf, valBytes)
	buf = append(buf, encodeDotMap(deltaDots)...)
	return buf, nil
}

// PutBytes stores pre-encoded value bytes with the given dotmap.
func (m *orMapState[V]) PutBytes(key string, valBytes []byte, dots DotMap) {
	m.backend.PutEntry(key, valBytes, encodeDotMap(dots))
}

// Remove removes a key, returns the encoded delta. The delta carries the
// provided causal context (typically the replica's HWM).
//
// Delta format: [op=0x02][varint key len][key][encoded vclock]
func (m *orMapState[V]) Remove(key string, ctx VClock) []byte {
	m.backend.DeleteEntry(key)

	buf := []byte{opRemove}
	buf = appendVarintBytes(buf, []byte(key))
	buf = append(buf, encodeVClock(ctx)...)
	return buf
}

// --- Reads ---

// Get returns the value, its dotmap, and whether the key exists.
func (m *orMapState[V]) Get(key string) (V, DotMap, bool) {
	var zero V
	valBytes, metaBytes, ok := m.backend.GetEntry(key)
	if !ok {
		return zero, nil, false
	}
	v, err := m.codec.Decode(valBytes)
	if err != nil {
		return zero, nil, false
	}
	dm, err := decodeDotMap(metaBytes)
	if err != nil {
		return zero, nil, false
	}
	return v, dm, true
}

// GetBytes returns raw value bytes, dotmap, and whether the key exists.
func (m *orMapState[V]) GetBytes(key string) ([]byte, DotMap, bool) {
	valBytes, metaBytes, ok := m.backend.GetEntry(key)
	if !ok {
		return nil, nil, false
	}
	dm, err := decodeDotMap(metaBytes)
	if err != nil {
		return nil, nil, false
	}
	return valBytes, dm, true
}

// Range calls fn for each entry.
func (m *orMapState[V]) Range(fn func(key string, value V, dots DotMap) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		v, err := m.codec.Decode(valBytes)
		if err != nil {
			return true
		}
		dm, err := decodeDotMap(metaBytes)
		if err != nil {
			return true // skip corrupt entry
		}
		return fn(key, v, dm)
	})
}

// RangeBytes calls fn for each entry with raw value bytes.
func (m *orMapState[V]) RangeBytes(fn func(key string, valBytes []byte, dots DotMap) bool) {
	m.backend.RangeEntries(func(key string, valBytes []byte, metaBytes []byte) bool {
		dm, err := decodeDotMap(metaBytes)
		if err != nil {
			return true // skip corrupt entry
		}
		return fn(key, valBytes, dm)
	})
}

// Len returns the number of entries.
func (m *orMapState[V]) Len() int { return m.backend.EntryLen() }

// --- Queryable ---

// EntryMeta returns the encoded dotmap metadata for the entry at key.
func (m *orMapState[V]) EntryMeta(key string) ([]byte, bool) {
	_, meta, ok := m.backend.GetEntry(key)
	return meta, ok
}

// TombstoneMeta always returns false — ORMap does not use tombstones.
func (m *orMapState[V]) TombstoneMeta(key string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [deltaInfo] from an encoded ORMap delta.
func (m *orMapState[V]) ParseDelta(delta []byte) (deltaInfo, error) {
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

func (m *orMapState[V]) parsePutDelta(data []byte) (deltaInfo, error) {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return deltaInfo{}, err
	}
	_, off, err = readVarintBytes(data, off)
	if err != nil {
		return deltaInfo{}, err
	}
	remoteDots, err := decodeDotMap(data[off:])
	if err != nil {
		return deltaInfo{}, err
	}

	var dots []Dot
	for rep, counter := range remoteDots {
		dots = append(dots, Dot{Replica: rep, Counter: counter})
	}

	return deltaInfo{
		Op:   opPut,
		Key:  string(keyBytes),
		Meta: data[off:],
		Dots: dots,
	}, nil
}

func (m *orMapState[V]) parseRemoveDelta(data []byte) (deltaInfo, error) {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return deltaInfo{}, err
	}
	vc, err := decodeVClock(data[off:])
	if err != nil {
		return deltaInfo{}, err
	}

	// Remove deltas carry a vclock context but no dots that should update
	// the received clock — the remove itself does not generate new dots.
	_ = vc
	return deltaInfo{
		Op:      opRemove,
		Key:     string(keyBytes),
		Context: data[off:],
	}, nil
}

// Apply unconditionally merges a delta into the ORMap. The caller must
// ensure the clock has already approved the delta.
func (m *orMapState[V]) Apply(delta []byte) error {
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

func (m *orMapState[V]) applyPut(data []byte) error {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	valBytes, off, err := readVarintBytes(data, off)
	if err != nil {
		return err
	}
	remoteDots, err := decodeDotMap(data[off:])
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

func (m *orMapState[V]) applyRemove(data []byte) error {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	removeVC, err := decodeVClock(data[off:])
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
func (m *orMapState[V]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte
	m.RangeBytes(func(key string, valBytes []byte, dots DotMap) bool {
		for rep, counter := range dots {
			if counter > peerHWM.Get(rep) {
				buf := []byte{opPut}
				buf = appendVarintBytes(buf, []byte(key))
				buf = appendVarintBytes(buf, valBytes)
				buf = append(buf, encodeDotMap(dots)...)
				deltas = append(deltas, buf)
				break
			}
		}
		return true
	})
	return deltas
}
