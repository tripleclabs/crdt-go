package crdt

// ORSet stores element → [DotMap] entries, backed by a [Backend].
// Elements are encoded to backend keys via the provided [Codec].
// Each element's DotMap tracks which replicas added it and when.
//
// ORSet implements [Mergeable] for use with [Replica] and [AlwaysMergeClock].
type ORSet[E any] struct {
	codec   Codec[E]
	backend Backend
}

// NewORSet returns an initialized ORSet.
func NewORSet[E any](codec Codec[E], opts ...Option) *ORSet[E] {
	requireCodec(codec)
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = NewMemoryBackend()
	}
	return &ORSet[E]{codec: codec, backend: b}
}

// NewORSetReplica creates a [Replica] wrapping an [ORSet] with
// [AlwaysMergeClock].
func NewORSetReplica[E any](replicaID ReplicaID, codec Codec[E], opts ...Option) *Replica[*ORSet[E]] {
	return NewReplica[*ORSet[E]](replicaID, NewORSet(codec, opts...), AlwaysMergeClock{})
}

// --- Mutations (return delta bytes) ---

// Add adds an element with the given dot. If the element already exists, the
// dot is merged into its existing DotMap. Returns the encoded delta to send to
// peers.
//
// Delta format: [op=0x01][varint elem len][elem bytes][encoded dotmap]
// The delta dotmap contains only the NEW dot (not the combined dotmap).
func (s *ORSet[E]) Add(elem E, dot Dot) ([]byte, error) {
	elemBytes, err := s.codec.Encode(elem)
	if err != nil {
		return nil, err
	}
	elemKey := string(elemBytes)

	// Combine new dot with existing dots in local state.
	dots := DotMap{dot.Replica: dot.Counter}
	if existing, ok := s.GetEncoded(elemKey); ok {
		for rep, c := range existing {
			dots[rep] = c
		}
		dots[dot.Replica] = dot.Counter
	}
	s.PutEncoded(elemKey, dots)

	// Delta carries only the new dot.
	deltaDots := DotMap{dot.Replica: dot.Counter}
	buf := []byte{OpPut}
	buf = AppendVarintBytes(buf, elemBytes)
	buf = append(buf, EncodeDotMap(deltaDots)...)
	return buf, nil
}

// Remove removes an element and returns the encoded delta. The context
// parameter is the local received HWM — the receiver uses it to determine
// which dots the remover had observed.
//
// Delta format: [op=0x02][varint elem len][elem bytes][encoded vclock]
func (s *ORSet[E]) Remove(elem E, context VClock) ([]byte, error) {
	elemBytes, err := s.codec.Encode(elem)
	if err != nil {
		return nil, err
	}
	s.RemoveEncoded(string(elemBytes))

	buf := []byte{OpRemove}
	buf = AppendVarintBytes(buf, elemBytes)
	buf = append(buf, EncodeVClock(context)...)
	return buf, nil
}

// --- Internal mutators (used by Apply) ---

// Put stores an element with the given dotmap.
func (s *ORSet[E]) Put(elem E, dots DotMap) error {
	elemBytes, err := s.codec.Encode(elem)
	if err != nil {
		return err
	}
	s.backend.PutEntry(string(elemBytes), nil, EncodeDotMap(dots))
	return nil
}

// PutEncoded stores an already-encoded element key with the given dotmap.
func (s *ORSet[E]) PutEncoded(elemKey string, dots DotMap) {
	s.backend.PutEntry(elemKey, nil, EncodeDotMap(dots))
}

// RemoveEncoded removes an already-encoded element key.
func (s *ORSet[E]) RemoveEncoded(elemKey string) {
	s.backend.DeleteEntry(elemKey)
}

// --- Reads ---

// Get returns the dotmap for an element and whether it exists.
func (s *ORSet[E]) Get(elem E) (DotMap, bool) {
	elemBytes, err := s.codec.Encode(elem)
	if err != nil {
		return nil, false
	}
	_, metaBytes, ok := s.backend.GetEntry(string(elemBytes))
	if !ok {
		return nil, false
	}
	dm, err := DecodeDotMap(metaBytes)
	if err != nil {
		return nil, false
	}
	return dm, true
}

// GetEncoded returns the dotmap for an encoded element key.
func (s *ORSet[E]) GetEncoded(elemKey string) (DotMap, bool) {
	_, metaBytes, ok := s.backend.GetEntry(elemKey)
	if !ok {
		return nil, false
	}
	dm, err := DecodeDotMap(metaBytes)
	if err != nil {
		return nil, false
	}
	return dm, true
}

// Contains reports whether an element is in the set.
func (s *ORSet[E]) Contains(elem E) bool {
	elemBytes, err := s.codec.Encode(elem)
	if err != nil {
		return false
	}
	_, _, ok := s.backend.GetEntry(string(elemBytes))
	return ok
}

// Elements returns all elements in the set.
func (s *ORSet[E]) Elements() ([]E, error) {
	out := make([]E, 0, s.backend.EntryLen())
	var decErr error
	s.backend.RangeEntries(func(key string, _ []byte, _ []byte) bool {
		v, err := s.codec.Decode([]byte(key))
		if err != nil {
			decErr = err
			return false
		}
		out = append(out, v)
		return true
	})
	return out, decErr
}

// Range calls fn for each (encoded element key, dotmap) pair.
func (s *ORSet[E]) Range(fn func(elemKey string, dots DotMap) bool) {
	s.backend.RangeEntries(func(key string, _ []byte, metaBytes []byte) bool {
		dm, err := DecodeDotMap(metaBytes)
		if err != nil {
			return true // skip corrupt entry
		}
		return fn(key, dm)
	})
}

// Len returns the number of elements.
func (s *ORSet[E]) Len() int { return s.backend.EntryLen() }

// --- Queryable ---

// EntryMeta returns the encoded dotmap for the element at key.
func (s *ORSet[E]) EntryMeta(key string) ([]byte, bool) {
	_, meta, ok := s.backend.GetEntry(key)
	return meta, ok
}

// TombstoneMeta always returns false — ORSet does not use tombstones.
func (s *ORSet[E]) TombstoneMeta(key string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [DeltaInfo] from an encoded ORSet delta.
func (s *ORSet[E]) ParseDelta(delta []byte) (DeltaInfo, error) {
	if len(delta) < 1 {
		return DeltaInfo{}, ErrShortBuffer
	}
	switch delta[0] {
	case OpPut:
		return s.parseAddDelta(delta[1:])
	case OpRemove:
		return s.parseRemoveDelta(delta[1:])
	default:
		return DeltaInfo{}, ErrUnknownOp
	}
}

func (s *ORSet[E]) parseAddDelta(data []byte) (DeltaInfo, error) {
	elemBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return DeltaInfo{}, err
	}
	remoteDots, err := DecodeDotMap(data[off:])
	if err != nil {
		return DeltaInfo{}, err
	}

	// Collect all dots from the remote dotmap.
	dots := make([]Dot, 0, len(remoteDots))
	for rep, counter := range remoteDots {
		dots = append(dots, Dot{Replica: rep, Counter: counter})
	}

	return DeltaInfo{
		Op:   OpPut,
		Key:  string(elemBytes),
		Meta: data[off:],
		Dots: dots,
	}, nil
}

func (s *ORSet[E]) parseRemoveDelta(data []byte) (DeltaInfo, error) {
	elemBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return DeltaInfo{}, err
	}
	// Removes don't carry dots to record.
	return DeltaInfo{
		Op:   OpRemove,
		Key:  string(elemBytes),
		Meta: data[off:],
		Dots: nil,
	}, nil
}

// Apply unconditionally merges a delta into the ORSet. The caller must
// ensure the clock has already approved the delta.
func (s *ORSet[E]) Apply(delta []byte) error {
	if len(delta) < 1 {
		return ErrShortBuffer
	}
	switch delta[0] {
	case OpPut:
		return s.applyAdd(delta[1:])
	case OpRemove:
		return s.applyRemove(delta[1:])
	default:
		return ErrUnknownOp
	}
}

func (s *ORSet[E]) applyAdd(data []byte) error {
	elemBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return err
	}
	remoteDots, err := DecodeDotMap(data[off:])
	if err != nil {
		return err
	}
	elemKey := string(elemBytes)

	// Combine with existing dots.
	if localDots, ok := s.GetEncoded(elemKey); ok {
		remoteDots = CombineDots(localDots, remoteDots)
	}
	s.PutEncoded(elemKey, remoteDots)
	return nil
}

func (s *ORSet[E]) applyRemove(data []byte) error {
	elemBytes, off, err := ReadVarintBytes(data, 0)
	if err != nil {
		return err
	}
	removeVC, err := DecodeVClock(data[off:])
	if err != nil {
		return err
	}
	elemKey := string(elemBytes)

	localDots, ok := s.GetEncoded(elemKey)
	if !ok {
		return nil
	}

	// Keep dots NOT dominated by the remover's causal context.
	surviving := make(DotMap)
	for rep, counter := range localDots {
		if counter > removeVC.Get(rep) {
			surviving[rep] = counter
		}
	}

	if len(surviving) > 0 {
		s.PutEncoded(elemKey, surviving)
	} else {
		s.RemoveEncoded(elemKey)
	}
	return nil
}

// DeltasSince returns add deltas for elements with any dot not covered
// by peerHWM.
func (s *ORSet[E]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte
	s.Range(func(elemKey string, dots DotMap) bool {
		for rep, counter := range dots {
			if counter > peerHWM.Get(rep) {
				buf := []byte{OpPut}
				buf = AppendVarintBytes(buf, []byte(elemKey))
				buf = append(buf, EncodeDotMap(dots)...)
				deltas = append(deltas, buf)
				break
			}
		}
		return true
	})
	return deltas
}
