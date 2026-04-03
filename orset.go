package crdt

// orSetState stores element → [DotMap] entries, backed by a [Backend].
// Elements are encoded to backend keys via the provided [Codec].
// Each element's DotMap tracks which replicas added it and when.
//
// orSetState implements [mergeable] for use with [replica] and [alwaysMergeClock].
type orSetState[E any] struct {
	codec   Codec[E]
	backend Backend
}

// newORSetState returns an initialized ORSet.
func newORSetState[E any](codec Codec[E], opts ...Option) *orSetState[E] {
	requireCodec(codec)
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = newMemoryBackend()
	}
	return &orSetState[E]{codec: codec, backend: b}
}

// --- Mutations (return delta bytes) ---

// Add adds an element with the given dot. If the element already exists, the
// dot is merged into its existing DotMap. Returns the encoded delta to send to
// peers.
//
// Delta format: [op=0x01][varint elem len][elem bytes][encoded dotmap]
// The delta dotmap contains only the NEW dot (not the combined dotmap).
func (s *orSetState[E]) Add(elem E, dot Dot) ([]byte, error) {
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
	buf := []byte{opPut}
	buf = appendVarintBytes(buf, elemBytes)
	buf = append(buf, encodeDotMap(deltaDots)...)
	return buf, nil
}

// Remove removes an element and returns the encoded delta. The context
// parameter is the local received HWM — the receiver uses it to determine
// which dots the remover had observed.
//
// Delta format: [op=0x02][varint elem len][elem bytes][16-byte dot][encoded vclock]
func (s *orSetState[E]) Remove(elem E, dot Dot, context VClock) ([]byte, error) {
	elemBytes, err := s.codec.Encode(elem)
	if err != nil {
		return nil, err
	}
	elemKey := string(elemBytes)
	s.RemoveEncoded(elemKey)
	s.backend.PutTombstone(elemKey, encodeRemoveTombstone(dot, context))

	buf := []byte{opRemove}
	buf = appendVarintBytes(buf, elemBytes)
	buf = append(buf, encodeDot(dot)...)
	buf = append(buf, encodeVClock(context)...)
	return buf, nil
}

// --- Internal mutators (used by Apply) ---

// Put stores an element with the given dotmap.
func (s *orSetState[E]) Put(elem E, dots DotMap) error {
	elemBytes, err := s.codec.Encode(elem)
	if err != nil {
		return err
	}
	s.backend.PutEntry(string(elemBytes), nil, encodeDotMap(dots))
	return nil
}

// PutEncoded stores an already-encoded element key with the given dotmap.
func (s *orSetState[E]) PutEncoded(elemKey string, dots DotMap) {
	s.backend.PutEntry(elemKey, nil, encodeDotMap(dots))
}

// RemoveEncoded removes an already-encoded element key.
func (s *orSetState[E]) RemoveEncoded(elemKey string) {
	s.backend.DeleteEntry(elemKey)
}

// --- Reads ---

// Get returns the dotmap for an element and whether it exists.
func (s *orSetState[E]) Get(elem E) (DotMap, bool) {
	elemBytes, err := s.codec.Encode(elem)
	if err != nil {
		return nil, false
	}
	_, metaBytes, ok := s.backend.GetEntry(string(elemBytes))
	if !ok {
		return nil, false
	}
	dm, err := decodeDotMap(metaBytes)
	if err != nil {
		return nil, false
	}
	return dm, true
}

// GetEncoded returns the dotmap for an encoded element key.
func (s *orSetState[E]) GetEncoded(elemKey string) (DotMap, bool) {
	_, metaBytes, ok := s.backend.GetEntry(elemKey)
	if !ok {
		return nil, false
	}
	dm, err := decodeDotMap(metaBytes)
	if err != nil {
		return nil, false
	}
	return dm, true
}

// Contains reports whether an element is in the set.
func (s *orSetState[E]) Contains(elem E) bool {
	elemBytes, err := s.codec.Encode(elem)
	if err != nil {
		return false
	}
	_, _, ok := s.backend.GetEntry(string(elemBytes))
	return ok
}

// Elements returns all elements in the set.
func (s *orSetState[E]) Elements() ([]E, error) {
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
func (s *orSetState[E]) Range(fn func(elemKey string, dots DotMap) bool) {
	s.backend.RangeEntries(func(key string, _ []byte, metaBytes []byte) bool {
		dm, err := decodeDotMap(metaBytes)
		if err != nil {
			return true // skip corrupt entry
		}
		return fn(key, dm)
	})
}

// Len returns the number of elements.
func (s *orSetState[E]) Len() int { return s.backend.EntryLen() }

// --- Queryable ---

// EntryMeta returns the encoded dotmap for the element at key.
func (s *orSetState[E]) EntryMeta(key string) ([]byte, bool) {
	_, meta, ok := s.backend.GetEntry(key)
	return meta, ok
}

// TombstoneMeta returns the encoded tombstone for the element at key.
func (s *orSetState[E]) TombstoneMeta(key string) ([]byte, bool) {
	return s.backend.GetTombstone(key)
}

// --- Mergeable ---

// ParseDelta extracts a [deltaInfo] from an encoded ORSet delta.
func (s *orSetState[E]) ParseDelta(delta []byte) (deltaInfo, error) {
	if len(delta) < 1 {
		return deltaInfo{}, errShortBuffer
	}
	switch delta[0] {
	case opPut:
		return s.parseAddDelta(delta[1:])
	case opRemove:
		return s.parseRemoveDelta(delta[1:])
	default:
		return deltaInfo{}, errUnknownOp
	}
}

func (s *orSetState[E]) parseAddDelta(data []byte) (deltaInfo, error) {
	elemBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return deltaInfo{}, err
	}
	remoteDots, err := decodeDotMap(data[off:])
	if err != nil {
		return deltaInfo{}, err
	}

	// Collect all dots from the remote dotmap.
	dots := make([]Dot, 0, len(remoteDots))
	for rep, counter := range remoteDots {
		dots = append(dots, Dot{Replica: rep, Counter: counter})
	}

	return deltaInfo{
		Op:   opPut,
		Key:  string(elemBytes),
		Meta: data[off:],
		Dots: dots,
	}, nil
}

func (s *orSetState[E]) parseRemoveDelta(data []byte) (deltaInfo, error) {
	elemBytes, off, err := readVarintBytes(data, 0)
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
		Key:  string(elemBytes),
		Meta: data[off : off+16],
		Dots: []Dot{dot},
	}, nil
}

// Apply unconditionally merges a delta into the ORSet. The caller must
// ensure the clock has already approved the delta.
func (s *orSetState[E]) Apply(delta []byte) error {
	if len(delta) < 1 {
		return errShortBuffer
	}
	switch delta[0] {
	case opPut:
		return s.applyAdd(delta[1:])
	case opRemove:
		return s.applyRemove(delta[1:])
	default:
		return errUnknownOp
	}
}

func (s *orSetState[E]) applyAdd(data []byte) error {
	elemBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	remoteDots, err := decodeDotMap(data[off:])
	if err != nil {
		return err
	}
	elemKey := string(elemBytes)

	// Combine with existing dots.
	if localDots, ok := s.GetEncoded(elemKey); ok {
		remoteDots = CombineDots(localDots, remoteDots)
	}

	// Prune against existing tombstone (add-wins: only dots NOT
	// dominated by the tombstone's context survive).
	if tombMeta, ok := s.backend.GetTombstone(elemKey); ok {
		_, tombCtx, err := decodeRemoveTombstone(tombMeta)
		if err == nil {
			surviving := make(DotMap)
			for rep, counter := range remoteDots {
				if counter > tombCtx.Get(rep) {
					surviving[rep] = counter
				}
			}
			remoteDots = surviving
		}
	}

	if len(remoteDots) > 0 {
		s.PutEncoded(elemKey, remoteDots)
	}
	return nil
}

func (s *orSetState[E]) applyRemove(data []byte) error {
	elemBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	if off+16 > len(data) {
		return errShortBuffer
	}
	dot, err := decodeDot(data[off:])
	if err != nil {
		return err
	}
	removeVC, err := decodeVClock(data[off+16:])
	if err != nil {
		return err
	}
	elemKey := string(elemBytes)

	// Store/merge tombstone.
	if existingMeta, ok := s.backend.GetTombstone(elemKey); ok {
		existingDot, existingCtx, err := decodeRemoveTombstone(existingMeta)
		if err == nil {
			if !DotGT(dot, existingDot) {
				dot = existingDot
			}
			removeVC.Merge(existingCtx)
		}
	}
	s.backend.PutTombstone(elemKey, encodeRemoveTombstone(dot, removeVC))

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

// DeltasSince returns deltas for entries and tombstones with dots not
// covered by peerHWM.
func (s *orSetState[E]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte
	s.Range(func(elemKey string, dots DotMap) bool {
		for rep, counter := range dots {
			if counter > peerHWM.Get(rep) {
				buf := []byte{opPut}
				buf = appendVarintBytes(buf, []byte(elemKey))
				buf = append(buf, encodeDotMap(dots)...)
				deltas = append(deltas, buf)
				break
			}
		}
		return true
	})
	s.backend.RangeTombstones(func(key string, meta []byte) bool {
		dot, ctx, err := decodeRemoveTombstone(meta)
		if err != nil {
			return true
		}
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

// GCTombstones removes tombstones whose dots are fully covered by minHWM.
func (s *orSetState[E]) GCTombstones(minHWM VClock) {
	var toDelete []string
	s.backend.RangeTombstones(func(key string, meta []byte) bool {
		dot, _, err := decodeRemoveTombstone(meta)
		if err != nil {
			return true
		}
		if dot.Counter <= minHWM.Get(dot.Replica) {
			toDelete = append(toDelete, key)
		}
		return true
	})
	for _, key := range toDelete {
		s.backend.DeleteTombstone(key)
	}
}
