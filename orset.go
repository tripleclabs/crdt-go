package crdt

// ORSet stores element → [DotMap] entries, backed by a [Backend].
// Elements are encoded to backend keys via the provided [Codec].
// Each element's DotMap tracks which replicas added it and when.
//
// This is pure storage — no clocks, no merge logic, no delta encoding.
type ORSet[E any] struct {
	codec   Codec[E]
	backend Backend
}

// NewORSet returns an initialized ORSet.
func NewORSet[E any](codec Codec[E], opts ...Option) *ORSet[E] {
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = NewMemoryBackend()
	}
	return &ORSet[E]{codec: codec, backend: b}
}

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

// Remove removes an element.
func (s *ORSet[E]) Remove(elem E) error {
	elemBytes, err := s.codec.Encode(elem)
	if err != nil {
		return err
	}
	s.backend.DeleteEntry(string(elemBytes))
	return nil
}

// RemoveEncoded removes an already-encoded element key.
func (s *ORSet[E]) RemoveEncoded(elemKey string) {
	s.backend.DeleteEntry(elemKey)
}

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
	dm, _ := DecodeDotMap(metaBytes)
	return dm, true
}

// GetEncoded returns the dotmap for an encoded element key.
func (s *ORSet[E]) GetEncoded(elemKey string) (DotMap, bool) {
	_, metaBytes, ok := s.backend.GetEntry(elemKey)
	if !ok {
		return nil, false
	}
	dm, _ := DecodeDotMap(metaBytes)
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
		dm, _ := DecodeDotMap(metaBytes)
		return fn(key, dm)
	})
}

// Len returns the number of elements.
func (s *ORSet[E]) Len() int { return s.backend.EntryLen() }
