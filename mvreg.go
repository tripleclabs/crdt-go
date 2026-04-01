package crdt

// MVRegister stores multiple concurrent values, each with a [Dot]. When
// values are written concurrently without synchronization, all values are
// preserved. The replica layer decides which values survive on merge.
//
// This is pure storage — no clocks, no merge logic.
type MVRegister[V any] struct {
	codec   Codec[V]
	entries []mvEntry
}

type mvEntry struct {
	valBytes []byte
	dot      Dot
}

// NewMVRegister returns an initialized MVRegister.
func NewMVRegister[V any](codec Codec[V]) *MVRegister[V] {
	return &MVRegister[V]{codec: codec}
}

// Set replaces all entries with a single value and dot.
func (r *MVRegister[V]) Set(value V, dot Dot) error {
	b, err := r.codec.Encode(value)
	if err != nil {
		return err
	}
	r.entries = []mvEntry{{valBytes: b, dot: dot}}
	return nil
}

// SetEntries replaces all entries with the given (valBytes, dot) pairs.
// Used by the replica layer after merge to write back surviving values.
func (r *MVRegister[V]) SetEntries(entries []struct {
	ValBytes []byte
	Dot      Dot
}) {
	r.entries = make([]mvEntry, len(entries))
	for i, e := range entries {
		r.entries[i] = mvEntry{valBytes: e.ValBytes, dot: e.Dot}
	}
}

// Values returns all current values.
func (r *MVRegister[V]) Values() ([]V, error) {
	out := make([]V, 0, len(r.entries))
	for _, e := range r.entries {
		v, err := r.codec.Decode(e.valBytes)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// RangeEntries calls fn for each (valBytes, dot) entry.
func (r *MVRegister[V]) RangeEntries(fn func(valBytes []byte, dot Dot) bool) {
	for _, e := range r.entries {
		if !fn(e.valBytes, e.dot) {
			return
		}
	}
}

// Len returns the number of concurrent values.
func (r *MVRegister[V]) Len() int { return len(r.entries) }
