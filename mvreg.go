package crdt

// MVRegister stores multiple concurrent values, each with a [Dot]. When
// values are written concurrently without synchronization, all values are
// preserved until a subsequent write resolves the conflict.
//
// MVRegister implements [Mergeable] for use with [Replica] and [AlwaysMergeClock].
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

// NewMVRegisterReplica creates a [Replica] wrapping an [MVRegister] with [AlwaysMergeClock].
func NewMVRegisterReplica[V any](replicaID ReplicaID, codec Codec[V]) *Replica[*MVRegister[V]] {
	return NewReplica[*MVRegister[V]](replicaID, NewMVRegister(codec), AlwaysMergeClock{})
}

// --- Mutations ---

// Write sets a value (clearing all concurrent values), stamps it with the
// given dot, and returns the encoded delta. The context (received HWM) is
// included so receivers can prune superseded values.
//
// Delta format: [varint val len][val bytes][16 byte dot][encoded vclock context]
func (r *MVRegister[V]) Write(value V, dot Dot, context VClock) ([]byte, error) {
	b, err := r.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	r.entries = []mvEntry{{valBytes: b, dot: dot}}

	var buf []byte
	buf = AppendVarintBytes(buf, b)
	buf = append(buf, EncodeDot(dot)...)
	buf = append(buf, EncodeVClock(context)...)
	return buf, nil
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
func (r *MVRegister[V]) SetEntries(entries []struct {
	ValBytes []byte
	Dot      Dot
}) {
	r.entries = make([]mvEntry, len(entries))
	for i, e := range entries {
		r.entries[i] = mvEntry{valBytes: e.ValBytes, dot: e.Dot}
	}
}

// --- Reads ---

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

// --- Queryable ---

// EntryMeta returns false — MVRegister uses AlwaysMerge, so this is never called.
func (r *MVRegister[V]) EntryMeta(string) ([]byte, bool) {
	return nil, false
}

// TombstoneMeta returns false — MVRegister has no tombstones.
func (r *MVRegister[V]) TombstoneMeta(string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [DeltaInfo] from an MVRegister delta.
func (r *MVRegister[V]) ParseDelta(delta []byte) (DeltaInfo, error) {
	_, off, err := ReadVarintBytes(delta, 0)
	if err != nil {
		return DeltaInfo{}, err
	}
	if off+16 > len(delta) {
		return DeltaInfo{}, ErrShortBuffer
	}
	dot, _ := DecodeDot(delta[off:])
	return DeltaInfo{
		Key:  "",
		Meta: delta[off : off+16],
		Dots: []Dot{dot},
	}, nil
}

// Apply merges an incoming delta. Local values whose dots are covered by
// the remote's context are pruned. The remote value is added if its dot is
// not covered by any surviving local entry.
func (r *MVRegister[V]) Apply(delta []byte) error {
	valBytes, off, err := ReadVarintBytes(delta, 0)
	if err != nil {
		return err
	}
	if off+16 > len(delta) {
		return ErrShortBuffer
	}
	remoteDot, _ := DecodeDot(delta[off:])
	off += 16

	remoteCtx, err := DecodeVClock(delta[off:])
	if err != nil {
		return err
	}

	// Keep local entries whose dots are NOT covered by the remote context.
	type entry struct {
		valBytes []byte
		dot      Dot
	}
	var surviving []entry
	for _, e := range r.entries {
		if remoteCtx.Get(e.dot.Replica) >= e.dot.Counter {
			continue // covered by remote context — prune
		}
		surviving = append(surviving, entry{e.valBytes, e.dot})
	}

	// Add remote value if not already superseded by a local entry
	// from the same replica with equal or higher counter.
	addRemote := true
	for _, e := range surviving {
		if e.dot.Replica == remoteDot.Replica && e.dot.Counter >= remoteDot.Counter {
			addRemote = false
			break
		}
	}
	if addRemote {
		surviving = append(surviving, entry{valBytes, remoteDot})
	}

	r.entries = make([]mvEntry, len(surviving))
	for i, e := range surviving {
		r.entries[i] = mvEntry{valBytes: e.valBytes, dot: e.dot}
	}
	return nil
}

// DeltasSince returns deltas for entries with dots not covered by peerHWM.
func (r *MVRegister[V]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte
	// Use current entries as context for all deltas.
	ctx := make(VClock)
	for _, e := range r.entries {
		if e.dot.Counter > ctx.Get(e.dot.Replica) {
			ctx[e.dot.Replica] = e.dot.Counter
		}
	}

	for _, e := range r.entries {
		if e.dot.Counter > peerHWM.Get(e.dot.Replica) {
			var buf []byte
			buf = AppendVarintBytes(buf, e.valBytes)
			buf = append(buf, EncodeDot(e.dot)...)
			buf = append(buf, EncodeVClock(ctx)...)
			deltas = append(deltas, buf)
		}
	}
	return deltas
}
