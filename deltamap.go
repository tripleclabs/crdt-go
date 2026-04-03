package crdt

import "context"

// deltaMapState implements mergeable for a map of string keys to inner CRDT
// states. It uses a shared causal context (the parent replica's clock) and
// DSON-style tombstone-free removes via causal context pruning.
type deltaMapTombstone struct {
	dot Dot
	ctx VClock
}

type deltaMapState[K CRDTKind] struct {
	kind       K
	entries    map[string]any
	tombstones map[string]deltaMapTombstone
}

func newDeltaMapState[K CRDTKind](kind K) *deltaMapState[K] {
	return &deltaMapState[K]{
		kind:       kind,
		entries:    make(map[string]any),
		tombstones: make(map[string]deltaMapTombstone),
	}
}

func (m *deltaMapState[K]) getOrCreate(key string) any {
	if s, ok := m.entries[key]; ok {
		return s
	}
	s := m.kind.newState()
	m.entries[key] = s
	return s
}

// --- Composite delta format ---
//
// Put:    [opPut][varint key len][key bytes][inner delta bytes]
// Remove: [opRemove][varint key len][key bytes][16-byte dot][encoded VClock context]

func (m *deltaMapState[K]) wrapPut(key string, innerDelta []byte) []byte {
	buf := []byte{opPut}
	buf = appendVarintBytes(buf, []byte(key))
	buf = append(buf, innerDelta...)
	return buf
}

func (m *deltaMapState[K]) wrapRemove(key string, dot Dot, ctx VClock) []byte {
	buf := []byte{opRemove}
	buf = appendVarintBytes(buf, []byte(key))
	buf = append(buf, encodeDot(dot)...)
	buf = append(buf, encodeVClock(ctx)...)
	return buf
}

// --- mergeable implementation ---

func (m *deltaMapState[K]) EntryMeta(key string) ([]byte, bool) {
	inner, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	return m.kind.entryMeta(inner, key)
}

func (m *deltaMapState[K]) TombstoneMeta(key string) ([]byte, bool) {
	ts, ok := m.tombstones[key]
	if !ok {
		return nil, false
	}
	return encodeRemoveTombstone(ts.dot, ts.ctx), true
}

func (m *deltaMapState[K]) ParseDelta(delta []byte) (deltaInfo, error) {
	if len(delta) < 1 {
		return deltaInfo{}, errShortBuffer
	}
	op := delta[0]
	keyBytes, off, err := readVarintBytes(delta[1:], 0)
	if err != nil {
		return deltaInfo{}, err
	}
	key := string(keyBytes)

	switch op {
	case opPut:
		innerDelta := delta[1+off:]
		inner := m.getOrCreate(key)
		info, err := m.kind.parseDelta(inner, innerDelta)
		if err != nil {
			return deltaInfo{}, err
		}
		info.Key = key
		info.Op = opPut
		return info, nil
	case opRemove:
		if off+16 > len(delta[1:]) {
			return deltaInfo{}, errShortBuffer
		}
		dot, err := decodeDot(delta[1+off:])
		if err != nil {
			return deltaInfo{}, err
		}
		return deltaInfo{
			Op:   opRemove,
			Key:  key,
			Dots: []Dot{dot},
		}, nil
	default:
		return deltaInfo{}, errUnknownOp
	}
}

func (m *deltaMapState[K]) Apply(delta []byte) error {
	if len(delta) < 1 {
		return errShortBuffer
	}
	op := delta[0]
	keyBytes, off, err := readVarintBytes(delta[1:], 0)
	if err != nil {
		return err
	}
	key := string(keyBytes)

	switch op {
	case opPut:
		innerDelta := delta[1+off:]
		inner := m.getOrCreate(key)
		return m.kind.applyDelta(inner, innerDelta)
	case opRemove:
		if off+16 > len(delta[1:]) {
			return errShortBuffer
		}
		dot, err := decodeDot(delta[1+off:])
		if err != nil {
			return err
		}
		removeCtx, err := decodeVClock(delta[1+off+16:])
		if err != nil {
			return err
		}
		// Store/merge tombstone.
		if existing, ok := m.tombstones[key]; ok {
			if !DotGT(dot, existing.dot) {
				dot = existing.dot
			}
			removeCtx.Merge(existing.ctx)
		}
		m.tombstones[key] = deltaMapTombstone{dot: dot, ctx: removeCtx}
		// Prune inner state.
		inner, ok := m.entries[key]
		if !ok {
			return nil
		}
		m.kind.removeAll(inner, removeCtx)
		if len(m.kind.deltasSince(inner, VClock{})) == 0 {
			delete(m.entries, key)
		}
		return nil
	default:
		return errUnknownOp
	}
}

func (m *deltaMapState[K]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte
	for key, inner := range m.entries {
		innerDeltas := m.kind.deltasSince(inner, peerHWM)
		for _, d := range innerDeltas {
			deltas = append(deltas, m.wrapPut(key, d))
		}
	}
	for key, ts := range m.tombstones {
		if ts.dot.Counter > peerHWM.Get(ts.dot.Replica) {
			deltas = append(deltas, m.wrapRemove(key, ts.dot, ts.ctx))
		}
	}
	return deltas
}

// GCTombstones removes tombstones whose dots are fully covered by minHWM.
func (m *deltaMapState[K]) GCTombstones(minHWM VClock) {
	for key, ts := range m.tombstones {
		if ts.dot.Counter <= minHWM.Get(ts.dot.Replica) {
			delete(m.tombstones, key)
		}
	}
}

// ---------------------------------------------------------------------------
// DeltaMap node type
// ---------------------------------------------------------------------------

// DeltaMap is a composable CRDT map whose values are themselves CRDTs.
// The kind type parameter K determines the inner CRDT type and constrains
// which [Mutation] and [Query] types are accepted.
type DeltaMap[K CRDTKind] struct {
	r *replica[*deltaMapState[K]]
}

// NewDeltaMap creates a new DeltaMap. The kind value carries the codec
// needed to create inner CRDT states.
func NewDeltaMap[K CRDTKind](id ReplicaID, kind K, opts ...Option) *DeltaMap[K] {
	state := newDeltaMapState(kind)
	r := newReplica(id, state, alwaysMergeClock{}, opts...)
	return &DeltaMap[K]{r: r}
}

// Mutate applies a mutation to the inner CRDT at the given key. The key's
// entry is created if it doesn't exist. Compile-time safe: only mutations
// matching the DeltaMap's kind type K are accepted.
func (dm *DeltaMap[K]) Mutate(ctx context.Context, key string, mut Mutation[K]) (*WriteResult, error) {
	dm.r.mu.Lock()
	d := dm.r.nextDot()
	inner := dm.r.data.getOrCreate(key)
	innerDelta, err := mut.applyMutation(inner, d, dm.r.received.HWM())
	if err != nil {
		dm.r.mu.Unlock()
		return nil, err
	}
	delta := dm.r.data.wrapPut(key, innerDelta)
	dm.r.trackKey(key)
	dm.r.mu.Unlock()
	return dm.r.propagate(ctx, d, delta), nil
}

// RemoveKey removes an entire key and its inner CRDT state. Uses
// DSON-style tombstone-free removal: the causal context at remove time
// determines what is pruned. Concurrent adds with unseen dots survive.
func (dm *DeltaMap[K]) RemoveKey(ctx context.Context, key string) *WriteResult {
	dm.r.mu.Lock()
	d := dm.r.nextDot()
	hwm := dm.r.received.HWM()
	delta := dm.r.data.wrapRemove(key, d, hwm)
	// Store tombstone and delete entry locally.
	dm.r.data.tombstones[key] = deltaMapTombstone{dot: d, ctx: hwm}
	dm.r.data.kind.removeAll(dm.r.data.getOrCreate(key), hwm)
	if len(dm.r.data.kind.deltasSince(dm.r.data.entries[key], VClock{})) == 0 {
		delete(dm.r.data.entries, key)
	}
	dm.r.trackKey(key)
	dm.r.mu.Unlock()
	return dm.r.propagate(ctx, d, delta)
}

// Query executes a type-safe query against the inner CRDT at the given key.
// Returns nil if the key doesn't exist.
func (dm *DeltaMap[K]) Query(key string, q Query[K]) any {
	dm.r.mu.Lock()
	defer dm.r.mu.Unlock()
	inner, ok := dm.r.data.entries[key]
	if !ok {
		return nil
	}
	return q.execQuery(inner)
}

// HasKey reports whether an entry exists for the given key.
func (dm *DeltaMap[K]) HasKey(key string) bool {
	dm.r.mu.Lock()
	defer dm.r.mu.Unlock()
	_, ok := dm.r.data.entries[key]
	return ok
}

// Keys returns all keys in the map.
func (dm *DeltaMap[K]) Keys() []string {
	dm.r.mu.Lock()
	defer dm.r.mu.Unlock()
	keys := make([]string, 0, len(dm.r.data.entries))
	for k := range dm.r.data.entries {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of keys.
func (dm *DeltaMap[K]) Len() int {
	dm.r.mu.Lock()
	defer dm.r.mu.Unlock()
	return len(dm.r.data.entries)
}

// Close stops the anti-entropy goroutine.
func (dm *DeltaMap[K]) Close() { dm.r.Close() }
