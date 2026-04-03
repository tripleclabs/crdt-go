package crdt

import (
	"fmt"
	"sort"
)

// gListState stores append-only items, each with a [Dot], backed by a [Backend].
// Items are keyed by "replica:counter" for deduplication. Retrieval is in
// causal order (sorted by counter, then replica ID).
//
// gListState implements [mergeable] for use with [replica] and [alwaysMergeClock].
type gListState[V any] struct {
	codec   Codec[V]
	backend Backend
}

// newGListState returns an initialized GList. Use [newGListReplica] to create
// a fully wired Replica.
func newGListState[V any](codec Codec[V], opts ...Option) *gListState[V] {
	requireCodec(codec)
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = newMemoryBackend()
	}
	return &gListState[V]{codec: codec, backend: b}
}

// --- Mutations (return delta bytes) ---

// Append stores a value with the given dot. The item is keyed by
// "replica:counter" for deduplication. Returns the encoded delta to send to peers.
//
// Delta format: [varint val len][val][16 byte dot]
func (l *gListState[V]) Append(value V, dot Dot) ([]byte, error) {
	valBytes, err := l.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%d:%d", dot.Replica, dot.Counter)
	l.backend.PutEntry(key, valBytes, encodeDot(dot))

	var buf []byte
	buf = appendVarintBytes(buf, valBytes)
	buf = append(buf, encodeDot(dot)...)
	return buf, nil
}

// AppendBytes stores pre-encoded value bytes with the given dot.
func (l *gListState[V]) AppendBytes(valBytes []byte, dot Dot) {
	key := fmt.Sprintf("%d:%d", dot.Replica, dot.Counter)
	l.backend.PutEntry(key, valBytes, encodeDot(dot))
}

// Has reports whether an item with the given dot exists.
func (l *gListState[V]) Has(dot Dot) bool {
	key := fmt.Sprintf("%d:%d", dot.Replica, dot.Counter)
	_, _, ok := l.backend.GetEntry(key)
	return ok
}

// Items returns all items in causal order (counter ascending, then
// replica ID ascending for ties).
func (l *gListState[V]) Items() ([]V, error) {
	type item struct {
		value V
		dot   Dot
	}
	items := make([]item, 0, l.backend.EntryLen())
	var decErr error
	l.backend.RangeEntries(func(_ string, valBytes []byte, metaBytes []byte) bool {
		v, err := l.codec.Decode(valBytes)
		if err != nil {
			decErr = err
			return false
		}
		d, err := decodeDot(metaBytes)
		if err != nil {
			decErr = err
			return false
		}
		items = append(items, item{value: v, dot: d})
		return true
	})
	if decErr != nil {
		return nil, decErr
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].dot.Counter != items[j].dot.Counter {
			return items[i].dot.Counter < items[j].dot.Counter
		}
		return items[i].dot.Replica < items[j].dot.Replica
	})
	out := make([]V, len(items))
	for i, it := range items {
		out[i] = it.value
	}
	return out, nil
}

// Range calls fn for each item with raw bytes, in unspecified order.
func (l *gListState[V]) Range(fn func(valBytes []byte, dot Dot) bool) {
	l.backend.RangeEntries(func(_ string, valBytes []byte, metaBytes []byte) bool {
		d, err := decodeDot(metaBytes)
		if err != nil {
			return true
		}
		return fn(valBytes, d)
	})
}

// Len returns the number of items.
func (l *gListState[V]) Len() int { return l.backend.EntryLen() }

// --- Queryable ---

// EntryMeta returns the encoded dot for the entry at key.
func (l *gListState[V]) EntryMeta(key string) ([]byte, bool) {
	_, meta, ok := l.backend.GetEntry(key)
	return meta, ok
}

// TombstoneMeta always returns false — GList is append-only.
func (l *gListState[V]) TombstoneMeta(key string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [deltaInfo] from an encoded GList delta.
// GList deltas have no op code — all deltas are appends.
func (l *gListState[V]) ParseDelta(delta []byte) (deltaInfo, error) {
	_, off, err := readVarintBytes(delta, 0)
	if err != nil {
		return deltaInfo{}, err
	}
	if off+16 > len(delta) {
		return deltaInfo{}, errShortBuffer
	}
	dot, err := decodeDot(delta[off:])
	if err != nil {
		return deltaInfo{}, err
	}
	key := fmt.Sprintf("%d:%d", dot.Replica, dot.Counter)
	return deltaInfo{
		Op:   opPut,
		Key:  key,
		Meta: delta[off : off+16],
		Dots: []Dot{dot},
	}, nil
}

// Apply unconditionally merges a delta into the GList. Deduplicates by dot —
// only appends if the item is not already present.
func (l *gListState[V]) Apply(delta []byte) error {
	valBytes, off, err := readVarintBytes(delta, 0)
	if err != nil {
		return err
	}
	if off+16 > len(delta) {
		return errShortBuffer
	}
	remoteDot, err := decodeDot(delta[off:])
	if err != nil {
		return err
	}
	if !l.Has(remoteDot) {
		l.AppendBytes(valBytes, remoteDot)
	}
	return nil
}

// DeltasSince returns encoded deltas for items with dots not covered by peerHWM.
func (l *gListState[V]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte
	l.Range(func(valBytes []byte, dot Dot) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			var buf []byte
			buf = appendVarintBytes(buf, valBytes)
			buf = append(buf, encodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})
	return deltas
}
