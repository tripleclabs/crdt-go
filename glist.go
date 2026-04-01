package crdt

import (
	"fmt"
	"sort"
)

// GList stores append-only items, each with a [Dot], backed by a [Backend].
// Items are keyed by "replica:counter" for deduplication. Retrieval is in
// causal order (sorted by counter, then replica ID).
//
// GList implements [Mergeable] for use with [Replica] and [AlwaysMergeClock].
type GList[V any] struct {
	codec   Codec[V]
	backend Backend
}

// NewGList returns an initialized GList. Use [NewGListReplica] to create
// a fully wired Replica.
func NewGList[V any](codec Codec[V], opts ...Option) *GList[V] {
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = NewMemoryBackend()
	}
	return &GList[V]{codec: codec, backend: b}
}

// NewGListReplica creates a [Replica] wrapping a [GList] with [AlwaysMergeClock].
func NewGListReplica[V any](replicaID ReplicaID, codec Codec[V], opts ...Option) *Replica[*GList[V]] {
	return NewReplica[*GList[V]](replicaID, NewGList(codec, opts...), AlwaysMergeClock{})
}

// --- Mutations (return delta bytes) ---

// Append stores a value with the given dot. The item is keyed by
// "replica:counter" for deduplication. Returns the encoded delta to send to peers.
//
// Delta format: [varint val len][val][16 byte dot]
func (l *GList[V]) Append(value V, dot Dot) ([]byte, error) {
	valBytes, err := l.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%d:%d", dot.Replica, dot.Counter)
	l.backend.PutEntry(key, valBytes, EncodeDot(dot))

	var buf []byte
	buf = AppendVarintBytes(buf, valBytes)
	buf = append(buf, EncodeDot(dot)...)
	return buf, nil
}

// AppendBytes stores pre-encoded value bytes with the given dot.
func (l *GList[V]) AppendBytes(valBytes []byte, dot Dot) {
	key := fmt.Sprintf("%d:%d", dot.Replica, dot.Counter)
	l.backend.PutEntry(key, valBytes, EncodeDot(dot))
}

// Has reports whether an item with the given dot exists.
func (l *GList[V]) Has(dot Dot) bool {
	key := fmt.Sprintf("%d:%d", dot.Replica, dot.Counter)
	_, _, ok := l.backend.GetEntry(key)
	return ok
}

// Items returns all items in causal order (counter ascending, then
// replica ID ascending for ties).
func (l *GList[V]) Items() ([]V, error) {
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
		d, _ := DecodeDot(metaBytes)
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
func (l *GList[V]) Range(fn func(valBytes []byte, dot Dot) bool) {
	l.backend.RangeEntries(func(_ string, valBytes []byte, metaBytes []byte) bool {
		d, _ := DecodeDot(metaBytes)
		return fn(valBytes, d)
	})
}

// Len returns the number of items.
func (l *GList[V]) Len() int { return l.backend.EntryLen() }

// --- Queryable ---

// EntryMeta returns the encoded dot for the entry at key.
func (l *GList[V]) EntryMeta(key string) ([]byte, bool) {
	_, meta, ok := l.backend.GetEntry(key)
	return meta, ok
}

// TombstoneMeta always returns false — GList is append-only.
func (l *GList[V]) TombstoneMeta(key string) ([]byte, bool) {
	return nil, false
}

// --- Mergeable ---

// ParseDelta extracts a [DeltaInfo] from an encoded GList delta.
// GList deltas have no op code — all deltas are appends.
func (l *GList[V]) ParseDelta(delta []byte) (DeltaInfo, error) {
	_, off, err := ReadVarintBytes(delta, 0)
	if err != nil {
		return DeltaInfo{}, err
	}
	if off+16 > len(delta) {
		return DeltaInfo{}, ErrShortBuffer
	}
	dot, _ := DecodeDot(delta[off:])
	key := fmt.Sprintf("%d:%d", dot.Replica, dot.Counter)
	return DeltaInfo{
		Op:   OpPut,
		Key:  key,
		Meta: delta[off : off+16],
		Dots: []Dot{dot},
	}, nil
}

// Apply unconditionally merges a delta into the GList. Deduplicates by dot —
// only appends if the item is not already present.
func (l *GList[V]) Apply(delta []byte) error {
	valBytes, off, err := ReadVarintBytes(delta, 0)
	if err != nil {
		return err
	}
	if off+16 > len(delta) {
		return ErrShortBuffer
	}
	remoteDot, _ := DecodeDot(delta[off:])
	if !l.Has(remoteDot) {
		l.AppendBytes(valBytes, remoteDot)
	}
	return nil
}

// DeltasSince returns encoded deltas for items with dots not covered by peerHWM.
func (l *GList[V]) DeltasSince(peerHWM VClock) [][]byte {
	var deltas [][]byte
	l.Range(func(valBytes []byte, dot Dot) bool {
		if dot.Counter > peerHWM.Get(dot.Replica) {
			var buf []byte
			buf = AppendVarintBytes(buf, valBytes)
			buf = append(buf, EncodeDot(dot)...)
			deltas = append(deltas, buf)
		}
		return true
	})
	return deltas
}
