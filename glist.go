package crdt

import (
	"fmt"
	"sort"
)

// GList stores append-only items, each with a [Dot], backed by a [Backend].
// Items are keyed by "replica:counter" for deduplication. Retrieval is in
// causal order (sorted by counter, then replica ID).
//
// This is pure storage — no clocks, no merge logic, no delta encoding.
type GList[V any] struct {
	codec   Codec[V]
	backend Backend
}

// NewGList returns an initialized GList.
func NewGList[V any](codec Codec[V], opts ...Option) *GList[V] {
	o := applyOptions(opts)
	b := o.backend
	if b == nil {
		b = NewMemoryBackend()
	}
	return &GList[V]{codec: codec, backend: b}
}

// Append stores a value with the given dot. The item is keyed by
// "replica:counter" for deduplication.
func (l *GList[V]) Append(value V, dot Dot) error {
	valBytes, err := l.codec.Encode(value)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%d:%d", dot.Replica, dot.Counter)
	l.backend.PutEntry(key, valBytes, EncodeDot(dot))
	return nil
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
