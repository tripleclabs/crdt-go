package crdt

// EntryStore abstracts element and entry storage for collection CRDT types
// (ORSet, ORMap, LWWMap, AWLWWMap, DeltaMap, GList). The default in-memory
// implementation is [MapStore]. Providing a disk-backed implementation (e.g.,
// backed by bbolt) enables CRDTs whose entries are too large for memory.
//
// Implementations must be safe for sequential use but need not be safe for
// concurrent use — concurrency control is the caller's responsibility.
type EntryStore interface {
	// Get retrieves the value for key. The second return value is false if
	// the key does not exist.
	Get(key string) ([]byte, bool)

	// Put stores value under key, overwriting any previous value.
	Put(key string, value []byte)

	// Delete removes the entry for key. It is a no-op if the key does not exist.
	Delete(key string)

	// Range calls fn for each entry in unspecified order. If fn returns false,
	// iteration stops early.
	Range(fn func(key string, value []byte) bool)

	// Len returns the number of entries.
	Len() int
}

// MapStore is the default in-memory [EntryStore] backed by a plain Go map.
// The zero value is ready to use.
type MapStore struct {
	m map[string][]byte
}

// NewMapStore returns an initialized [MapStore].
func NewMapStore() *MapStore {
	return &MapStore{m: make(map[string][]byte)}
}

// Get retrieves the value for key.
func (s *MapStore) Get(key string) ([]byte, bool) {
	if s.m == nil {
		return nil, false
	}
	v, ok := s.m[key]
	return v, ok
}

// Put stores value under key.
func (s *MapStore) Put(key string, value []byte) {
	if s.m == nil {
		s.m = make(map[string][]byte)
	}
	s.m[key] = value
}

// Delete removes the entry for key.
func (s *MapStore) Delete(key string) {
	if s.m == nil {
		return
	}
	delete(s.m, key)
}

// Range calls fn for each entry. If fn returns false, iteration stops.
func (s *MapStore) Range(fn func(key string, value []byte) bool) {
	if s.m == nil {
		return
	}
	for k, v := range s.m {
		if !fn(k, v) {
			return
		}
	}
}

// Len returns the number of entries.
func (s *MapStore) Len() int {
	return len(s.m)
}

// Option configures a CRDT at construction time.
type Option func(*options)

type options struct {
	store EntryStore
}

func applyOptions(opts []Option) options {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// WithStore sets the [EntryStore] for a collection CRDT. If not provided,
// the CRDT uses an in-memory [MapStore].
func WithStore(s EntryStore) Option {
	return func(o *options) {
		o.store = s
	}
}
