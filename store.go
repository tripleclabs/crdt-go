package crdt

// Backend abstracts entry storage for collection CRDT types (ORSet, ORMap,
// LWWMap, AWLWWMap, GList). The default in-memory implementation
// is [MemoryBackend]. Providing a disk-backed implementation (e.g., backed
// by bbolt) enables CRDTs whose entries are too large for memory.
//
// Values are split into two byte slices: value (user data, opaque) and meta
// (CRDT metadata like dots, encoded via [EncodeDot]/[EncodeDotMap]). This
// separation allows merge operations to compare metadata without
// deserializing potentially large user values.
//
// All operations mutate the backend in place. There is no Clone method —
// backends are mutable stores, not value types.
//
// Implementations must be safe for sequential use but need not be safe for
// concurrent use — concurrency control is the caller's responsibility.
type Backend interface {
	// GetEntry retrieves the value and metadata for key. Returns ok=false
	// if the key does not exist.
	GetEntry(key string) (value []byte, meta []byte, ok bool)

	// PutEntry stores value and metadata under key, overwriting any
	// previous entry.
	PutEntry(key string, value []byte, meta []byte)

	// DeleteEntry removes the entry for key. No-op if the key does not exist.
	DeleteEntry(key string)

	// RangeEntries calls fn for each entry in unspecified order. If fn
	// returns false, iteration stops early.
	RangeEntries(fn func(key string, value []byte, meta []byte) bool)

	// EntryLen returns the number of entries.
	EntryLen() int

	// GetTombstone retrieves the metadata for a tombstoned key. Returns
	// ok=false if no tombstone exists. Types without remove operations
	// (GList, ORSet) may use a no-op implementation.
	GetTombstone(key string) (meta []byte, ok bool)

	// PutTombstone stores tombstone metadata under key.
	PutTombstone(key string, meta []byte)

	// DeleteTombstone removes the tombstone for key. No-op if none exists.
	DeleteTombstone(key string)

	// RangeTombstones calls fn for each tombstone in unspecified order.
	// If fn returns false, iteration stops early.
	RangeTombstones(fn func(key string, meta []byte) bool)

	// TombstoneLen returns the number of tombstones.
	TombstoneLen() int
}

// Option configures a CRDT at construction time.
type Option func(*options)

type options struct {
	backend Backend
}

func applyOptions(opts []Option) options {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// WithBackend sets the [Backend] for a collection CRDT. If not provided,
// the CRDT uses an in-memory [MemoryBackend].
func WithBackend(b Backend) Option {
	return func(o *options) {
		o.backend = b
	}
}
