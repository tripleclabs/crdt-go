package crdt

import "time"

// Backend abstracts entry storage for collection CRDT types (ORSet, ORMap,
// LWWMap, AWLWWMap, GList). The default in-memory implementation
// is [memoryBackend]. Providing a disk-backed implementation (e.g., backed
// by bbolt) enables CRDTs whose entries are too large for memory.
//
// Values are split into two byte slices: value (user data, opaque) and meta
// (CRDT metadata like dots, encoded via [encodeDot]/[encodeDotMap]). This
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
	backend    Backend
	transport  Transport
	topology   TopologyProvider
	concern    WriteConcern
	aeInterval time.Duration
}

func applyOptions(opts []Option) options {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// WithBackend sets the [Backend] for a collection CRDT. If not provided,
// the CRDT uses an in-memory [memoryBackend].
func WithBackend(b Backend) Option {
	return func(o *options) {
		o.backend = b
	}
}

// WithTransport sets the [Transport] for replication. If not provided,
// the CRDT operates as a local-only data structure.
func WithTransport(t Transport) Option {
	return func(o *options) { o.transport = t }
}

// WithTopology sets the [TopologyProvider] for peer discovery.
func WithTopology(t TopologyProvider) Option {
	return func(o *options) { o.topology = t }
}

// WithAntiEntropyInterval sets how often the replica runs anti-entropy
// sync with peers. Default is 1 second when a transport is configured.
// Set to 0 to disable anti-entropy.
func WithAntiEntropyInterval(d time.Duration) Option {
	return func(o *options) { o.aeInterval = d }
}
