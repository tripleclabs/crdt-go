// Package crdtbolt provides a bbolt-backed [crdt.Backend] implementation,
// enabling CRDTs whose entries are too large for memory.
//
// Each BoltBackend wraps a bbolt database with two buckets: "entries" for
// CRDT entries and "tombstones" for removal markers. Metadata (value bytes
// and CRDT dot/dotmap bytes) are stored as-is in the bucket values.
package crdtbolt

import (
	"encoding/binary"
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketEntries    = []byte("entries")
	bucketTombstones = []byte("tombstones")
)

// BoltBackend implements [crdt.Backend] backed by a bbolt database file.
// Entries and tombstones are stored in separate buckets. Each entry's value
// in the bucket is: [4-byte value length][value bytes][meta bytes].
//
// BoltBackend is not safe for concurrent use. The caller must provide
// synchronization if needed.
type BoltBackend struct {
	db   *bolt.DB
	path string
}

// Open opens or creates a bbolt database at path and returns a BoltBackend.
// The database file is created with 0600 permissions if it doesn't exist.
func Open(path string) (*BoltBackend, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("crdtbolt: open %s: %w", path, err)
	}
	// Create buckets if they don't exist.
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketEntries); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(bucketTombstones)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("crdtbolt: create buckets: %w", err)
	}
	return &BoltBackend{db: db, path: path}, nil
}

// Close closes the underlying bbolt database.
func (b *BoltBackend) Close() error {
	return b.db.Close()
}

// Path returns the filesystem path of the database.
func (b *BoltBackend) Path() string {
	return b.path
}

// encodeEntry packs value and meta into a single byte slice:
// [4-byte value length (big-endian)][value bytes][meta bytes]
func encodeEntry(value, meta []byte) []byte {
	vlen := len(value)
	buf := make([]byte, 4+vlen+len(meta))
	binary.BigEndian.PutUint32(buf[0:4], uint32(vlen))
	copy(buf[4:4+vlen], value)
	copy(buf[4+vlen:], meta)
	return buf
}

// decodeEntry unpacks value and meta from a bucket value.
func decodeEntry(data []byte) (value, meta []byte) {
	if len(data) < 4 {
		return nil, nil
	}
	vlen := int(binary.BigEndian.Uint32(data[0:4]))
	if len(data) < 4+vlen {
		return nil, nil
	}
	return data[4 : 4+vlen], data[4+vlen:]
}

// GetEntry retrieves the value and metadata for key.
func (b *BoltBackend) GetEntry(key string) (value []byte, meta []byte, ok bool) {
	b.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketEntries)
		data := bkt.Get([]byte(key))
		if data != nil {
			value, meta = decodeEntry(data)
			ok = true
		}
		return nil
	})
	return
}

// PutEntry stores value and metadata under key.
func (b *BoltBackend) PutEntry(key string, value []byte, meta []byte) {
	b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketEntries).Put([]byte(key), encodeEntry(value, meta))
	})
}

// DeleteEntry removes the entry for key.
func (b *BoltBackend) DeleteEntry(key string) {
	b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketEntries).Delete([]byte(key))
	})
}

// RangeEntries calls fn for each entry. If fn returns false, iteration stops.
func (b *BoltBackend) RangeEntries(fn func(key string, value []byte, meta []byte) bool) {
	b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketEntries).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			val, met := decodeEntry(v)
			if !fn(string(k), val, met) {
				break
			}
		}
		return nil
	})
}

// EntryLen returns the number of entries.
func (b *BoltBackend) EntryLen() int {
	var n int
	b.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(bucketEntries).Stats().KeyN
		return nil
	})
	return n
}

// GetTombstone retrieves the metadata for a tombstoned key.
func (b *BoltBackend) GetTombstone(key string) (meta []byte, ok bool) {
	b.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketTombstones).Get([]byte(key))
		if data != nil {
			meta = make([]byte, len(data))
			copy(meta, data)
			ok = true
		}
		return nil
	})
	return
}

// PutTombstone stores tombstone metadata under key.
func (b *BoltBackend) PutTombstone(key string, meta []byte) {
	b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTombstones).Put([]byte(key), meta)
	})
}

// DeleteTombstone removes the tombstone for key.
func (b *BoltBackend) DeleteTombstone(key string) {
	b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTombstones).Delete([]byte(key))
	})
}

// RangeTombstones calls fn for each tombstone. If fn returns false, iteration stops.
func (b *BoltBackend) RangeTombstones(fn func(key string, meta []byte) bool) {
	b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketTombstones).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if !fn(string(k), v) {
				break
			}
		}
		return nil
	})
}

// TombstoneLen returns the number of tombstones.
func (b *BoltBackend) TombstoneLen() int {
	var n int
	b.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(bucketTombstones).Stats().KeyN
		return nil
	})
	return n
}

// Snapshot writes a consistent snapshot of the database to w. This can be
// used to bootstrap new replicas by shipping the entire database file.
func (b *BoltBackend) Snapshot(w *os.File) error {
	return b.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(w)
		return err
	})
}
