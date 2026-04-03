package crdt

// memoryBackend is the default in-memory [Backend] backed by Go maps.
// All operations mutate in place. The zero value is ready to use.
type memoryBackend struct {
	entries    map[string]memEntry
	tombstones map[string][]byte
}

type memEntry struct {
	value []byte
	meta  []byte
}

// newMemoryBackend returns an initialized [memoryBackend].
func newMemoryBackend() *memoryBackend {
	return &memoryBackend{
		entries:    make(map[string]memEntry),
		tombstones: make(map[string][]byte),
	}
}

// GetEntry retrieves the value and metadata for key.
func (m *memoryBackend) GetEntry(key string) ([]byte, []byte, bool) {
	if m.entries == nil {
		return nil, nil, false
	}
	e, ok := m.entries[key]
	if !ok {
		return nil, nil, false
	}
	return e.value, e.meta, true
}

// PutEntry stores value and metadata under key.
func (m *memoryBackend) PutEntry(key string, value []byte, meta []byte) {
	if m.entries == nil {
		m.entries = make(map[string]memEntry)
	}
	m.entries[key] = memEntry{value: value, meta: meta}
}

// DeleteEntry removes the entry for key.
func (m *memoryBackend) DeleteEntry(key string) {
	if m.entries == nil {
		return
	}
	delete(m.entries, key)
}

// RangeEntries calls fn for each entry. If fn returns false, iteration stops.
func (m *memoryBackend) RangeEntries(fn func(key string, value []byte, meta []byte) bool) {
	if m.entries == nil {
		return
	}
	for k, e := range m.entries {
		if !fn(k, e.value, e.meta) {
			return
		}
	}
}

// EntryLen returns the number of entries.
func (m *memoryBackend) EntryLen() int {
	return len(m.entries)
}

// GetTombstone retrieves the metadata for a tombstoned key.
func (m *memoryBackend) GetTombstone(key string) ([]byte, bool) {
	if m.tombstones == nil {
		return nil, false
	}
	meta, ok := m.tombstones[key]
	return meta, ok
}

// PutTombstone stores tombstone metadata under key.
func (m *memoryBackend) PutTombstone(key string, meta []byte) {
	if m.tombstones == nil {
		m.tombstones = make(map[string][]byte)
	}
	m.tombstones[key] = meta
}

// DeleteTombstone removes the tombstone for key.
func (m *memoryBackend) DeleteTombstone(key string) {
	if m.tombstones == nil {
		return
	}
	delete(m.tombstones, key)
}

// RangeTombstones calls fn for each tombstone.
func (m *memoryBackend) RangeTombstones(fn func(key string, meta []byte) bool) {
	if m.tombstones == nil {
		return
	}
	for k, meta := range m.tombstones {
		if !fn(k, meta) {
			return
		}
	}
}

// TombstoneLen returns the number of tombstones.
func (m *memoryBackend) TombstoneLen() int {
	return len(m.tombstones)
}
