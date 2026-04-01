// Package crdt provides pure-Go implementations of Conflict-free Replicated
// Data Types (CRDTs) with provably correct semantics.
//
// CRDTs are data structures that can be replicated across multiple nodes in a
// distributed system and merged without coordination, guaranteeing eventual
// consistency. Every merge operation in this library is commutative, associative,
// and idempotent.
//
// # CRDT Types
//
// The library provides 10 CRDT types:
//
//   - [GCounter]: Grow-only counter (increment only)
//   - [PNCounter]: Positive-negative counter (increment and decrement)
//   - [LWWRegister]: Last-write-wins register (single value, deterministic tie-breaking)
//   - [MVRegister]: Multi-value register (preserves concurrent writes)
//   - [ORSet]: Observed-remove set (add-wins semantics)
//   - [GList]: Grow-only list (append only, causal ordering)
//   - [ORMap]: Observed-remove map (add-wins keys)
//   - [LWWMap]: Last-write-wins map (per-key LWW with tombstones)
//   - [AWLWWMap]: Add-wins last-write-wins map (add-wins bias on concurrent add/remove)
//   - [DeltaMap]: Typed nested CRDT map (each key holds a typed CRDT value)
//
// # Design
//
// All CRDT types are plain value types with no internal synchronization. Mutations
// return a new state and a [Delta] representing the minimal change — the original
// value is never modified. This makes the types safe to use in concurrent code
// when wrapped with the caller's own synchronization.
//
// Transport, membership, persistence, and process management are explicitly out
// of scope. Consumers serialize deltas via [encoding.BinaryMarshaler] /
// [encoding.BinaryUnmarshaler] and send them however they choose.
//
// # Storage
//
// Collection types (sets, maps, lists) accept an optional [EntryStore] via
// functional options. The default is an in-memory store. Providing a disk-backed
// implementation (e.g., bbolt) enables CRDTs that are too large for memory.
package crdt
