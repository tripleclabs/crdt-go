// Package crdt provides pure-Go CRDT data types and causality primitives.
//
// CRDT types in this package are pure typed storage — they read and write
// a [Backend] with dot/dotmap metadata but contain no merge logic, no
// clocks, and no delta encoding. The merge semantics and clock management
// live in the replica layer (see the replica sub-package).
//
// # CRDT Types
//
//   - [GCounter]: Grow-only counter (replica → count)
//   - [PNCounter]: Positive-negative counter
//   - [LWWRegister]: Last-write-wins register (single value + dot)
//   - [MVRegister]: Multi-value register (concurrent values + dots)
//   - [LWWMap]: Last-write-wins map (key → value + dot, with tombstones)
//   - [ORSet]: Observed-remove set (element → dotmap)
//   - [ORMap]: Observed-remove map (key → value + dotmap)
//   - [AWLWWMap]: Add-wins LWW map (tombstones carry causal context)
//   - [GList]: Grow-only list (append-only, causal ordering)
//
// # Causality Primitives
//
//   - [Dot]: A single causal event (replica, counter)
//   - [DotMap]: Compressed vector clock per element
//   - [VClock]: Vector clock
//   - [LocalClock]: Monotonic counter for a single replica
//   - [ReceivedClock]: Tracks contiguous receipt per remote replica
//
// # Storage
//
// Collection types use a pluggable [Backend] interface. The default
// [MemoryBackend] uses in-memory Go maps. Disk-backed implementations
// (e.g., bbolt) enable CRDTs that exceed memory.
package crdt
