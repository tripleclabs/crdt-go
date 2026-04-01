// Package crdt provides pure-Go CRDT data types with pluggable storage and
// clock semantics.
//
// Each CRDT is composed of three orthogonal axes:
//
//   - Storage semantics: the data type ([LWWMap], [ORSet], etc.) backed by a
//     pluggable [Backend] (in-memory or disk-backed via bbolt)
//   - Clock semantics: the domination rule ([LWWClock], [AddWinsClock],
//     [MaxWinsClock], [AlwaysMergeClock])
//   - Merge semantics: how winning deltas are applied to storage
//     ([Mergeable.Apply])
//
// A single generic [Replica] wraps any [Mergeable] type with clocks and
// provides the replication surface: [Replica.ApplyDelta] for incoming deltas,
// [Replica.DeltasSince] for anti-entropy, and [Replica.NextDot] for local
// mutations.
//
// # CRDT Types
//
//   - [LWWMap]: Last-write-wins map (key → value + dot, with tombstones)
//   - [ORSet]: Observed-remove set (element → dotmap)
//   - [ORMap]: Observed-remove map (key → value + dotmap)
//   - [AWLWWMap]: Add-wins LWW map (tombstones carry causal context)
//   - [GList]: Grow-only list (append-only, causal ordering)
//   - [GCounter]: Grow-only counter (replica → count)
//   - [PNCounter]: Positive-negative counter
//   - [LWWRegister]: Last-write-wins register (single value + dot)
//   - [MVRegister]: Multi-value register (concurrent values + dots)
//
// # Clock Strategies
//
//   - [LWWClock]: Higher dot wins ([DotGT])
//   - [AddWinsClock]: Concurrent add beats concurrent remove
//   - [MaxWinsClock]: Higher count wins
//   - [AlwaysMergeClock]: Always apply (merge logic in [Mergeable.Apply])
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
