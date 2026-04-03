// Package crdt provides replicated data types that synchronize automatically
// across peers. Pick a type, wire up a [Transport] and [TopologyProvider],
// and mutations propagate without any manual sync.
//
// # CRDT Types
//
//   - [LWWMap]: Last-write-wins map
//   - [ORSet]: Observed-remove set
//   - [ORMap]: Observed-remove map
//   - [AWLWWMap]: Add-wins LWW map
//   - [GList]: Grow-only list
//   - [GCounter]: Grow-only counter
//   - [PNCounter]: Positive-negative counter
//   - [LWWRegister]: Last-write-wins register
//   - [MVRegister]: Multi-value register
//
// # Configuration
//
// All types accept functional [Option] values:
//
//   - [WithTransport]: sets the network transport
//   - [WithTopology]: sets the peer discovery provider
//   - [WithBackend]: sets a custom storage backend (default: in-memory)
//   - [WithWriteConcern]: sets quorum level ([WLocal], [WMajority], [WAll])
//   - [WithAntiEntropyInterval]: sets background sync interval
//
// Without a transport, types operate as local-only data structures.
//
// # Write Concerns
//
// Mutations return a [*WriteResult]. Call [WriteResult.Wait] to block until
// the configured quorum is reached. For [WLocal] (the default), Wait
// returns immediately.
package crdt
