# crdt

Pure-Go CRDT library with clean separation of storage, causality, and replication.

```
go get github.com/3clabs/crdt
```

## Architecture

The library has three layers:

**`crdt` package** — Pure typed storage. CRDT types are thin wrappers over a pluggable `Backend`. They store data with dot metadata but contain no merge logic, no clocks, and no delta encoding.

**`crdt` package (clock primitives)** — `LocalClock`, `ReceivedClock`, `Dot`, `DotMap`, `VClock`. All causality tracking in one place.

**`crdt/replica` package** — Distributed orchestration. Wraps a storage type with clocks. Stamps dots on mutations, compares dots on incoming deltas, encodes/decodes delta bytes, tracks what peers have received.

## Quick start

```go
import (
    "github.com/3clabs/crdt"
    "github.com/3clabs/crdt/replica"
)

// Create replicas on two nodes.
nodeA := replica.NewLWWMap[string](1, crdt.StringCodec{})
nodeB := replica.NewLWWMap[string](2, crdt.StringCodec{})

// Mutate locally — returns delta bytes to send to peers.
deltaA, _ := nodeA.Put("name", "alice")
deltaB, _ := nodeB.Put("color", "blue")

// Send deltas over any transport (gRPC, NATS, TCP, ...).
nodeA.ApplyDelta(deltaB)
nodeB.ApplyDelta(deltaA)

// Both nodes converge.
v, _, _ := nodeA.Data.Get("color")  // "blue"
v, _, _ = nodeB.Data.Get("name")    // "alice"
```

## Anti-entropy

When deltas may be lost (network partitions, restarts), use the received clock
to catch up:

```go
// Node A asks B: "what do you have?"
peerHWM := nodeB.Received.HWM()

// Node A sends what B is missing.
for _, delta := range nodeA.DeltasSince(peerHWM) {
    send(nodeB, delta)
}
```

The `ReceivedClock` tracks contiguous receipt per replica and handles
out-of-order delivery. If deltas arrive as 1, 3, 5, the high-water mark
stays at 1 until 2 arrives, then advances to 3 (or 5 if both fill).

## Types

| Type | Storage | Replica | Semantics |
|------|---------|---------|-----------|
| `GCounter` | replica → count | `GCounterReplica` | Grow-only, max per replica |
| `PNCounter` | pos/neg maps | `PNCounterReplica` | Increment + decrement |
| `LWWRegister[V]` | value + dot | `LWWRegisterReplica[V]` | Last write wins |
| `MVRegister[V]` | values + dots | `MVRegisterReplica[V]` | Preserves concurrent writes |
| `LWWMap[V]` | key → (value, dot) + tombstones | `LWWMapReplica[V]` | Per-key LWW |
| `ORSet[E]` | element → dotmap | `ORSetReplica[E]` | Add-wins |
| `ORMap[V]` | key → (value, dotmap) | `ORMapReplica[V]` | Add-wins keys |
| `AWLWWMap[V]` | key → (value, dot) + context tombstones | `AWLWWMapReplica[V]` | Add-wins bias |
| `GList[V]` | items + dots | `GListReplica[V]` | Append-only, causal order |

## Generics and Codecs

Collection types are generic over their value type. Values are encoded/decoded
via a `Codec[V]` interface:

```go
type Codec[V any] interface {
    Encode(V) ([]byte, error)
    Decode([]byte) (V, error)
}
```

Built-in codecs for common types:

```go
crdt.StringCodec{}    // string ↔ []byte
crdt.Int64Codec{}     // int64 ↔ 8 bytes big-endian
crdt.Uint64Codec{}    // uint64 ↔ 8 bytes big-endian
crdt.BytesCodec{}     // []byte passthrough
```

Custom types:

```go
type UserCodec struct{}

func (UserCodec) Encode(u User) ([]byte, error) { return json.Marshal(u) }
func (UserCodec) Decode(b []byte) (User, error) {
    var u User
    return u, json.Unmarshal(b, &u)
}

m := replica.NewLWWMap[User](replicaID, UserCodec{})
m.Put("user:123", User{Name: "alice", Age: 30})
```

## Storage backends

Collection types (maps, sets, lists) use a pluggable `Backend` interface:

```go
type Backend interface {
    GetEntry(key string) (value []byte, meta []byte, ok bool)
    PutEntry(key string, value []byte, meta []byte)
    DeleteEntry(key string)
    RangeEntries(fn func(key string, value []byte, meta []byte) bool)
    EntryLen() int
    GetTombstone(key string) (meta []byte, ok bool)
    PutTombstone(key string, meta []byte)
    DeleteTombstone(key string)
    RangeTombstones(fn func(key string, meta []byte) bool)
    TombstoneLen() int
}
```

The default is `MemoryBackend` (Go maps). The `crdtbolt` sub-module provides a
bbolt-backed implementation for data that exceeds memory:

```go
import "github.com/3clabs/crdt/crdtbolt"

backend, _ := crdtbolt.Open("/path/to/data.db")
defer backend.Close()

m := replica.NewLWWMap[string](replicaID, crdt.StringCodec{}, crdt.WithBackend(backend))
```

## Clock model

Each replica has two clocks:

- **`LocalClock`** — Monotonic counter. Only incremented by local mutations. Produces `Dot` values (replica ID + counter) that stamp each operation.

- **`ReceivedClock`** — Tracks the highest *contiguous* counter received per remote replica. Handles out-of-order delivery with gap buffering. This is what peers exchange during anti-entropy.

The CRDT storage types have no clock fields at all. Clocks are owned by the
replica layer.

## Delta wire format

Each mutation returns `[]byte` — the encoded delta ready to send over the wire.
Each type has its own compact binary format. Example for LWWMap:

**Put delta:** `[op=0x01][varint key len][key][varint val len][val][16 byte dot]`

**Remove delta:** `[op=0x02][varint key len][key][16 byte dot]`

Deltas are idempotent — applying the same delta twice has no effect.

## Direct storage access

The replica layer exposes its underlying storage type via the `Data` field.
Use this for reads that don't need clock management:

```go
r := replica.NewLWWMap[string](1, crdt.StringCodec{})
r.Put("name", "alice")

// Direct typed read.
value, dot, ok := r.Data.Get("name")

// Iterate all entries.
r.Data.Range(func(key string, value string, dot crdt.Dot) bool {
    fmt.Printf("%s = %s (dot: %v)\n", key, value, dot)
    return true
})
```

## Testing

```
go test ./...                    # all tests
go test ./... -cover             # with coverage
go test ./replica/... -v         # replica layer verbose
```

## License

See LICENSE file.
