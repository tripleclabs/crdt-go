# crdt

Pure-Go CRDT library with clean separation of storage, causality, and replication.

```
go get github.com/3clabs/crdt
```

## Architecture

Everything lives in the `crdt` package. The library composes three orthogonal concerns:

1. **Storage types** — CRDT data structures (`LWWMap`, `ORSet`, `GCounter`, etc.) that implement `Mergeable`. They own data and delta encoding but contain no clocks.

2. **Clock strategies** — Pluggable conflict resolution (`LWWClock`, `AddWinsClock`, `MaxWinsClock`, `AlwaysMergeClock`) that decide whether an incoming delta should be applied.

3. **Replication** — `Replica[M]` wraps a storage type with a clock strategy and causal bookkeeping (`LocalClock` + `ReceivedClock`). `DurableReplica[M]` optionally wraps `Replica[M]` with topology-aware write concerns.

These concerns are independent — you choose a storage type, a clock strategy (usually via a factory function), a backend, and optionally a write concern level.

## Quick start

```go
import "github.com/3clabs/crdt"

// Create replicas on two nodes.
nodeA := crdt.NewLWWMapReplica[string](1, crdt.StringCodec{})
nodeB := crdt.NewLWWMapReplica[string](2, crdt.StringCodec{})

// Mutate locally — returns delta bytes to send to peers.
deltaA, _ := nodeA.Data.Put("name", "alice", nodeA.NextDot())
deltaB, _ := nodeB.Data.Put("color", "blue", nodeB.NextDot())

// Send deltas over any transport (gRPC, NATS, TCP, ...).
nodeB.ApplyDelta(deltaA)
nodeA.ApplyDelta(deltaB)

// Both nodes converge.
v, _, _ := nodeA.Data.Get("color")  // "blue"
v, _, _ = nodeB.Data.Get("name")    // "alice"
```

## Replica

`Replica[M Mergeable]` is the core replication wrapper. It composes:

- `Data M` — the CRDT storage type (use for reads and mutations)
- `Strategy Clock` — conflict resolution rule
- `Local *LocalClock` — stamps dots for local mutations
- `Received *ReceivedClock` — tracks which remote dots have been processed

```go
type Replica[M Mergeable] struct {
    Data     M
    Strategy Clock
    Local    *LocalClock
    Received *ReceivedClock
}
```

**Key methods:**

| Method | Description |
|--------|-------------|
| `NextDot() Dot` | Stamp and return the next dot for a local mutation |
| `ApplyDelta(delta []byte) error` | Apply an incoming delta from a remote peer |
| `DeltasSince(peerHWM VClock) [][]byte` | Return deltas the peer hasn't seen (for anti-entropy) |
| `HWM() VClock` | Return the received high-water mark vector clock |

A `Replica` is **not** safe for concurrent use. Serialise all mutations, delta applications, and reads.

**Factory functions** create replicas with the correct clock strategy:

```go
crdt.NewLWWMapReplica[V](replicaID, codec, opts...)        // LWWMap + LWWClock
crdt.NewORSetReplica[E](replicaID, codec, opts...)          // ORSet + AlwaysMergeClock
crdt.NewORMapReplica[V](replicaID, codec, opts...)          // ORMap + AlwaysMergeClock
crdt.NewAWLWWMapReplica[V](replicaID, codec, opts...)       // AWLWWMap + AddWinsClock
crdt.NewGListReplica[V](replicaID, codec, opts...)          // GList + AlwaysMergeClock
crdt.NewLWWRegisterReplica[V](replicaID, codec)             // LWWRegister + LWWClock
crdt.NewMVRegisterReplica[V](replicaID, codec)              // MVRegister + AlwaysMergeClock
crdt.NewGCounterReplica(replicaID)                          // GCounter + MaxWinsClock
crdt.NewPNCounterReplica(replicaID)                         // PNCounter + AlwaysMergeClock
```

Or build your own with `NewReplica[M](replicaID, data, clock)`.

## CRDT types

### LWWMap[V] — Last-write-wins map

Per-key last-write-wins. Higher dot counter wins; lower replica ID breaks ties. Supports tombstones for deletes.

```go
r := crdt.NewLWWMapReplica[string](1, crdt.StringCodec{})

delta, _ := r.Data.Put("user:1", "alice", r.NextDot())   // returns delta bytes
rmDelta := r.Data.Remove("user:1", r.NextDot())          // tombstone

v, dot, ok := r.Data.Get("user:1")                       // typed read
raw, dot, ok := r.Data.GetBytes("user:1")                // raw bytes
tombDot, ok := r.Data.GetTombstone("user:1")              // tombstone dot
r.Data.Range(func(key string, value string, dot crdt.Dot) bool { return true })
r.Data.Len()                                              // entry count
r.Data.TombstoneLen()                                     // tombstone count
```

### AWLWWMap[V] — Add-wins LWW map

Like LWWMap but with add-wins bias: a concurrent put beats a concurrent remove. Tombstones carry causal context (a `VClock`) to determine coverage.

```go
r := crdt.NewAWLWWMapReplica[string](1, crdt.StringCodec{})

delta, _ := r.Data.Put("key", "val", r.NextDot())
rmDelta := r.Data.Remove("key", r.NextDot(), r.HWM())    // context = current HWM

v, dot, ok := r.Data.Get("key")
tombDot, ctx, ok := r.Data.GetTombstone("key")            // dot + causal context
```

### ORSet[E] — Observed-remove set

Add-wins set. Each element tracks a `DotMap` of contributing replicas. Removes require the current HWM as causal context.

```go
r := crdt.NewORSetReplica[string](1, crdt.StringCodec{})

delta, _ := r.Data.Add("item", r.NextDot())
rmDelta, _ := r.Data.Remove("item", r.HWM())             // context-based remove

ok := r.Data.Contains("item")
elems, _ := r.Data.Elements()                             // []string
dots, ok := r.Data.Get("item")                            // DotMap
r.Data.Len()
```

### ORMap[V] — Observed-remove map

Like ORSet but maps keys to values. Each key tracks a `DotMap`. Add-wins semantics.

```go
r := crdt.NewORMapReplica[string](1, crdt.StringCodec{})

delta, _ := r.Data.Put("key", "val", r.NextDot())
rmDelta := r.Data.Remove("key", r.HWM())

v, dots, ok := r.Data.Get("key")                          // value + DotMap
r.Data.Range(func(key string, value string, dots crdt.DotMap) bool { return true })
```

### GList[V] — Grow-only list

Append-only list with causal ordering. Items are keyed by their dot.

```go
r := crdt.NewGListReplica[string](1, crdt.StringCodec{})

delta, _ := r.Data.Append("entry", r.NextDot())

items, _ := r.Data.Items()                                // []string in causal order
ok := r.Data.Has(dot)
r.Data.Len()
```

### LWWRegister[V] — Last-write-wins register

Single value with LWW conflict resolution.

```go
r := crdt.NewLWWRegisterReplica[string](1, crdt.StringCodec{})

delta, _ := r.Data.Set("value", r.NextDot())

v, dot, ok := r.Data.Get()
ok := r.Data.HasValue()
```

### MVRegister[V] — Multi-value register

Preserves all concurrent writes. A subsequent write with causal context resolves the conflict.

```go
r := crdt.NewMVRegisterReplica[string](1, crdt.StringCodec{})

delta, _ := r.Data.Write("value", r.NextDot(), r.HWM())  // context prunes old values

values, _ := r.Data.Values()                              // []string (all concurrent)
r.Data.Len()                                              // number of concurrent values
```

### GCounter — Grow-only counter

Each replica maintains its own count. Total = sum of all replicas.

```go
r := crdt.NewGCounterReplica(1)

delta := r.Data.Increment(r.Local.Replica(), 1)

total := r.Data.Int64()
count := r.Data.Get(replicaID)                            // per-replica count
```

### PNCounter — Positive-negative counter

Two-sided counter: increment and decrement. Value = sum(positive) - sum(negative).

```go
r := crdt.NewPNCounterReplica(1)

delta := r.Data.Increment(r.Local.Replica(), 5, r.NextDot())
delta = r.Data.Decrement(r.Local.Replica(), 2, r.NextDot())

total := r.Data.Int64()                                   // 3
```

## Anti-entropy

When deltas may be lost (network partitions, restarts), use high-water marks to catch up:

```go
// Node A asks B: "what have you seen?"
peerHWM := nodeB.HWM()

// Node A sends what B is missing.
for _, delta := range nodeA.DeltasSince(peerHWM) {
    nodeB.ApplyDelta(delta)
}
```

`ReceivedClock` tracks contiguous receipt per replica. If deltas arrive as 1, 3, 5, the high-water mark stays at 1 until 2 arrives, then advances to 3 (or 5 if both fill). This means `DeltasSince` always returns a correct, minimal set of missing deltas.

For large datasets, use `MerkleMap` to detect divergence cheaply before running a full anti-entropy exchange:

```go
mmA := crdt.NewMerkleMap()
mmB := crdt.NewMerkleMap()
// ... populate from CRDT state ...

if !mmA.Equal(mmB) {
    keys := mmA.DivergentKeys(mmB)  // only sync these keys
}
```

## Codecs

Collection types are generic over their value type. Values are encoded/decoded via a `Codec[V]`:

```go
type Codec[V any] interface {
    Encode(V) ([]byte, error)
    Decode([]byte) (V, error)
}
```

Built-in codecs:

| Codec | Type | Encoding |
|-------|------|----------|
| `StringCodec` | `string` | UTF-8 bytes |
| `Int64Codec` | `int64` | 8 bytes big-endian |
| `Uint64Codec` | `uint64` | 8 bytes big-endian |
| `BytesCodec` | `[]byte` | passthrough |

Custom types — implement `Codec[V]`:

```go
type UserCodec struct{}

func (UserCodec) Encode(u User) ([]byte, error) { return json.Marshal(u) }
func (UserCodec) Decode(b []byte) (User, error) {
    var u User
    return u, json.Unmarshal(b, &u)
}

r := crdt.NewLWWMapReplica[User](replicaID, UserCodec{})
r.Data.Put("user:123", User{Name: "alice", Age: 30}, r.NextDot())
```

## Storage backends

Collection types (maps, sets, lists) use a pluggable `Backend`:

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

The default is `MemoryBackend` (Go maps). The `crdtbolt` sub-module provides a bbolt-backed implementation for data that exceeds memory:

```go
import "github.com/3clabs/crdt/crdtbolt"

backend, _ := crdtbolt.Open("/path/to/data.db")
defer backend.Close()

r := crdt.NewLWWMapReplica[string](replicaID, crdt.StringCodec{},
    crdt.WithBackend(backend))
```

Implement `Backend` for any storage engine (SQLite, Redis, etc.). Backends must be safe for sequential use but need not be safe for concurrent use.

## Clock strategies

Each `Replica` uses a `Clock` to decide whether an incoming delta should be applied:

```go
type Clock interface {
    Allows(local Queryable, info DeltaInfo) bool
}
```

Built-in strategies:

| Clock | Semantics | Used by |
|-------|-----------|---------|
| `LWWClock` | Higher dot wins, lower replica ID breaks ties | LWWMap, LWWRegister |
| `AddWinsClock` | Concurrent put beats concurrent remove (tombstones carry causal context) | AWLWWMap |
| `MaxWinsClock` | Higher count wins | GCounter |
| `AlwaysMergeClock` | Always apply; merge logic is in `Apply()` | ORSet, ORMap, MVRegister, GList, PNCounter |

Implement `Clock` for custom conflict resolution. The `DeltaInfo` passed to `Allows` contains:

```go
type DeltaInfo struct {
    Op      byte    // OpPut (0x01) or OpRemove (0x02)
    Key     string  // target key or element
    Meta    []byte  // remote causal metadata (dot, dotmap, or count)
    Context []byte  // causal context for add-wins removes (VClock)
    Dots    []Dot   // all causal dots in this delta
}
```

## Causality primitives

### Dot

A single causal event: `(ReplicaID, Counter)`. Every mutation produces a new dot.

```go
dot := replica.NextDot()  // Dot{Replica: 1, Counter: 1}
```

`DotGT(a, b)` — higher counter wins; equal counters broken by lower replica ID.

### DotMap

`map[ReplicaID]uint64` — compressed vector clock tracking which replicas contributed to an element. Used inside ORSet and ORMap entries.

### VClock

`map[ReplicaID]uint64` — vector clock for causal ordering.

```go
vc := crdt.NewVClock()
vc = vc.Increment(replicaID)

a.LTE(b)           // a causally before or equal to b?
a.Dominates(b)     // a causally after b?
a.Concurrent(b)    // neither dominates?
a.Merge(b)         // join (max per replica)
a.LowerBound(b)    // meet (min per replica, intersection only)
a.Clone()
a.Equal(b)
a.Fingerprint()    // fast hash for inequality checks
```

### LocalClock

Monotonic counter for stamping local mutations. Owned by `Replica`.

```go
lc := crdt.NewLocalClock(replicaID)
dot := lc.Next()           // increment and return dot
dot = lc.Current()         // current dot without incrementing
lc.SetCounter(100)         // restore from persisted state
```

### ReceivedClock

Tracks contiguous receipt per remote replica, with gap buffering.

```go
rc := crdt.NewReceivedClock()
rc.Record(replicaID, 1)       // record receipt
rc.Record(replicaID, 3)       // out of order — buffered
rc.Record(replicaID, 2)       // fills gap — HWM advances to 3
hwm := rc.HWM()               // VClock for anti-entropy
rc.Covers(replicaID, 2)       // true
rc.SetHWM(replicaID, 10)      // restore from persisted state
```

## Write concerns

`DurableReplica[M]` wraps a `Replica[M]` with topology-aware write concerns. Writes still use CRDT semantics — the write concern adds a propagation guarantee that the delta has been applied on multiple peers before returning.

```go
inner := crdt.NewLWWMapReplica[string](myID, crdt.StringCodec{})
topo := crdt.NewStaticTopology(peer1, peer2)
dr := crdt.NewDurableReplica(inner, topo, myTransport,
    crdt.WithWriteConcern(crdt.WMajority),
    crdt.WithPropagateTimeout(3*time.Second),
)

dot := dr.NextDot()
delta, _ := dr.Data.Put("key", "val", dot)
err := dr.Propagate(ctx, dot, delta)
// err == nil: quorum reached
// err == ErrQuorumTimeout: locally applied, quorum not reached — anti-entropy will sync later
// err == ErrClosed: replica was shut down
```

### Write concern levels

| Level | Behavior |
|-------|----------|
| `WLocal` | Send to all peers, return immediately (default) |
| `WMajority` | Block until `floor(n/2)+1` nodes have the write (counting local) |
| `WAll` | Block until every peer has acknowledged |

### Topology provider

`TopologyProvider` supplies the current set of peer replica IDs:

```go
type TopologyProvider interface {
    Peers() []ReplicaID  // must NOT include the local replica
}
```

`StaticTopology` is a built-in implementation for fixed peer sets:

```go
topo := crdt.NewStaticTopology(2, 3, 4)
```

Implement `TopologyProvider` for dynamic membership (service discovery, gossip, etc.). Membership is snapshotted at write time — changes during a pending write don't affect the in-flight quorum.

### Transport

Write concerns require a transport that can send deltas to specific peers and deliver acknowledgements:

```go
// Base interface — fire-and-forget delta sending.
type Transport interface {
    Send(ctx context.Context, peer ReplicaID, dot Dot, delta []byte) error
}

// Extends Transport with ack delivery for write concerns.
type AckTransport interface {
    Transport
    OnAck(fn func(peer ReplicaID, dot Dot))
}
```

A single concrete type satisfies both interfaces. The implementation owns the network layer (gRPC, NATS, libp2p, etc.). On the receiving side, the transport must:

1. Receive `(dot, delta)` from the wire
2. Call `peer.ApplyDelta(delta)` on the receiving replica
3. Send `ack(dot)` back to the sender
4. When the sender's transport receives the ack, call the `OnAck` callback

### Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `WithWriteConcern(wc)` | `WLocal` | Write concern level |
| `WithPropagateTimeout(d)` | 5s | Default timeout when context has no deadline |

Call `dr.Close()` during graceful shutdown to unblock pending writes.

## Delta wire format

Each mutation returns `[]byte` — the encoded delta ready to send. Each type has its own compact binary format. Example for LWWMap:

| Op | Format |
|----|--------|
| Put | `[0x01][varint key len][key][varint val len][val][16-byte dot]` |
| Remove | `[0x02][varint key len][key][16-byte dot]` |

Dots are encoded as 16 bytes: `[8-byte replica big-endian][8-byte counter big-endian]`. DotMaps and VClocks use a 4-byte count prefix followed by sorted 16-byte entries.

Deltas are **idempotent** — applying the same delta twice is safe. Deltas are **commutative** — order doesn't matter. These properties mean you can send deltas over unreliable transports without coordination.

## Persistence and recovery

The library does not persist state automatically. To recover after a restart:

1. **Restore the backend** — Use a disk-backed `Backend` like `crdtbolt`, or reload entries into a `MemoryBackend`.
2. **Restore the local clock** — Call `replica.Local.SetCounter(n)` with the last known counter value.
3. **Restore the received clock** — Call `replica.Received.SetHWM(replicaID, n)` for each known peer.

After restoring, run anti-entropy with peers to catch up on any missed deltas.

## Concurrency

`Replica` and `DurableReplica` are **not** safe for concurrent use. All mutations, delta applications, and reads must be serialised by the caller. The `DurableReplica.Propagate` method is internally synchronised for ack tracking, but the underlying `Data` and `Replica` methods are not.

Backends follow the same rule: safe for sequential use, not concurrent use.

## Testing

```
go test ./...                         # all tests
go test ./... -race                   # with race detector
go test ./... -cover                  # with coverage
go test -run TestDurable -v           # write concern tests only
```

## License

See LICENSE file.
