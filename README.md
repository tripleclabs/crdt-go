# crdt

Pure-Go CRDT library. Pick a type, wire up a transport, and state synchronizes automatically.

```
go get github.com/3clabs/crdt
```

## Quick start

```go
import "github.com/3clabs/crdt"

// Create two nodes connected via your transport.
a := crdt.NewLWWMap[string](1, crdt.StringCodec{},
    crdt.WithTransport(myTransport),
    crdt.WithTopology(myTopology),
)
defer a.Close()

b := crdt.NewLWWMap[string](2, crdt.StringCodec{},
    crdt.WithTransport(myTransport),
    crdt.WithTopology(myTopology),
)
defer b.Close()

// Write on one node, read on the other.
a.Put(ctx, "name", "alice")
v, ok := b.Get("name") // "alice"
```

No deltas, no dots, no clocks — just `Put` and `Get`.

## CRDT types

### LWWMap[V] — Last-write-wins map

```go
m := crdt.NewLWWMap[string](id, crdt.StringCodec{}, opts...)

m.Put(ctx, "key", "val")   // (*WriteResult, error)
m.Remove(ctx, "key")       // *WriteResult
v, ok := m.Get("key")
m.Len()
m.Range(func(key string, value string) bool { return true })
```

### ORSet[E] — Observed-remove set

```go
s := crdt.NewORSet[string](id, crdt.StringCodec{}, opts...)

s.Add(ctx, "item")         // (*WriteResult, error)
s.Remove(ctx, "item")      // error
s.Contains("item")
elems, _ := s.Elements()
s.Len()
```

### ORMap[V] — Observed-remove map

```go
m := crdt.NewORMap[string](id, crdt.StringCodec{}, opts...)

m.Put(ctx, "key", "val")   // (*WriteResult, error)
m.Remove(ctx, "key")
v, ok := m.Get("key")
m.Len()
m.Range(func(key string, value string) bool { return true })
```

### DeltaMap[K] — Composable CRDT map

A map whose values are themselves CRDTs. Uses a shared causal context, composite deltas, and DSON-style tombstone-free removes with add-wins semantics.

The kind type parameter `K` determines the inner CRDT type and constrains which mutations and queries are accepted at compile time.

```go
// Map of node IDs to sets of topics (pubsub registry).
byNode := crdt.NewDeltaMap(id, crdt.ORSetKind[string]{Codec: crdt.StringCodec{}}, opts...)

// Mutations — type-safe via phantom kind types.
byNode.Mutate(ctx, "node-1", crdt.AddSetMember[string]{Value: "chat.general"})
byNode.Mutate(ctx, "node-1", crdt.AddSetMember[string]{Value: "chat.dev"})

// Queries.
ok := byNode.Query("node-1", crdt.ContainsSetMember[string]{Value: "chat.general"}).(bool)
elems := byNode.Query("node-1", crdt.SetElements[string]{}).([]string)
count := byNode.Query("node-1", crdt.SetLen[string]{}).(int)

// Cascading remove — node leaves, all its topics are pruned.
// Tombstone-free: no metadata accumulates after removal.
// Concurrent adds from other nodes survive (add-wins).
byNode.RemoveKey(ctx, "node-1")

// Map-level queries.
byNode.HasKey("node-1")
byNode.Keys()
byNode.Len()
```

**Available ORSet mutations:** `AddSetMember[E]`, `RemoveSetMember[E]`

**Available ORSet queries:** `ContainsSetMember[E]`, `SetElements[E]`, `SetLen[E]`

### AWLWWMap[V] — Add-wins LWW map

Like LWWMap but concurrent puts beat concurrent removes.

```go
m := crdt.NewAWLWWMap[string](id, crdt.StringCodec{}, opts...)
// Same API as LWWMap.
```

### GList[V] — Grow-only list

```go
l := crdt.NewGList[string](id, crdt.StringCodec{}, opts...)

l.Append(ctx, "entry")     // (*WriteResult, error)
items, _ := l.Items()      // in causal order
l.Len()
```

### GCounter — Grow-only counter

```go
c := crdt.NewGCounter(id, opts...)

c.Increment(ctx, 5)        // *WriteResult
c.Int64()                   // total across all replicas
```

### PNCounter — Positive-negative counter

```go
c := crdt.NewPNCounter(id, opts...)

c.Increment(ctx, 10)       // *WriteResult
c.Decrement(ctx, 3)        // *WriteResult
c.Int64()                   // 7
```

### LWWRegister[V] — Last-write-wins register

```go
r := crdt.NewLWWRegister[string](id, crdt.StringCodec{}, opts...)

r.Set(ctx, "value")        // (*WriteResult, error)
v, ok := r.Get()
```

### MVRegister[V] — Multi-value register

Preserves all concurrent writes until a subsequent write resolves the conflict.

```go
r := crdt.NewMVRegister[string](id, crdt.StringCodec{}, opts...)

r.Write(ctx, "value")      // (*WriteResult, error)
vals, _ := r.Values()      // all concurrent values
r.Len()
```

## Configuration

All constructors accept functional options:

```go
crdt.NewLWWMap[string](id, codec,
    crdt.WithTransport(tr),
    crdt.WithTopology(topo),
    crdt.WithBackend(boltBackend),
    crdt.WithWriteConcern(crdt.WMajority),
    crdt.WithAntiEntropyInterval(2 * time.Second),
)
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithTransport(t)` | nil | Network transport for replication |
| `WithTopology(t)` | nil | Peer discovery provider |
| `WithBackend(b)` | in-memory | Pluggable storage backend |
| `WithWriteConcern(wc)` | `WLocal` | Quorum level for writes |
| `WithAntiEntropyInterval(d)` | 1s | Background sync interval (0 to disable) |

Without a transport, types work as local-only data structures.

## Write concerns

Mutations return a `*WriteResult`. Call `Wait` to block until the configured quorum is reached:

```go
wr, err := m.Put(ctx, "key", "val")
if err != nil {
    // local apply failed (e.g., codec error)
}
if err := wr.Wait(ctx); err != nil {
    // quorum not reached before context expired
}
```

| Level | Behavior |
|-------|----------|
| `WLocal` | Send to all peers, return immediately (default) |
| `WMajority` | Wait for `⌊n/2⌋+1` nodes (counting local) |
| `WAll` | Wait for every peer to acknowledge |

For `WLocal`, `Wait` returns nil immediately.

## Anti-entropy

Background anti-entropy runs automatically when a transport is configured. Each replica periodically exchanges state digests with peers and sends any missing deltas. This handles lost messages, network partitions, and late-joining peers — no user action required.

The sync interval is configurable via `WithAntiEntropyInterval`. Set to 0 to disable.

## Transport

Implement the `Transport` interface to connect replicas over your network:

```go
type Transport interface {
    Send(ctx context.Context, peer ReplicaID, msg TransportMessage) (<-chan struct{}, error)
    OnReceive(fn func(msg TransportMessage))
}
```

`TransportMessage` is opaque — the transport moves it between peers without inspecting its contents. On the receiving side, set `msg.From()` to the sender's ID and pass it to the `OnReceive` handler.

When `Send` is called with a message requesting an ack, return a channel that closes when the peer acknowledges receipt. For fire-and-forget messages, return nil.

## Topology

`TopologyProvider` tells the replica who its peers are:

```go
type TopologyProvider interface {
    Peers() []ReplicaID // must NOT include the local replica
}
```

Implement this for your membership system (static config, service discovery, gossip, etc.).

## Codecs

Collection types are generic over their value type. Values are encoded via `Codec[V]`:

```go
type Codec[V any] interface {
    Encode(V) ([]byte, error)
    Decode([]byte) (V, error)
}
```

Built-in: `StringCodec`, `Int64Codec`, `Uint64Codec`, `BytesCodec`.

Custom types — implement `Codec[V]`:

```go
type UserCodec struct{}
func (UserCodec) Encode(u User) ([]byte, error) { return json.Marshal(u) }
func (UserCodec) Decode(b []byte) (User, error) {
    var u User
    return u, json.Unmarshal(b, &u)
}

m := crdt.NewLWWMap[User](id, UserCodec{}, opts...)
```

## Storage backends

Collection types use a pluggable `Backend` for storage. The default is in-memory. The `crdtbolt` sub-module provides bbolt-backed persistence:

```go
import "github.com/3clabs/crdt/crdtbolt"

backend, _ := crdtbolt.Open("/path/to/data.db")
defer backend.Close()

m := crdt.NewLWWMap[string](id, crdt.StringCodec{},
    crdt.WithBackend(backend),
    crdt.WithTransport(tr),
    crdt.WithTopology(topo),
)
```

Implement `Backend` for any storage engine (SQLite, Redis, etc.).

## Concurrency

All CRDT types are safe for concurrent use. Reads and writes from any goroutine are synchronized internally.

## Testing

```
go test ./...
go test ./... -race
go test ./... -cover
```

## License

See LICENSE file.
