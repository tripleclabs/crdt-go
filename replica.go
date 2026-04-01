package crdt

// ReplicaID uniquely identifies a replica (node or process) in a distributed
// system. Each replica that can independently mutate a CRDT must have its own
// ReplicaID. The type is uint64 for deterministic ordering (used in tie-breaking)
// and compact representation in maps and on the wire.
//
// Consumers are responsible for generating replica IDs — for example by hashing
// a node hostname, drawing from a sequence, or using a random uint64.
type ReplicaID = uint64
