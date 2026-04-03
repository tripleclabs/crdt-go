package crdt

// localClock is a monotonic counter for a single replica. It produces
// [Dot] values for stamping local mutations. Only the owning replica
// should call [localClock.Next].
type localClock struct {
	replica ReplicaID
	counter uint64
}

// newLocalClock returns a LocalClock for the given replica, starting at 0.
func newLocalClock(replica ReplicaID) *localClock {
	return &localClock{replica: replica}
}

// Next increments the counter and returns the next [Dot].
func (lc *localClock) Next() Dot {
	lc.counter++
	return Dot{Replica: lc.replica, Counter: lc.counter}
}

// Current returns the current counter value as a [Dot] without incrementing.
func (lc *localClock) Current() Dot {
	return Dot{Replica: lc.replica, Counter: lc.counter}
}

// Counter returns the raw counter value.
func (lc *localClock) Counter() uint64 {
	return lc.counter
}

// Replica returns the replica ID.
func (lc *localClock) Replica() ReplicaID {
	return lc.replica
}

// SetCounter sets the counter to a specific value. Used when restoring
// from persisted state.
func (lc *localClock) SetCounter(counter uint64) {
	lc.counter = counter
}
