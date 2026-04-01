package crdt

// LocalClock is a monotonic counter for a single replica. It produces
// [Dot] values for stamping local mutations. Only the owning replica
// should call [LocalClock.Next].
type LocalClock struct {
	replica ReplicaID
	counter uint64
}

// NewLocalClock returns a LocalClock for the given replica, starting at 0.
func NewLocalClock(replica ReplicaID) *LocalClock {
	return &LocalClock{replica: replica}
}

// Next increments the counter and returns the next [Dot].
func (lc *LocalClock) Next() Dot {
	lc.counter++
	return Dot{Replica: lc.replica, Counter: lc.counter}
}

// Current returns the current counter value as a [Dot] without incrementing.
func (lc *LocalClock) Current() Dot {
	return Dot{Replica: lc.replica, Counter: lc.counter}
}

// Counter returns the raw counter value.
func (lc *LocalClock) Counter() uint64 {
	return lc.counter
}

// Replica returns the replica ID.
func (lc *LocalClock) Replica() ReplicaID {
	return lc.replica
}

// SetCounter sets the counter to a specific value. Used when restoring
// from persisted state.
func (lc *LocalClock) SetCounter(counter uint64) {
	lc.counter = counter
}
