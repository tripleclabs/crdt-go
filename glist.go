package crdt

import "sort"

// GList is a grow-only list CRDT. Elements can only be appended, never
// removed. Each element is tagged with the replica and counter at the time
// of append, providing causal ordering. Duplicate items (same replica and
// counter) are deduplicated during merge.
//
// The zero value is not usable; create instances with [NewGList].
type GList struct {
	replica ReplicaID
	items   []gListItem
	vclock  VClock
}

type gListItem struct {
	Value   any
	Replica ReplicaID
	Counter uint64
}

// NewGList returns a new GList owned by the given replica.
func NewGList(replica ReplicaID) *GList {
	return &GList{
		replica: replica,
		vclock:  NewVClock(),
	}
}

// Append adds a value to the end of the list and returns the new state with
// a [Delta]. The receiver is not modified.
func (l *GList) Append(value any) (*GList, *Delta) {
	newVC := l.vclock.Increment(l.replica)
	counter := newVC.Get(l.replica)

	item := gListItem{Value: value, Replica: l.replica, Counter: counter}
	newItems := make([]gListItem, len(l.items)+1)
	copy(newItems, l.items)
	newItems[len(l.items)] = item

	next := &GList{replica: l.replica, items: newItems, vclock: newVC}

	delta := &GList{
		replica: l.replica,
		items:   []gListItem{item},
		vclock:  VClock{l.replica: counter},
	}
	return next, &Delta{Type: TypeGList, State: delta}
}

// Value returns the list items as a []any in causal order (sorted by
// counter, then replica ID for deterministic ordering of concurrent appends).
func (l *GList) Value() any {
	return l.Items()
}

// Items returns the list items as a typed []any slice in causal order.
func (l *GList) Items() []any {
	sorted := make([]gListItem, len(l.items))
	copy(sorted, l.items)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Counter != sorted[j].Counter {
			return sorted[i].Counter < sorted[j].Counter
		}
		return sorted[i].Replica < sorted[j].Replica
	})
	out := make([]any, len(sorted))
	for i, item := range sorted {
		out[i] = item.Value
	}
	return out
}

// Len returns the number of items in the list.
func (l *GList) Len() int {
	return len(l.items)
}

// VClock returns the vector clock for this list.
func (l *GList) VClock() VClock {
	return l.vclock.Clone()
}

// Merge merges a remote GList state and returns the result. Items are
// deduplicated by (replica, counter) and the vclocks are merged. The
// receiver is not modified.
func (l *GList) Merge(other State) State {
	o := other.(*GList)
	mergedVC := l.vclock.Merge(o.vclock)

	type itemKey struct {
		replica ReplicaID
		counter uint64
	}
	seen := make(map[itemKey]struct{}, len(l.items))
	merged := make([]gListItem, 0, len(l.items)+len(o.items))

	for _, item := range l.items {
		k := itemKey{item.Replica, item.Counter}
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			merged = append(merged, item)
		}
	}
	for _, item := range o.items {
		k := itemKey{item.Replica, item.Counter}
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			merged = append(merged, item)
		}
	}

	return &GList{replica: l.replica, items: merged, vclock: mergedVC}
}

// CRDTType returns [TypeGList].
func (l *GList) CRDTType() TypeID {
	return TypeGList
}

// MarshalBinary encodes the GList into a binary format.
func (l *GList) MarshalBinary() ([]byte, error) {
	return gobEncode(l.replica, l.items, map[ReplicaID]uint64(l.vclock))
}

// UnmarshalBinary decodes a GList from binary format.
func (l *GList) UnmarshalBinary(data []byte) error {
	var vc map[ReplicaID]uint64
	if err := gobDecode(data, &l.replica, &l.items, &vc); err != nil {
		return err
	}
	l.vclock = VClock(vc)
	return nil
}
