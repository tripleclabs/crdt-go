package crdt

// CRDTKind is implemented by kind marker types that tell [DeltaMap] how
// to create inner CRDT states. Each kind carries the codec needed to
// construct the inner state.
type CRDTKind interface {
	newState() any
	// parseDelta delegates to the inner state's ParseDelta for dot tracking.
	parseDelta(state any, delta []byte) (deltaInfo, error)
	// applyDelta delegates to the inner state's Apply.
	applyDelta(state any, delta []byte) error
	// deltasSince delegates to the inner state's DeltasSince.
	deltasSince(state any, hwm VClock) [][]byte
	// entryMeta delegates to the inner state's EntryMeta.
	entryMeta(state any, key string) ([]byte, bool)
	// removeAll prunes inner state: removes all entries whose dots are
	// fully dominated by the given causal context. Entries with dots NOT
	// dominated survive (add-wins). Used by DeltaMap cascading remove.
	removeAll(state any, ctx VClock)
}

// Mutation is a type-safe mutation for a [DeltaMap] parameterized on kind K.
// The forKind method is unexported, so only library-defined mutations compile.
type Mutation[K any] interface {
	forKind(K)
	applyMutation(state any, d Dot, hwm VClock) (delta []byte, err error)
}

// Query is a type-safe query for a [DeltaMap] parameterized on kind K.
type Query[K any] interface {
	forKind(K)
	execQuery(state any) any
}

// ---------------------------------------------------------------------------
// ORSetKind
// ---------------------------------------------------------------------------

// ORSetKind is a [CRDTKind] for [DeltaMap] entries that are observed-remove sets.
type ORSetKind[E any] struct {
	Codec Codec[E]
}

func (k ORSetKind[E]) newState() any {
	return newORSetState(k.Codec)
}

func (k ORSetKind[E]) parseDelta(state any, delta []byte) (deltaInfo, error) {
	return state.(*orSetState[E]).ParseDelta(delta)
}

func (k ORSetKind[E]) applyDelta(state any, delta []byte) error {
	return state.(*orSetState[E]).Apply(delta)
}

func (k ORSetKind[E]) deltasSince(state any, hwm VClock) [][]byte {
	return state.(*orSetState[E]).DeltasSince(hwm)
}

func (k ORSetKind[E]) entryMeta(state any, key string) ([]byte, bool) {
	return state.(*orSetState[E]).EntryMeta(key)
}

func (k ORSetKind[E]) removeAll(state any, ctx VClock) {
	s := state.(*orSetState[E])
	// Iterate all elements, prune dots dominated by the remove context.
	// Collect keys to process first to avoid mutating during iteration.
	type entry struct {
		key  string
		dots DotMap
	}
	var entries []entry
	s.Range(func(elemKey string, dots DotMap) bool {
		entries = append(entries, entry{elemKey, dots})
		return true
	})
	for _, e := range entries {
		surviving := make(DotMap)
		for rep, counter := range e.dots {
			if counter > ctx.Get(rep) {
				surviving[rep] = counter
			}
		}
		if len(surviving) > 0 {
			s.PutEncoded(e.key, surviving)
		} else {
			s.RemoveEncoded(e.key)
		}
	}
}

// --- ORSet Mutations ---

// AddSetMember adds an element to an ORSet entry in a [DeltaMap].
type AddSetMember[E any] struct{ Value E }

func (AddSetMember[E]) forKind(ORSetKind[E]) {}
func (m AddSetMember[E]) applyMutation(state any, d Dot, _ VClock) ([]byte, error) {
	return state.(*orSetState[E]).Add(m.Value, d)
}

// RemoveSetMember removes an element from an ORSet entry in a [DeltaMap].
type RemoveSetMember[E any] struct{ Value E }

func (RemoveSetMember[E]) forKind(ORSetKind[E]) {}
func (m RemoveSetMember[E]) applyMutation(state any, d Dot, hwm VClock) ([]byte, error) {
	return state.(*orSetState[E]).Remove(m.Value, d, hwm)
}

// --- ORSet Queries ---

// ContainsSetMember checks if an element exists in an ORSet entry.
type ContainsSetMember[E any] struct{ Value E }

func (ContainsSetMember[E]) forKind(ORSetKind[E]) {}
func (q ContainsSetMember[E]) execQuery(state any) any {
	return state.(*orSetState[E]).Contains(q.Value)
}

// SetElements returns all elements in an ORSet entry.
type SetElements[E any] struct{}

func (SetElements[E]) forKind(ORSetKind[E]) {}
func (q SetElements[E]) execQuery(state any) any {
	elems, _ := state.(*orSetState[E]).Elements()
	return elems
}

// SetLen returns the number of elements in an ORSet entry.
type SetLen[E any] struct{}

func (SetLen[E]) forKind(ORSetKind[E]) {}
func (q SetLen[E]) execQuery(state any) any {
	return state.(*orSetState[E]).Len()
}
