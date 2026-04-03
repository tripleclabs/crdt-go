package crdt

import "context"

// ---------------------------------------------------------------------------
// LWWMap
// ---------------------------------------------------------------------------

// LWWMap is a last-writer-wins map. Mutations automatically propagate to
// peers when a [Transport] and [TopologyProvider] are configured.
type LWWMap[V any] struct {
	r *replica[*lwwMapState[V]]
}

// NewLWWMap creates a new LWWMap. Without [WithTransport] and [WithTopology]
// it operates as a local-only data structure.
func NewLWWMap[V any](id ReplicaID, codec Codec[V], opts ...Option) *LWWMap[V] {
	o := applyOptions(opts)
	var stateOpts []Option
	if o.backend != nil {
		stateOpts = append(stateOpts, WithBackend(o.backend))
	}
	state := newLWWMapState(codec, stateOpts...)
	r := newReplica(id, state, lwwClock{}, opts...)
	return &LWWMap[V]{r: r}
}

func (m *LWWMap[V]) Put(ctx context.Context, key string, value V) (*WriteResult, error) {
	m.r.mu.Lock()
	dot := m.r.nextDot()
	delta, err := m.r.data.Put(key, value, dot)
	if err != nil {
		m.r.mu.Unlock()
		return nil, err
	}
	m.r.trackKey(key)
	m.r.mu.Unlock()
	return m.r.propagate(ctx, dot, delta), nil
}

func (m *LWWMap[V]) Remove(ctx context.Context, key string) *WriteResult {
	m.r.mu.Lock()
	dot := m.r.nextDot()
	delta := m.r.data.Remove(key, dot)
	m.r.trackKey(key)
	m.r.mu.Unlock()
	return m.r.propagate(ctx, dot, delta)
}

func (m *LWWMap[V]) Get(key string) (V, bool) {
	m.r.mu.Lock()
	defer m.r.mu.Unlock()
	v, _, ok := m.r.data.Get(key)
	return v, ok
}

func (m *LWWMap[V]) Len() int {
	m.r.mu.Lock()
	defer m.r.mu.Unlock()
	return m.r.data.Len()
}

func (m *LWWMap[V]) Range(fn func(key string, value V) bool) {
	m.r.mu.Lock()
	defer m.r.mu.Unlock()
	m.r.data.Range(func(key string, value V, _ Dot) bool {
		return fn(key, value)
	})
}

func (m *LWWMap[V]) Close() { m.r.Close() }

// ---------------------------------------------------------------------------
// ORSet
// ---------------------------------------------------------------------------

// ORSet is an observed-remove set.
type ORSet[E any] struct {
	r *replica[*orSetState[E]]
}

func NewORSet[E any](id ReplicaID, codec Codec[E], opts ...Option) *ORSet[E] {
	o := applyOptions(opts)
	var stateOpts []Option
	if o.backend != nil {
		stateOpts = append(stateOpts, WithBackend(o.backend))
	}
	state := newORSetState(codec, stateOpts...)
	r := newReplica(id, state, alwaysMergeClock{}, opts...)
	return &ORSet[E]{r: r}
}

func (s *ORSet[E]) Add(ctx context.Context, elem E) (*WriteResult, error) {
	s.r.mu.Lock()
	dot := s.r.nextDot()
	delta, err := s.r.data.Add(elem, dot)
	if err != nil {
		s.r.mu.Unlock()
		return nil, err
	}
	// ORSet Add key is the encoded element; get it from ParseDelta.
	if info, e := s.r.data.ParseDelta(delta); e == nil {
		s.r.trackKey(info.Key)
	}
	s.r.mu.Unlock()
	return s.r.propagate(ctx, dot, delta), nil
}

// Remove removes an element. Remove deltas are context-based (no dot)
// and are broadcast fire-and-forget.
func (s *ORSet[E]) Remove(ctx context.Context, elem E) error {
	s.r.mu.Lock()
	delta, err := s.r.data.Remove(elem, s.r.received.HWM())
	if err != nil {
		s.r.mu.Unlock()
		return err
	}
	if info, e := s.r.data.ParseDelta(delta); e == nil {
		s.r.trackKey(info.Key)
	}
	s.r.mu.Unlock()
	s.r.broadcast(ctx, Dot{}, delta)
	return nil
}

func (s *ORSet[E]) Contains(elem E) bool {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	return s.r.data.Contains(elem)
}

func (s *ORSet[E]) Elements() ([]E, error) {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	return s.r.data.Elements()
}

func (s *ORSet[E]) Len() int {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	return s.r.data.Len()
}

func (s *ORSet[E]) Close() { s.r.Close() }

// ---------------------------------------------------------------------------
// ORMap
// ---------------------------------------------------------------------------

// ORMap is an observed-remove map.
type ORMap[V any] struct {
	r *replica[*orMapState[V]]
}

func NewORMap[V any](id ReplicaID, codec Codec[V], opts ...Option) *ORMap[V] {
	o := applyOptions(opts)
	var stateOpts []Option
	if o.backend != nil {
		stateOpts = append(stateOpts, WithBackend(o.backend))
	}
	state := newORMapState(codec, stateOpts...)
	r := newReplica(id, state, alwaysMergeClock{}, opts...)
	return &ORMap[V]{r: r}
}

func (m *ORMap[V]) Put(ctx context.Context, key string, value V) (*WriteResult, error) {
	m.r.mu.Lock()
	dot := m.r.nextDot()
	delta, err := m.r.data.Put(key, value, dot)
	if err != nil {
		m.r.mu.Unlock()
		return nil, err
	}
	m.r.trackKey(key)
	m.r.mu.Unlock()
	return m.r.propagate(ctx, dot, delta), nil
}

func (m *ORMap[V]) Remove(ctx context.Context, key string) {
	m.r.mu.Lock()
	delta := m.r.data.Remove(key, m.r.received.HWM())
	m.r.trackKey(key)
	m.r.mu.Unlock()
	m.r.broadcast(ctx, Dot{}, delta)
}

func (m *ORMap[V]) Get(key string) (V, bool) {
	m.r.mu.Lock()
	defer m.r.mu.Unlock()
	v, _, ok := m.r.data.Get(key)
	return v, ok
}

func (m *ORMap[V]) Len() int {
	m.r.mu.Lock()
	defer m.r.mu.Unlock()
	return m.r.data.Len()
}

func (m *ORMap[V]) Range(fn func(key string, value V) bool) {
	m.r.mu.Lock()
	defer m.r.mu.Unlock()
	m.r.data.Range(func(key string, value V, _ DotMap) bool {
		return fn(key, value)
	})
}

func (m *ORMap[V]) Close() { m.r.Close() }

// ---------------------------------------------------------------------------
// AWLWWMap
// ---------------------------------------------------------------------------

// AWLWWMap is an add-wins last-writer-wins map.
type AWLWWMap[V any] struct {
	r *replica[*awLWWMapState[V]]
}

func NewAWLWWMap[V any](id ReplicaID, codec Codec[V], opts ...Option) *AWLWWMap[V] {
	o := applyOptions(opts)
	var stateOpts []Option
	if o.backend != nil {
		stateOpts = append(stateOpts, WithBackend(o.backend))
	}
	state := newAWLWWMapState(codec, stateOpts...)
	r := newReplica(id, state, addWinsClock{}, opts...)
	return &AWLWWMap[V]{r: r}
}

func (m *AWLWWMap[V]) Put(ctx context.Context, key string, value V) (*WriteResult, error) {
	m.r.mu.Lock()
	dot := m.r.nextDot()
	delta, err := m.r.data.Put(key, value, dot)
	if err != nil {
		m.r.mu.Unlock()
		return nil, err
	}
	m.r.trackKey(key)
	m.r.mu.Unlock()
	return m.r.propagate(ctx, dot, delta), nil
}

func (m *AWLWWMap[V]) Remove(ctx context.Context, key string) *WriteResult {
	m.r.mu.Lock()
	dot := m.r.nextDot()
	delta := m.r.data.Remove(key, dot, m.r.received.HWM())
	m.r.trackKey(key)
	m.r.mu.Unlock()
	return m.r.propagate(ctx, dot, delta)
}

func (m *AWLWWMap[V]) Get(key string) (V, bool) {
	m.r.mu.Lock()
	defer m.r.mu.Unlock()
	v, _, ok := m.r.data.Get(key)
	return v, ok
}

func (m *AWLWWMap[V]) Len() int {
	m.r.mu.Lock()
	defer m.r.mu.Unlock()
	return m.r.data.Len()
}

func (m *AWLWWMap[V]) Range(fn func(key string, value V) bool) {
	m.r.mu.Lock()
	defer m.r.mu.Unlock()
	m.r.data.Range(func(key string, value V, _ Dot) bool {
		return fn(key, value)
	})
}

func (m *AWLWWMap[V]) Close() { m.r.Close() }

// ---------------------------------------------------------------------------
// GList
// ---------------------------------------------------------------------------

// GList is a grow-only list.
type GList[V any] struct {
	r *replica[*gListState[V]]
}

func NewGList[V any](id ReplicaID, codec Codec[V], opts ...Option) *GList[V] {
	o := applyOptions(opts)
	var stateOpts []Option
	if o.backend != nil {
		stateOpts = append(stateOpts, WithBackend(o.backend))
	}
	state := newGListState(codec, stateOpts...)
	r := newReplica(id, state, alwaysMergeClock{}, opts...)
	return &GList[V]{r: r}
}

func (l *GList[V]) Append(ctx context.Context, value V) (*WriteResult, error) {
	l.r.mu.Lock()
	dot := l.r.nextDot()
	delta, err := l.r.data.Append(value, dot)
	if err != nil {
		l.r.mu.Unlock()
		return nil, err
	}
	if info, e := l.r.data.ParseDelta(delta); e == nil {
		l.r.trackKey(info.Key)
	}
	l.r.mu.Unlock()
	return l.r.propagate(ctx, dot, delta), nil
}

func (l *GList[V]) Items() ([]V, error) {
	l.r.mu.Lock()
	defer l.r.mu.Unlock()
	return l.r.data.Items()
}

func (l *GList[V]) Len() int {
	l.r.mu.Lock()
	defer l.r.mu.Unlock()
	return l.r.data.Len()
}

func (l *GList[V]) Close() { l.r.Close() }

// ---------------------------------------------------------------------------
// GCounter
// ---------------------------------------------------------------------------

// GCounter is a grow-only counter.
type GCounter struct {
	r *replica[*gCounterState]
}

func NewGCounter(id ReplicaID, opts ...Option) *GCounter {
	state := newGCounterState()
	r := newReplica(id, state, maxWinsClock{}, opts...)
	return &GCounter{r: r}
}

func (c *GCounter) Increment(ctx context.Context, amount uint64) *WriteResult {
	c.r.mu.Lock()
	rid := c.r.local.Replica()
	delta := c.r.data.Increment(rid, amount)
	newCount := c.r.data.Get(rid)
	c.r.local.SetCounter(newCount)
	c.r.received.Record(rid, newCount)
	c.r.trackKey(formatReplicaKey(rid))
	c.r.mu.Unlock()
	return c.r.propagate(ctx, Dot{Replica: rid, Counter: newCount}, delta)
}

func (c *GCounter) Int64() int64 {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	return c.r.data.Int64()
}

func (c *GCounter) Close() { c.r.Close() }

// ---------------------------------------------------------------------------
// PNCounter
// ---------------------------------------------------------------------------

// PNCounter is a positive-negative counter.
type PNCounter struct {
	r *replica[*pnCounterState]
}

func NewPNCounter(id ReplicaID, opts ...Option) *PNCounter {
	state := newPNCounterState()
	r := newReplica(id, state, alwaysMergeClock{}, opts...)
	return &PNCounter{r: r}
}

func (c *PNCounter) Increment(ctx context.Context, amount uint64) *WriteResult {
	c.r.mu.Lock()
	dot := c.r.nextDot()
	delta := c.r.data.Increment(c.r.local.Replica(), amount, dot)
	c.r.trackKey("")
	c.r.mu.Unlock()
	return c.r.propagate(ctx, dot, delta)
}

func (c *PNCounter) Decrement(ctx context.Context, amount uint64) *WriteResult {
	c.r.mu.Lock()
	dot := c.r.nextDot()
	delta := c.r.data.Decrement(c.r.local.Replica(), amount, dot)
	c.r.trackKey("")
	c.r.mu.Unlock()
	return c.r.propagate(ctx, dot, delta)
}

func (c *PNCounter) Int64() int64 {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	return c.r.data.Int64()
}

func (c *PNCounter) Close() { c.r.Close() }

// ---------------------------------------------------------------------------
// LWWRegister
// ---------------------------------------------------------------------------

// LWWRegister is a last-writer-wins register.
type LWWRegister[V any] struct {
	r *replica[*lwwRegisterState[V]]
}

func NewLWWRegister[V any](id ReplicaID, codec Codec[V], opts ...Option) *LWWRegister[V] {
	state := newLWWRegisterState(codec)
	r := newReplica(id, state, lwwClock{}, opts...)
	return &LWWRegister[V]{r: r}
}

func (reg *LWWRegister[V]) Set(ctx context.Context, value V) (*WriteResult, error) {
	reg.r.mu.Lock()
	dot := reg.r.nextDot()
	delta, err := reg.r.data.Set(value, dot)
	if err != nil {
		reg.r.mu.Unlock()
		return nil, err
	}
	reg.r.trackKey("")
	reg.r.mu.Unlock()
	return reg.r.propagate(ctx, dot, delta), nil
}

func (reg *LWWRegister[V]) Get() (V, bool) {
	reg.r.mu.Lock()
	defer reg.r.mu.Unlock()
	v, _, ok := reg.r.data.Get()
	return v, ok
}

func (reg *LWWRegister[V]) Close() { reg.r.Close() }

// ---------------------------------------------------------------------------
// MVRegister
// ---------------------------------------------------------------------------

// MVRegister is a multi-value register.
type MVRegister[V any] struct {
	r *replica[*mvRegisterState[V]]
}

func NewMVRegister[V any](id ReplicaID, codec Codec[V], opts ...Option) *MVRegister[V] {
	state := newMVRegisterState(codec)
	r := newReplica(id, state, alwaysMergeClock{}, opts...)
	return &MVRegister[V]{r: r}
}

func (reg *MVRegister[V]) Write(ctx context.Context, value V) (*WriteResult, error) {
	reg.r.mu.Lock()
	dot := reg.r.nextDot()
	delta, err := reg.r.data.Write(value, dot, reg.r.received.HWM())
	if err != nil {
		reg.r.mu.Unlock()
		return nil, err
	}
	reg.r.trackKey("")
	reg.r.mu.Unlock()
	return reg.r.propagate(ctx, dot, delta), nil
}

func (reg *MVRegister[V]) Values() ([]V, error) {
	reg.r.mu.Lock()
	defer reg.r.mu.Unlock()
	return reg.r.data.Values()
}

func (reg *MVRegister[V]) Len() int {
	reg.r.mu.Lock()
	defer reg.r.mu.Unlock()
	return reg.r.data.Len()
}

func (reg *MVRegister[V]) Close() { reg.r.Close() }
