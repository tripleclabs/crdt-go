package crdt

import (
	"context"
	"sync"
	"testing"
	"time"
)

// mockTransport implements AckTransport for testing.
type mockTransport struct {
	mu    sync.Mutex
	sent  []sentDelta
	ackFn func(ReplicaID, Dot)
}

type sentDelta struct {
	peer  ReplicaID
	dot   Dot
	delta []byte
}

func (m *mockTransport) Send(_ context.Context, peer ReplicaID, dot Dot, delta []byte) error {
	m.mu.Lock()
	m.sent = append(m.sent, sentDelta{peer, dot, delta})
	m.mu.Unlock()
	return nil
}

func (m *mockTransport) OnAck(fn func(ReplicaID, Dot)) {
	m.ackFn = fn
}

func (m *mockTransport) simulateAck(peer ReplicaID, dot Dot) {
	m.ackFn(peer, dot)
}

func (m *mockTransport) sentCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sent)
}

func TestDurableReplica_WLocal(t *testing.T) {
	tr := &mockTransport{}
	inner := NewLWWMapReplica[string](1, StringCodec{})
	topo := NewStaticTopology(2, 3)
	dr := NewDurableReplica(inner, topo, tr, WithWriteConcern(WLocal))

	dot := dr.NextDot()
	delta, err := dr.Data.Put("key", "val", dot)
	if err != nil {
		t.Fatal(err)
	}

	// WLocal should return immediately.
	err = dr.Propagate(context.Background(), dot, delta)
	if err != nil {
		t.Fatalf("WLocal Propagate should not error, got %v", err)
	}

	// Delta should still be sent to both peers.
	if n := tr.sentCount(); n != 2 {
		t.Fatalf("expected 2 sends, got %d", n)
	}
}

func TestDurableReplica_WMajority(t *testing.T) {
	tr := &mockTransport{}
	inner := NewLWWMapReplica[string](1, StringCodec{})
	topo := NewStaticTopology(2, 3) // 3-node cluster
	dr := NewDurableReplica(inner, topo, tr,
		WithWriteConcern(WMajority),
		WithPropagateTimeout(2*time.Second),
	)

	dot := dr.NextDot()
	delta, err := dr.Data.Put("key", "val", dot)
	if err != nil {
		t.Fatal(err)
	}

	// Ack from one peer in background (majority = 2, local counts as 1).
	done := make(chan error, 1)
	go func() {
		done <- dr.Propagate(context.Background(), dot, delta)
	}()

	// Small delay to let Propagate register the pending write.
	time.Sleep(10 * time.Millisecond)
	tr.simulateAck(2, dot) // one peer ack → majority reached

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Propagate did not return after majority ack")
	}
}

func TestDurableReplica_WAll(t *testing.T) {
	tr := &mockTransport{}
	inner := NewLWWMapReplica[string](1, StringCodec{})
	topo := NewStaticTopology(2, 3)
	dr := NewDurableReplica(inner, topo, tr,
		WithWriteConcern(WAll),
		WithPropagateTimeout(2*time.Second),
	)

	dot := dr.NextDot()
	delta, err := dr.Data.Put("key", "val", dot)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- dr.Propagate(context.Background(), dot, delta)
	}()

	time.Sleep(10 * time.Millisecond)

	// One ack is not enough for WAll.
	tr.simulateAck(2, dot)
	select {
	case <-done:
		t.Fatal("Propagate returned after only one ack with WAll")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocking
	}

	// Second ack completes quorum.
	tr.simulateAck(3, dot)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Propagate did not return after all acks")
	}
}

func TestDurableReplica_Timeout(t *testing.T) {
	tr := &mockTransport{}
	inner := NewLWWMapReplica[string](1, StringCodec{})
	topo := NewStaticTopology(2, 3)
	dr := NewDurableReplica(inner, topo, tr,
		WithWriteConcern(WAll),
		WithPropagateTimeout(50*time.Millisecond),
	)

	dot := dr.NextDot()
	delta, err := dr.Data.Put("key", "val", dot)
	if err != nil {
		t.Fatal(err)
	}

	// No acks — should timeout.
	err = dr.Propagate(context.Background(), dot, delta)
	if err != ErrQuorumTimeout {
		t.Fatalf("expected ErrQuorumTimeout, got %v", err)
	}
}

func TestDurableReplica_Close(t *testing.T) {
	tr := &mockTransport{}
	inner := NewLWWMapReplica[string](1, StringCodec{})
	topo := NewStaticTopology(2, 3)
	dr := NewDurableReplica(inner, topo, tr,
		WithWriteConcern(WAll),
		WithPropagateTimeout(5*time.Second),
	)

	dot := dr.NextDot()
	delta, err := dr.Data.Put("key", "val", dot)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- dr.Propagate(context.Background(), dot, delta)
	}()

	time.Sleep(10 * time.Millisecond)

	// Close should unblock the pending write with ErrClosed.
	dr.Close()

	select {
	case err := <-done:
		if err != ErrClosed {
			t.Fatalf("expected ErrClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Propagate was not unblocked by Close")
	}
}

func TestDurableReplica_PropagateAfterClose(t *testing.T) {
	tr := &mockTransport{}
	inner := NewLWWMapReplica[string](1, StringCodec{})
	topo := NewStaticTopology(2, 3)
	dr := NewDurableReplica(inner, topo, tr,
		WithWriteConcern(WAll),
	)

	dr.Close()

	dot := dr.NextDot()
	delta, err := dr.Data.Put("key", "val", dot)
	if err != nil {
		t.Fatal(err)
	}

	err = dr.Propagate(context.Background(), dot, delta)
	if err != ErrClosed {
		t.Fatalf("expected ErrClosed after Close, got %v", err)
	}
}

func TestDurableReplica_FastAckRace(t *testing.T) {
	// Transport that acks synchronously inside Send, exercising the
	// race where acks arrive before Propagate used to register pending.
	tr := &syncAckTransport{}
	inner := NewLWWMapReplica[string](1, StringCodec{})
	topo := NewStaticTopology(2, 3)
	dr := NewDurableReplica(inner, topo, tr,
		WithWriteConcern(WMajority),
		WithPropagateTimeout(time.Second),
	)

	dot := dr.NextDot()
	delta, err := dr.Data.Put("key", "val", dot)
	if err != nil {
		t.Fatal(err)
	}

	// Both peers ack inside Send. With the pre-registration fix this
	// should return nil. Without it, it would timeout.
	err = dr.Propagate(context.Background(), dot, delta)
	if err != nil {
		t.Fatalf("expected nil with synchronous acks, got %v", err)
	}
}

func TestDurableReplica_SnapshotIsolation(t *testing.T) {
	// Use a mutable topology to verify snapshot-at-write-time behavior.
	mutable := &mutableTopology{peers: []ReplicaID{2, 3}}
	tr := &mockTransport{}
	inner := NewLWWMapReplica[string](1, StringCodec{})
	dr := NewDurableReplica(inner, mutable, tr,
		WithWriteConcern(WAll),
		WithPropagateTimeout(2*time.Second),
	)

	dot := dr.NextDot()
	delta, err := dr.Data.Put("key", "val", dot)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- dr.Propagate(context.Background(), dot, delta)
	}()

	time.Sleep(10 * time.Millisecond)

	// Add a new peer after the write started. Should NOT affect the
	// in-flight quorum (snapshot was {2, 3}).
	mutable.mu.Lock()
	mutable.peers = []ReplicaID{2, 3, 4}
	mutable.mu.Unlock()

	// Ack from original peers should satisfy WAll from the snapshot.
	tr.simulateAck(2, dot)
	tr.simulateAck(3, dot)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Propagate did not return with original peer acks")
	}
}

func TestDurableReplica_DuplicateAck(t *testing.T) {
	tr := &mockTransport{}
	inner := NewLWWMapReplica[string](1, StringCodec{})
	topo := NewStaticTopology(2, 3)
	dr := NewDurableReplica(inner, topo, tr,
		WithWriteConcern(WAll),
		WithPropagateTimeout(2*time.Second),
	)

	dot := dr.NextDot()
	delta, err := dr.Data.Put("key", "val", dot)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- dr.Propagate(context.Background(), dot, delta)
	}()

	time.Sleep(10 * time.Millisecond)

	// Duplicate acks from same peer should not count double.
	tr.simulateAck(2, dot)
	tr.simulateAck(2, dot)
	tr.simulateAck(2, dot)

	select {
	case <-done:
		t.Fatal("Propagate returned with duplicate acks from same peer")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocking, need peer 3
	}

	tr.simulateAck(3, dot)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Propagate did not return after all distinct acks")
	}
}

func TestDurableReplica_NoPeers(t *testing.T) {
	tr := &mockTransport{}
	inner := NewLWWMapReplica[string](1, StringCodec{})
	topo := NewStaticTopology() // no peers
	dr := NewDurableReplica(inner, topo, tr,
		WithWriteConcern(WAll),
	)

	dot := dr.NextDot()
	delta, err := dr.Data.Put("key", "val", dot)
	if err != nil {
		t.Fatal(err)
	}

	// With no peers, even WAll should return immediately.
	err = dr.Propagate(context.Background(), dot, delta)
	if err != nil {
		t.Fatalf("expected nil with no peers, got %v", err)
	}
}

// mutableTopology is a test helper that allows changing the peer set.
type mutableTopology struct {
	mu    sync.Mutex
	peers []ReplicaID
}

func (m *mutableTopology) Peers() []ReplicaID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ReplicaID, len(m.peers))
	copy(out, m.peers)
	return out
}

// syncAckTransport acks synchronously inside Send, simulating an
// extremely fast (in-process) transport.
type syncAckTransport struct {
	ackFn func(ReplicaID, Dot)
}

func (s *syncAckTransport) Send(_ context.Context, peer ReplicaID, dot Dot, _ []byte) error {
	if s.ackFn != nil {
		s.ackFn(peer, dot)
	}
	return nil
}

func (s *syncAckTransport) OnAck(fn func(ReplicaID, Dot)) {
	s.ackFn = fn
}
