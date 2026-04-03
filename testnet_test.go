package crdt

import (
	"context"
	"fmt"
	"sync"
)

type testNet struct {
	mu        sync.RWMutex
	receivers map[ReplicaID]func(TransportMessage)
	peers     map[ReplicaID][]ReplicaID
}

func newTestNet() *testNet {
	return &testNet{
		receivers: make(map[ReplicaID]func(TransportMessage)),
		peers:     make(map[ReplicaID][]ReplicaID),
	}
}

func (n *testNet) addPeer(id ReplicaID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var peersForNew []ReplicaID
	for existingID := range n.peers {
		peersForNew = append(peersForNew, existingID)
		n.peers[existingID] = append(n.peers[existingID], id)
	}
	n.peers[id] = peersForNew
}

func (n *testNet) transport(id ReplicaID) Transport {
	return &testNetTransport{net: n, self: id}
}

func (n *testNet) topology(id ReplicaID) TopologyProvider {
	return &testNetTopology{net: n, self: id}
}

type testNetTransport struct {
	net  *testNet
	self ReplicaID
}

func (t *testNetTransport) Send(_ context.Context, peer ReplicaID, msg TransportMessage) (<-chan struct{}, error) {
	t.net.mu.RLock()
	recv, ok := t.net.receivers[peer]
	t.net.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("crdt: unknown peer %d", peer)
	}
	msg.from = t.self
	recv(msg)
	if msg.ack {
		ch := make(chan struct{})
		close(ch)
		return ch, nil
	}
	return nil, nil
}

func (t *testNetTransport) OnReceive(fn func(TransportMessage)) {
	t.net.mu.Lock()
	t.net.receivers[t.self] = fn
	t.net.mu.Unlock()
}

type testNetTopology struct {
	net  *testNet
	self ReplicaID
}

func (t *testNetTopology) Peers() []ReplicaID {
	t.net.mu.RLock()
	defer t.net.mu.RUnlock()
	src := t.net.peers[t.self]
	out := make([]ReplicaID, len(src))
	copy(out, src)
	return out
}
