package crdtbolt

import (
	"context"
	"fmt"
	"sync"

	"github.com/tripleclabs/crdt-go"
)

type testNet struct {
	mu        sync.RWMutex
	receivers map[crdt.ReplicaID]func(crdt.TransportMessage)
	peers     map[crdt.ReplicaID][]crdt.ReplicaID
}

func newTestNet() *testNet {
	return &testNet{
		receivers: make(map[crdt.ReplicaID]func(crdt.TransportMessage)),
		peers:     make(map[crdt.ReplicaID][]crdt.ReplicaID),
	}
}

func (n *testNet) addPeer(id crdt.ReplicaID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var peersForNew []crdt.ReplicaID
	for existingID := range n.peers {
		peersForNew = append(peersForNew, existingID)
		n.peers[existingID] = append(n.peers[existingID], id)
	}
	n.peers[id] = peersForNew
}

func (n *testNet) transport(id crdt.ReplicaID) crdt.Transport {
	return &testNetTransport{net: n, self: id}
}

func (n *testNet) topology(id crdt.ReplicaID) crdt.TopologyProvider {
	return &testNetTopology{net: n, self: id}
}

type testNetTransport struct {
	net  *testNet
	self crdt.ReplicaID
}

func (t *testNetTransport) Send(_ context.Context, peer crdt.ReplicaID, msg crdt.TransportMessage) (<-chan struct{}, error) {
	t.net.mu.RLock()
	recv, ok := t.net.receivers[peer]
	t.net.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("crdtbolt: unknown peer %d", peer)
	}
	recv(msg)
	return nil, nil
}

func (t *testNetTransport) OnReceive(fn func(crdt.TransportMessage)) {
	t.net.mu.Lock()
	t.net.receivers[t.self] = fn
	t.net.mu.Unlock()
}

type testNetTopology struct {
	net  *testNet
	self crdt.ReplicaID
}

func (t *testNetTopology) Peers() []crdt.ReplicaID {
	t.net.mu.RLock()
	defer t.net.mu.RUnlock()
	src := t.net.peers[t.self]
	out := make([]crdt.ReplicaID, len(src))
	copy(out, src)
	return out
}
