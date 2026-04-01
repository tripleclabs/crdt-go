package replica

import (
	"testing"

	"github.com/3clabs/crdt"
)

func TestGCounterReplica_Increment(t *testing.T) {
	r := NewGCounter(1)
	delta := r.Increment(5)
	if r.Data.Int64() != 5 {
		t.Fatalf("expected 5, got %d", r.Data.Int64())
	}
	if len(delta) != 16 {
		t.Fatalf("expected 16 byte delta, got %d", len(delta))
	}
}

func TestGCounterReplica_ApplyDelta(t *testing.T) {
	a := NewGCounter(1)
	b := NewGCounter(2)
	da := a.Increment(5)
	db := b.Increment(3)

	a.ApplyDelta(db)
	b.ApplyDelta(da)

	if a.Data.Int64() != 8 || b.Data.Int64() != 8 {
		t.Fatalf("expected both 8, got a=%d b=%d", a.Data.Int64(), b.Data.Int64())
	}
}

func TestGCounterReplica_Idempotent(t *testing.T) {
	a := NewGCounter(1)
	b := NewGCounter(2)
	d := a.Increment(5)
	b.ApplyDelta(d)
	b.ApplyDelta(d)
	if b.Data.Int64() != 5 {
		t.Fatalf("expected 5, got %d", b.Data.Int64())
	}
}

func TestGCounterReplica_AntiEntropy(t *testing.T) {
	a := NewGCounter(1)
	b := NewGCounter(2)
	a.Increment(10)
	b.Increment(7)

	for _, d := range a.DeltasSince(b.Received.HWM()) {
		b.ApplyDelta(d)
	}
	for _, d := range b.DeltasSince(a.Received.HWM()) {
		a.ApplyDelta(d)
	}

	if a.Data.Int64() != 17 || b.Data.Int64() != 17 {
		t.Fatalf("expected both 17, got a=%d b=%d", a.Data.Int64(), b.Data.Int64())
	}
}

func TestGCounterReplica_FiveNodes(t *testing.T) {
	replicas := make([]*GCounterReplica, 5)
	var allDeltas [][]byte
	for i := range replicas {
		replicas[i] = NewGCounter(crdt.ReplicaID(i + 1))
		d := replicas[i].Increment(uint64(10 * (i + 1)))
		allDeltas = append(allDeltas, d)
	}

	for _, r := range replicas {
		for _, d := range allDeltas {
			r.ApplyDelta(d)
		}
	}

	// Total: 10 + 20 + 30 + 40 + 50 = 150
	for i, r := range replicas {
		if r.Data.Int64() != 150 {
			t.Fatalf("replica %d: expected 150, got %d", i+1, r.Data.Int64())
		}
	}
}
