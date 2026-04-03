package crdt

import (
	"encoding/binary"
	"testing"
)

// stubQuery implements Queryable for testing clock strategies.
type stubQuery struct {
	entry []byte
	hasE  bool
	tomb  []byte
	hasT  bool
}

func (s stubQuery) EntryMeta(string) ([]byte, bool)     { return s.entry, s.hasE }
func (s stubQuery) TombstoneMeta(string) ([]byte, bool) { return s.tomb, s.hasT }

func dotMeta(replica, counter uint64) []byte {
	return encodeDot(Dot{Replica: replica, Counter: counter})
}

func countMeta(count uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, count)
	return b
}

// --- LWWClock ---

func TestLWWClock_AllowsWhenNoLocal(t *testing.T) {
	c := lwwClock{}
	info := deltaInfo{Op: opPut, Key: "k", Meta: dotMeta(1, 5)}
	if !c.Allows(stubQuery{}, info) {
		t.Fatal("should allow when no local state")
	}
}

func TestLWWClock_AllowsHigherDot(t *testing.T) {
	c := lwwClock{}
	info := deltaInfo{Op: opPut, Key: "k", Meta: dotMeta(1, 5)}
	local := stubQuery{entry: dotMeta(1, 3), hasE: true}
	if !c.Allows(local, info) {
		t.Fatal("higher counter should win")
	}
}

func TestLWWClock_RejectsLowerDot(t *testing.T) {
	c := lwwClock{}
	info := deltaInfo{Op: opPut, Key: "k", Meta: dotMeta(1, 2)}
	local := stubQuery{entry: dotMeta(1, 5), hasE: true}
	if c.Allows(local, info) {
		t.Fatal("lower counter should lose")
	}
}

func TestLWWClock_ChecksTombstone(t *testing.T) {
	c := lwwClock{}
	info := deltaInfo{Op: opPut, Key: "k", Meta: dotMeta(1, 3)}
	local := stubQuery{tomb: dotMeta(1, 5), hasT: true}
	if c.Allows(local, info) {
		t.Fatal("tombstone with higher dot should reject")
	}
}

func TestLWWClock_TieBreakByReplicaID(t *testing.T) {
	c := lwwClock{}
	// Same counter, lower replica ID wins.
	info := deltaInfo{Op: opPut, Key: "k", Meta: dotMeta(1, 5)}
	local := stubQuery{entry: dotMeta(2, 5), hasE: true}
	if !c.Allows(local, info) {
		t.Fatal("lower replica ID should win tie")
	}

	info2 := deltaInfo{Op: opPut, Key: "k", Meta: dotMeta(2, 5)}
	local2 := stubQuery{entry: dotMeta(1, 5), hasE: true}
	if c.Allows(local2, info2) {
		t.Fatal("higher replica ID should lose tie")
	}
}

// --- MaxWinsClock ---

func TestMaxWinsClock_AllowsHigherCount(t *testing.T) {
	c := maxWinsClock{}
	info := deltaInfo{Key: "1", Meta: countMeta(10)}
	local := stubQuery{entry: countMeta(5), hasE: true}
	if !c.Allows(local, info) {
		t.Fatal("higher count should win")
	}
}

func TestMaxWinsClock_RejectsLowerCount(t *testing.T) {
	c := maxWinsClock{}
	info := deltaInfo{Key: "1", Meta: countMeta(3)}
	local := stubQuery{entry: countMeta(5), hasE: true}
	if c.Allows(local, info) {
		t.Fatal("lower count should lose")
	}
}

func TestMaxWinsClock_AllowsWhenNoLocal(t *testing.T) {
	c := maxWinsClock{}
	info := deltaInfo{Key: "1", Meta: countMeta(5)}
	if !c.Allows(stubQuery{}, info) {
		t.Fatal("should allow when no local state")
	}
}

// --- AlwaysMergeClock ---

func TestAlwaysMergeClock_AlwaysAllows(t *testing.T) {
	c := alwaysMergeClock{}
	info := deltaInfo{Op: opPut, Key: "k", Meta: dotMeta(1, 1)}
	local := stubQuery{entry: dotMeta(1, 100), hasE: true}
	if !c.Allows(local, info) {
		t.Fatal("should always allow")
	}
}

// --- AddWinsClock ---

func TestAddWinsClock_PutAllowedWhenNoLocal(t *testing.T) {
	c := addWinsClock{}
	info := deltaInfo{Op: opPut, Key: "k", Meta: dotMeta(1, 5)}
	if !c.Allows(stubQuery{}, info) {
		t.Fatal("put should be allowed when no local state")
	}
}

func TestAddWinsClock_PutLWWAgainstEntry(t *testing.T) {
	c := addWinsClock{}
	info := deltaInfo{Op: opPut, Key: "k", Meta: dotMeta(1, 3)}
	local := stubQuery{entry: dotMeta(1, 5), hasE: true}
	if c.Allows(local, info) {
		t.Fatal("put with lower dot should lose to entry")
	}
}

func TestAddWinsClock_PutWinsOverTombstoneWhenNotCovered(t *testing.T) {
	c := addWinsClock{}
	// Remote put with dot (1, 10). Tombstone has context that only covers
	// up to counter 5 for replica 1.
	ctx := VClock{1: 5}
	tombMeta := append(dotMeta(2, 3), encodeVClock(ctx)...)
	info := deltaInfo{Op: opPut, Key: "k", Meta: dotMeta(1, 10)}
	local := stubQuery{tomb: tombMeta, hasT: true}
	if !c.Allows(local, info) {
		t.Fatal("put not covered by tombstone context should win")
	}
}

func TestAddWinsClock_PutLosesToTombstoneWhenCovered(t *testing.T) {
	c := addWinsClock{}
	ctx := VClock{1: 10}
	tombMeta := append(dotMeta(2, 3), encodeVClock(ctx)...)
	info := deltaInfo{Op: opPut, Key: "k", Meta: dotMeta(1, 5)}
	local := stubQuery{tomb: tombMeta, hasT: true}
	if c.Allows(local, info) {
		t.Fatal("put covered by tombstone context should lose")
	}
}

func TestAddWinsClock_RemoveBlockedByUncoveredEntry(t *testing.T) {
	c := addWinsClock{}
	// Remote remove with context covering up to counter 3 for replica 1.
	// Local entry has dot (1, 5) — not covered.
	ctx := encodeVClock(VClock{1: 3})
	info := deltaInfo{Op: opRemove, Key: "k", Meta: dotMeta(2, 6), Context: ctx}
	local := stubQuery{entry: dotMeta(1, 5), hasE: true}
	if c.Allows(local, info) {
		t.Fatal("remove should be blocked when entry is not covered by context")
	}
}

func TestAddWinsClock_RemoveAllowedWhenEntryCovered(t *testing.T) {
	c := addWinsClock{}
	ctx := encodeVClock(VClock{1: 10})
	info := deltaInfo{Op: opRemove, Key: "k", Meta: dotMeta(2, 6), Context: ctx}
	local := stubQuery{entry: dotMeta(1, 5), hasE: true}
	if !c.Allows(local, info) {
		t.Fatal("remove should be allowed when entry is covered by context")
	}
}

func TestAddWinsClock_RemoveLWWAgainstTombstone(t *testing.T) {
	c := addWinsClock{}
	ctx := encodeVClock(VClock{})
	info := deltaInfo{Op: opRemove, Key: "k", Meta: dotMeta(1, 3), Context: ctx}
	local := stubQuery{tomb: dotMeta(1, 5), hasT: true}
	if c.Allows(local, info) {
		t.Fatal("remove with lower dot should lose to existing tombstone")
	}
}
