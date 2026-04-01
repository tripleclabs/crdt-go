package replica

import "github.com/3clabs/crdt"

// ORSetReplica wraps an [crdt.ORSet] with clock and add-wins merge logic.
type ORSetReplica[E any] struct {
	Data     *crdt.ORSet[E]
	Codec    crdt.Codec[E]
	Clock    *crdt.LocalClock
	Received *crdt.ReceivedClock
}

// NewORSet creates an ORSetReplica.
func NewORSet[E any](replicaID crdt.ReplicaID, codec crdt.Codec[E], opts ...crdt.Option) *ORSetReplica[E] {
	return &ORSetReplica[E]{
		Data:     crdt.NewORSet(codec, opts...),
		Codec:    codec,
		Clock:    crdt.NewLocalClock(replicaID),
		Received: crdt.NewReceivedClock(),
	}
}

// Add adds an element, stamps a dot, and returns the encoded delta.
//
// Delta format: [op=0x01][varint elem len][elem bytes][encoded dotmap]
func (r *ORSetReplica[E]) Add(elem E) ([]byte, error) {
	dot := r.Clock.Next()
	r.Received.Record(dot.Replica, dot.Counter)

	elemBytes, err := r.Codec.Encode(elem)
	if err != nil {
		return nil, err
	}
	elemKey := string(elemBytes)

	// Combine new dot with existing dots.
	dots := crdt.DotMap{dot.Replica: dot.Counter}
	if existing, ok := r.Data.GetEncoded(elemKey); ok {
		for rep, c := range existing {
			dots[rep] = c
		}
		dots[dot.Replica] = dot.Counter
	}
	r.Data.PutEncoded(elemKey, dots)

	// Delta carries only the new dot.
	deltaDots := crdt.DotMap{dot.Replica: dot.Counter}
	buf := []byte{OpPut}
	buf = appendVarintBytes(buf, elemBytes)
	buf = append(buf, crdt.EncodeDotMap(deltaDots)...)
	return buf, nil
}

// Remove removes an element and returns the encoded delta. The delta
// carries the local received HWM as causal context — the receiver uses
// it to determine which dots the remover had observed.
//
// Delta format: [op=0x02][varint elem len][elem bytes][encoded vclock]
func (r *ORSetReplica[E]) Remove(elem E) ([]byte, error) {
	elemBytes, err := r.Codec.Encode(elem)
	if err != nil {
		return nil, err
	}
	r.Data.RemoveEncoded(string(elemBytes))

	buf := []byte{OpRemove}
	buf = appendVarintBytes(buf, elemBytes)
	buf = append(buf, crdt.EncodeVClock(r.Received.HWM())...)
	return buf, nil
}

// ApplyDelta applies an incoming delta.
func (r *ORSetReplica[E]) ApplyDelta(delta []byte) error {
	if len(delta) < 1 {
		return crdt.ErrShortBuffer
	}
	switch delta[0] {
	case OpPut:
		return r.applyAdd(delta[1:])
	case OpRemove:
		return r.applyRemove(delta[1:])
	default:
		return crdt.ErrUnknownOp
	}
}

func (r *ORSetReplica[E]) applyAdd(data []byte) error {
	elemBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	remoteDots, err := crdt.DecodeDotMap(data[off:])
	if err != nil {
		return err
	}
	elemKey := string(elemBytes)

	// Combine with existing dots.
	if localDots, ok := r.Data.GetEncoded(elemKey); ok {
		remoteDots = crdt.CombineDots(localDots, remoteDots)
	}
	r.Data.PutEncoded(elemKey, remoteDots)

	// Record each incoming dot.
	for rep, counter := range remoteDots {
		r.Received.Record(rep, counter)
	}
	return nil
}

func (r *ORSetReplica[E]) applyRemove(data []byte) error {
	elemBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	removeVC, err := crdt.DecodeVClock(data[off:])
	if err != nil {
		return err
	}
	elemKey := string(elemBytes)

	localDots, ok := r.Data.GetEncoded(elemKey)
	if !ok {
		return nil
	}

	// Keep dots NOT dominated by the remover's causal context.
	surviving := make(crdt.DotMap)
	for rep, counter := range localDots {
		if counter > removeVC.Get(rep) {
			surviving[rep] = counter
		}
	}

	if len(surviving) > 0 {
		r.Data.PutEncoded(elemKey, surviving)
	} else {
		r.Data.RemoveEncoded(elemKey)
	}
	return nil
}

// DeltasSince returns add deltas for elements with any dot not covered
// by peerHWM.
func (r *ORSetReplica[E]) DeltasSince(peerHWM crdt.VClock) [][]byte {
	var deltas [][]byte
	r.Data.Range(func(elemKey string, dots crdt.DotMap) bool {
		for rep, counter := range dots {
			if counter > peerHWM.Get(rep) {
				buf := []byte{OpPut}
				buf = appendVarintBytes(buf, []byte(elemKey))
				buf = append(buf, crdt.EncodeDotMap(dots)...)
				deltas = append(deltas, buf)
				break
			}
		}
		return true
	})
	return deltas
}
