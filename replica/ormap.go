package replica

import "github.com/3clabs/crdt"

// ORMapReplica wraps a [crdt.ORMap] with clock and add-wins merge logic.
// Similar to ORSet but with string keys and typed values.
type ORMapReplica[V any] struct {
	Data     *crdt.ORMap[V]
	Codec    crdt.Codec[V]
	Clock    *crdt.LocalClock
	Received *crdt.ReceivedClock
}

// NewORMap creates an ORMapReplica.
func NewORMap[V any](replicaID crdt.ReplicaID, codec crdt.Codec[V], opts ...crdt.Option) *ORMapReplica[V] {
	return &ORMapReplica[V]{
		Data:     crdt.NewORMap(codec, opts...),
		Codec:    codec,
		Clock:    crdt.NewLocalClock(replicaID),
		Received: crdt.NewReceivedClock(),
	}
}

// Put stores a key-value pair, stamps a dot, returns the encoded delta.
// Delta format: [op=0x01][varint key len][key][varint val len][val][encoded dotmap]
func (r *ORMapReplica[V]) Put(key string, value V) ([]byte, error) {
	dot := r.Clock.Next()
	r.Received.Record(dot.Replica, dot.Counter)

	// Combine new dot with existing dots.
	dots := crdt.DotMap{dot.Replica: dot.Counter}
	if _, existing, ok := r.Data.GetBytes(key); ok {
		for rep, c := range existing {
			dots[rep] = c
		}
		dots[dot.Replica] = dot.Counter
	}

	valBytes, err := r.Codec.Encode(value)
	if err != nil {
		return nil, err
	}
	r.Data.PutBytes(key, valBytes, dots)

	// Delta carries only the new dot.
	deltaDots := crdt.DotMap{dot.Replica: dot.Counter}
	buf := []byte{OpPut}
	buf = appendVarintBytes(buf, []byte(key))
	buf = appendVarintBytes(buf, valBytes)
	buf = append(buf, crdt.EncodeDotMap(deltaDots)...)
	return buf, nil
}

// Remove removes a key, returns the encoded delta. The delta carries
// the received HWM as causal context.
// Delta format: [op=0x02][varint key len][key][encoded vclock]
func (r *ORMapReplica[V]) Remove(key string) []byte {
	r.Data.Remove(key)

	buf := []byte{OpRemove}
	buf = appendVarintBytes(buf, []byte(key))
	buf = append(buf, crdt.EncodeVClock(r.Received.HWM())...)
	return buf
}

// ApplyDelta applies an incoming delta.
func (r *ORMapReplica[V]) ApplyDelta(delta []byte) error {
	if len(delta) < 1 {
		return crdt.ErrShortBuffer
	}
	switch delta[0] {
	case OpPut:
		return r.applyPut(delta[1:])
	case OpRemove:
		return r.applyRemove(delta[1:])
	default:
		return crdt.ErrUnknownOp
	}
}

func (r *ORMapReplica[V]) applyPut(data []byte) error {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	valBytes, off, err := readVarintBytes(data, off)
	if err != nil {
		return err
	}
	remoteDots, err := crdt.DecodeDotMap(data[off:])
	if err != nil {
		return err
	}
	key := string(keyBytes)

	// Combine with existing dots. If key exists, merge dots and keep
	// the value from the higher max-dot.
	if localVal, localDots, ok := r.Data.GetBytes(key); ok {
		combined := crdt.CombineDots(localDots, remoteDots)
		winner := localVal
		if crdt.DotGT(crdt.MaxDot(remoteDots), crdt.MaxDot(localDots)) {
			winner = valBytes
		}
		r.Data.PutBytes(key, winner, combined)
	} else {
		r.Data.PutBytes(key, valBytes, remoteDots)
	}

	for rep, counter := range remoteDots {
		r.Received.Record(rep, counter)
	}
	return nil
}

func (r *ORMapReplica[V]) applyRemove(data []byte) error {
	keyBytes, off, err := readVarintBytes(data, 0)
	if err != nil {
		return err
	}
	removeVC, err := crdt.DecodeVClock(data[off:])
	if err != nil {
		return err
	}
	key := string(keyBytes)

	_, localDots, ok := r.Data.GetBytes(key)
	if !ok {
		return nil
	}

	surviving := make(crdt.DotMap)
	for rep, counter := range localDots {
		if counter > removeVC.Get(rep) {
			surviving[rep] = counter
		}
	}

	if len(surviving) > 0 {
		val, _, _ := r.Data.GetBytes(key)
		r.Data.PutBytes(key, val, surviving)
	} else {
		r.Data.Remove(key)
	}
	return nil
}

// DeltasSince returns deltas for entries with dots not covered by peerHWM.
func (r *ORMapReplica[V]) DeltasSince(peerHWM crdt.VClock) [][]byte {
	var deltas [][]byte
	r.Data.RangeBytes(func(key string, valBytes []byte, dots crdt.DotMap) bool {
		for rep, counter := range dots {
			if counter > peerHWM.Get(rep) {
				buf := []byte{OpPut}
				buf = appendVarintBytes(buf, []byte(key))
				buf = appendVarintBytes(buf, valBytes)
				buf = append(buf, crdt.EncodeDotMap(dots)...)
				deltas = append(deltas, buf)
				break
			}
		}
		return true
	})
	return deltas
}
