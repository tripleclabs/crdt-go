package crdt

import (
	"fmt"
	"testing"
)

func BenchmarkLWWMap_Put(b *testing.B) {
	b.ReportAllocs()
	r := NewLWWMapReplica[string](1, StringCodec{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		r.Data.Put(key, val, r.NextDot())
	}
}

func BenchmarkLWWMap_ApplyDelta(b *testing.B) {
	b.ReportAllocs()
	src := NewLWWMapReplica[string](1, StringCodec{})
	dst := NewLWWMapReplica[string](2, StringCodec{})

	// Pre-generate deltas.
	deltas := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		d, _ := src.Data.Put(key, val, src.NextDot())
		deltas[i] = d
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst.ApplyDelta(deltas[i])
	}
}

func BenchmarkLWWMap_DeltasSince_1K(b *testing.B) {
	b.ReportAllocs()
	r := NewLWWMapReplica[string](1, StringCodec{})
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		r.Data.Put(key, val, r.NextDot())
	}

	empty := VClock{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.DeltasSince(empty)
	}
}

func BenchmarkLWWMap_DeltasSince_1K_Partial(b *testing.B) {
	b.ReportAllocs()
	r := NewLWWMapReplica[string](1, StringCodec{})
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		r.Data.Put(key, val, r.NextDot())
	}

	// HWM covers 990 of 1000 entries (counters 1..990).
	hwm := VClock{1: 990}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.DeltasSince(hwm)
	}
}

func BenchmarkLWWMap_Get(b *testing.B) {
	b.ReportAllocs()
	r := NewLWWMapReplica[string](1, StringCodec{})
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		r.Data.Put(key, val, r.NextDot())
	}

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Data.Get(keys[i%1000])
	}
}

func BenchmarkGCounter_Increment(b *testing.B) {
	b.ReportAllocs()
	r := NewGCounterReplica(1)
	rid := r.Local.Replica()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Data.Increment(rid, 1)
	}
}

func BenchmarkGCounter_ApplyDelta(b *testing.B) {
	b.ReportAllocs()
	src := NewGCounterReplica(1)
	dst := NewGCounterReplica(2)

	// Pre-generate deltas (each increment produces a new count).
	deltas := make([][]byte, b.N)
	rid := src.Local.Replica()
	for i := 0; i < b.N; i++ {
		deltas[i] = src.Data.Increment(rid, 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst.ApplyDelta(deltas[i])
	}
}

func BenchmarkORSet_Add(b *testing.B) {
	b.ReportAllocs()
	r := NewORSetReplica[string](1, StringCodec{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		elem := fmt.Sprintf("key-%d", i)
		r.Data.Add(elem, r.NextDot())
	}
}

func BenchmarkORSet_ApplyDelta(b *testing.B) {
	b.ReportAllocs()
	src := NewORSetReplica[string](1, StringCodec{})
	dst := NewORSetReplica[string](2, StringCodec{})

	// Pre-generate deltas.
	deltas := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		elem := fmt.Sprintf("key-%d", i)
		d, _ := src.Data.Add(elem, src.NextDot())
		deltas[i] = d
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst.ApplyDelta(deltas[i])
	}
}
