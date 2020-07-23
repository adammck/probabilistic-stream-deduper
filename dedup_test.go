package dedup

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeduper(t *testing.T) {
	d := NewDeduper(3, InverseBloomFilterFactory(1024*1024*1)) // 1MiB
	// [], [], []
	// ^^

	ka := []byte("aaa")
	kb := []byte("bbb")
	kc := []byte("ccc")
	kd := []byte("ddd")

	assert.False(t, d.Test(ka))
	d.Add(ka)
	// [a], [], []
	// ^^^

	assert.True(t, d.Test(ka))
	assert.False(t, d.Test(kb))

	d.Add(kb)
	assert.True(t, d.Test(kb))
	// [ab], [], []
	// ^^^^

	d.Cycle()
	// [ab], [], []
	//       ^^

	assert.True(t, d.Test(ka))
	assert.True(t, d.Test(kb))
	assert.False(t, d.Test(kc))

	d.Add(kc)
	assert.True(t, d.Test(kc))
	// [ab], [c], []
	//       ^^^

	d.Cycle()
	// [ab], [c], []
	//            ^^

	assert.True(t, d.Test(ka))
	assert.True(t, d.Test(kb))
	assert.True(t, d.Test(kc))
	assert.False(t, d.Test(kd))

	d.Add(kd)
	assert.True(t, d.Test(kd))
	// [ab], [c], [d]
	//            ^^^

	d.Cycle()
	// [], [c], [d]
	// ^^

	assert.False(t, d.Test(ka))
	assert.False(t, d.Test(kb))
	assert.True(t, d.Test(kc))
	assert.True(t, d.Test(kd))

	d.Cycle()
	// [], [], [d]
	//     ^^

	assert.False(t, d.Test(ka))
	assert.False(t, d.Test(kb))
	assert.False(t, d.Test(kc))
	assert.True(t, d.Test(kd))

	d.Cycle()
	// [], [], []
	//         ^^

	assert.False(t, d.Test(ka))
	assert.False(t, d.Test(kb))
	assert.False(t, d.Test(kc))
	assert.False(t, d.Test(kd))
}

func BenchmarkBloomFilterAdd1M(b *testing.B) {
	// expect 1M keys, want 1/1M fpr
	d := NewDeduper(1, BloomFilterFactory(1000000, 0.000001))

	for i := 0; i < b.N; i++ {
		k := make([]byte, 16)
		rand.Read(k)
		d.Add(k)
	}
}

func BenchmarkInverseBloomFilterAdd1M(b *testing.B) {
	// 1M buckets
	d := NewDeduper(1, InverseBloomFilterFactory(1000000))

	for i := 0; i < b.N; i++ {
		k := make([]byte, 16)
		rand.Read(k)
		d.Add(k)
	}
}

func BenchmarkStableBloomFilterAdd1M(b *testing.B) {
	// 1M buckets, want 1/1M fpr
	d := NewDeduper(1, StableBloomFilterFactory(1000000, 0.000001))

	for i := 0; i < b.N; i++ {
		k := make([]byte, 16)
		rand.Read(k)
		d.Add(k)
	}
}
