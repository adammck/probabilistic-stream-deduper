package dedup

import "testing"
import "github.com/stretchr/testify/assert"

func TestDeduper(t *testing.T) {
	d := NewDeduper(3)
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

func BenchmarkTestLayerZero(b *testing.B) {
	d := NewDeduper(10)
	k := []byte("aaa")
	d.Add(k)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.Test(k)
	}
}
