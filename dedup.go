package dedup

import (
	"container/ring"
	"sync"

	boom "github.com/tylertreat/BoomFilters"
)

// Deduper is a struct.
type Deduper struct {
	ring    *ring.Ring // banana phone
	factory func() boom.Filter
	mu      sync.Mutex
}

// NewDeduper returns a Deduper with the given configuration.
func NewDeduper(n int, factory func() boom.Filter) *Deduper {
	d := &Deduper{
		ring:    ring.New(n),
		factory: factory,
	}

	// init each element to an empty bloom filter
	for i := 0; i < n; i++ {
		// expect 100k items, allow fp rate of 1%
		d.ring.Value = d.factory()
		d.ring = d.ring.Next()
	}

	return d
}

// BloomFilterFactory returns a func to return a Bloom Filter. Memory usage is
// optimized to store n items with the given false-positive rate. The actual
// number of buckets is variable.
func BloomFilterFactory(n uint, fpRate float64) func() boom.Filter {
	return func() boom.Filter {
		return boom.NewBloomFilter(n, fpRate)
	}
}

// InverseBloomFilterFactory returns a func to return a Inverse Bloom Filter.
// Memory usage will be roughly the capacity multipled by eight (bytes), because
// each bucket is a 64 bit pointer. This is a lower-level interface than the
// other two.
func InverseBloomFilterFactory(capacity uint) func() boom.Filter {
	return func() boom.Filter {
		return boom.NewInverseBloomFilter(capacity)
	}
}

// StableBloomFilterFactory returns a func to return a Stable Bloom Filter, to
// be given to NewDeduper.
func StableBloomFilterFactory(m uint, fpRate float64) func() boom.Filter {
	return func() boom.Filter {
		return boom.NewDefaultStableBloomFilter(m, fpRate)
	}
}

// Test returns true if k is probably in one of the filters in the ring. Results
// are guaranteed to be inaccurate; how much so depends on which filter factory
// is in use. See the docs for those for more info.
func (d *Deduper) Test(k []byte) bool {
	// inlined meat of ring.Do for early return
	// https://golang.org/src/container/ring/ring.go?s=3118:3156#L124

	if d.ring.Value.(boom.Filter).Test(k) {
		//fmt.Printf("found in topmost layer\n")
		return true
	}

	// just for debugging, for counting layer depth
	// (finding things in deep layers indicates serious skew, since duplicates
	// should be arriving around the same time.)
	n := 1

	for p := d.ring.Prev(); p != d.ring; p = p.Prev() {
		if p.Value.(boom.Filter).Test(k) {
			//fmt.Printf("found in layer -%d\n", n)
			return true
		}

		n++
	}

	return false
}

// Add adds the given key to the topmost filter.
func (d *Deduper) Add(key []byte) {
	// todo: check whether b.F is already threadsafe
	// todo: move this lock to the element if not
	d.mu.Lock()
	defer d.mu.Unlock()

	d.ring.Value.(boom.Filter).Add(key)
}

// Cycle advances the ring buffer to the next slot, and clears it. This has the
// effect of discarding the oldest data.
func (d *Deduper) Cycle() {
	// todo: split this lock in two; no reason to block writes since we won't
	// be touching the topmost layer anyway
	d.mu.Lock()
	defer d.mu.Unlock()

	// erase deepest/oldest layer, and cycle to it, making it the topmost
	oldest := d.ring.Next()
	oldest.Value = d.factory()
	d.ring = oldest
}
