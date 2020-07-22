package dedup

import "github.com/patrickmn/go-bloom"
import "container/ring"
import "sync"

type Message struct {
	whatever string
	offset   int
}

func Key(msg Message) []byte {
	return []byte(msg.whatever)
}

type Deduper struct {
	ring *ring.Ring // banana phone
	mu   sync.Mutex
}

func NewDeduper(n int) *Deduper {
	r := ring.New(n)

	// init each element to an empty bloom filter
	for i := 0; i < n; i++ {
		// expect 100k items, allow fp rate of 1%
		r.Value = bloom.New(100000, 0.01)
		r = r.Next()
	}

	return &Deduper{
		ring: r,
	}
}

// Test returns true if k is probably in one of the ring, or false if it's definitely in none.
func (d *Deduper) Test(k []byte) bool {
	// inlined meat of ring.Do for early return
	// https://golang.org/src/container/ring/ring.go?s=3118:3156#L124

	if d.ring.Value.(*bloom.Filter).Test(k) {
		//fmt.Printf("found in topmost layer\n")
		return true
	}

	// just for debugging, for counting layer depth
	// (finding things in deep layers indicates serious skew, since duplicates
	// should be arriving around the same time.)
	n := 1

	for p := d.ring.Prev(); p != d.ring; p = p.Prev() {
		if p.Value.(*bloom.Filter).Test(k) {
			//fmt.Printf("found in layer -%d\n", n)
			return true
		}

		n += 1
	}

	return false
}

func (d *Deduper) Add(key []byte) {
	// todo: check whether b.F is already threadsafe
	// todo: move this lock to the element if not
	d.mu.Lock()
	defer d.mu.Unlock()

	d.ring.Value.(*bloom.Filter).Add(key)
}

func (d *Deduper) Cycle() {
	//fmt.Println("cycling")

	// todo: split this lock in two; no reason to block writes since we won't
	// be touching the topmost layer anyway
	d.mu.Lock()
	defer d.mu.Unlock()

	// erase deepest/oldest layer, and cycle to it, making it the topmost
	oldest := d.ring.Next()
	oldest.Value = bloom.New(100000, 0.01)
	d.ring = oldest
}

func Try(d *Deduper, msg Message) bool {
	k := Key(msg)

	if d.Test(k) {
		//fmt.Printf("found: %v\n", msg)
		return false
	}

	//fmt.Printf("inserting: %v\n", msg)
	d.Add(k)
	return true
}
