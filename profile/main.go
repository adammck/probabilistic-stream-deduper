package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"

	dedup "github.com/adammck/probabilistic-stream-deduper"
	boom "github.com/tylertreat/BoomFilters"
)

var cpuProfile = flag.String("cpu-profile", "", "write cpu profile")
var memProfile = flag.String("mem-profile", "", "write memory profile")
var keySize = flag.Int("key-size", 16, "size of keys, in bytes")
var numLayers = flag.Int("layers", 1, "number of elements in ring buffer")
var numKeys = flag.Int("keys", 1000000, "number of keys to insert")

var useClassic = flag.Bool("classic", false, "use a classic bloom filter")
var classicNumItems = flag.Uint("classic-num-items", 1000000, "classic: expected number of items")
var classicFPR = flag.Float64("classic-fpr", 0.000001, "classic: desired false-positive rate")

var useInverse = flag.Bool("inverse", false, "use an inverse bloom filter")
var inverseCap = flag.Uint("inverse-cap", 1000000, "inverse: number of buckets")

var useStable = flag.Bool("stable", false, "use a stable bloom filter")
var stableCap = flag.Uint("stable-num-items", 1000000, "stable: number of buckets")
var stableFPR = flag.Float64("stable-fpr", 0.000001, "stable: desired false-positive rate")

const keyBytes = 16

func randomKey() [keyBytes]byte {
	tmp := make([]byte, keyBytes)
	rand.Read(tmp)
	var k [keyBytes]byte
	copy(k[:], tmp[:keyBytes])
	return k
}

func runTest(d *dedup.Deduper, numLayers int, numKeys int) *dedup.Deduper {
	keys := make(map[[keyBytes]byte]struct{})
	notPresent := make([][keyBytes]byte, 0)
	yesPresent := make([][keyBytes]byte, 0)

	// generate N random keys
	for i := 0; i < numKeys; i++ {
		var k [keyBytes]byte

		// repeat for duplicate keys
		// this is highly unlikely to be necessary
		for {
			k = randomKey()
			if _, ok := keys[k]; !ok {
				break
			} else {
				fmt.Printf("collision: %v\n", k)
				panic(k)
			}
		}

		keys[k] = struct{}{}
	}

	n := 0
	for k := range keys {
		if n%2 == 0 {
			notPresent = append(notPresent, k)
		} else {
			yesPresent = append(yesPresent, k)

			// type hackery to copy actual bytes
			tmp := make([]byte, keyBytes)
			copy(tmp[:keyBytes], k[:])
			d.Add(tmp)
		}
		n++
	}

	// count false positives
	fpc := 0
	for _, k := range notPresent {
		if d.Test(k[:]) {
			fpc++
		}
	}
	fpr := float64(fpc) / float64(len(notPresent))
	fmt.Printf("false positive rate: %v\n", fpr)

	// count false negatives
	fnc := 0
	for _, k := range yesPresent {
		if !d.Test(k[:]) {
			fnc++
		}
	}
	fnr := float64(fnc) / float64(len(yesPresent))
	fmt.Printf("false negative rate: %v\n", fnr)

	return d
}

func main() {
	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Printf("could not create CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Printf("could not start CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	var f func() boom.Filter

	if *useClassic {
		f = dedup.BloomFilterFactory(*classicNumItems, *classicFPR)
		bf := f().(*boom.BloomFilter)
		fmt.Printf("BloomFilter (n=%v, fpr=%v) (cap=%v)\n", *classicNumItems, *classicFPR, bf.Capacity())
	}

	if *useInverse {
		f = dedup.InverseBloomFilterFactory(*inverseCap)
		fmt.Printf("InverseBloomFilter (cap=%v)\n", *inverseCap)
	}

	if *useStable {
		fmt.Printf("StableBloomFilter (m=%v, fpr=%v)\n", *stableCap, *stableFPR)
		f = dedup.StableBloomFilterFactory(*stableCap, *stableFPR)
	}

	d := dedup.NewDeduper(*numLayers, f)

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			fmt.Printf("could not create memory profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Printf("could not write memory profile: %v\n", err)
			os.Exit(1)
		}
	}

	runTest(d, *numLayers, *numKeys)
}
