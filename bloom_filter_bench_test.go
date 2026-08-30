package main

import (
	"hash/maphash"
	"slices"
	"strconv"
	"testing"
)

func newBenchBloomFilter(b *testing.B, bitSize, hashFuncs int) *BloomFilter[string] {
	b.Helper()

	bf, err := NewComparableBloomFilter[string](bitSize, hashFuncs)
	if err != nil {
		b.Fatalf("NewComparableBloomFilter() returned error: %v", err)
	}
	return bf
}

func BenchmarkBloomFilterAdd(b *testing.B) {
	keys := benchKeys("add-", 1024)
	for _, hashFuncs := range []int{3, 7} {
		b.Run("k="+strconv.Itoa(hashFuncs), func(b *testing.B) {
			bf := newBenchBloomFilter(b, 1<<20, hashFuncs)
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				bf.Add(keys[i%len(keys)])
				i++
			}
		})
	}
}

func BenchmarkBloomFilterTestHit(b *testing.B) {
	keys := benchKeys("hit-", 1024)
	bf := newBenchBloomFilter(b, 1<<20, 7)
	for _, key := range keys {
		bf.Add(key)
	}

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if !bf.Test(keys[i%len(keys)]) {
			b.Fatalf("Test(%q) = false, expected true", keys[i%len(keys)])
		}
		i++
	}
}

func BenchmarkBloomFilterTestMiss(b *testing.B) {
	bf := newBenchBloomFilter(b, 1<<20, 7)
	for _, key := range benchKeys("hit-", 1024) {
		bf.Add(key)
	}
	absent := benchKeys("miss-", 1024)

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		bf.Test(absent[i%len(absent)])
		i++
	}
}

func BenchmarkBloomFilterMerge(b *testing.B) {
	src := newBenchBloomFilter(b, 1<<20, 7)
	for _, key := range benchKeys("merge-", 4096) {
		src.Add(key)
	}
	dst := newBenchBloomFilter(b, 1<<20, 7)

	b.ReportAllocs()
	for b.Loop() {
		if err := dst.Merge(src); err != nil {
			b.Fatalf("Merge() returned error: %v", err)
		}
	}
}

func BenchmarkStreamCollectBloomFilter(b *testing.B) {
	hasher := maphash.ComparableHasher[string]{}
	for _, n := range benchSizes {
		keys := benchKeys("collect-", n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := NewStream(slices.Values(keys)).
					CollectBloomFilter(hasher, 1<<20, 7)
				if err != nil {
					b.Fatalf("CollectBloomFilter() returned error: %v", err)
				}
			}
		})
	}
}

func BenchmarkStreamCollectBloomFilterByError(b *testing.B) {
	hasher := maphash.ComparableHasher[string]{}
	for _, n := range benchSizes {
		keys := benchKeys("collect-err-", n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := NewStream(slices.Values(keys)).
					CollectBloomFilterByError(hasher, n, 0.01)
				if err != nil {
					b.Fatalf("CollectBloomFilterByError() returned error: %v", err)
				}
			}
		})
	}
}
