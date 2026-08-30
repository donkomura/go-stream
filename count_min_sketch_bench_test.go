package main

import (
	"hash/maphash"
	"slices"
	"strconv"
	"testing"
)

func newBenchCountMinSketch(b *testing.B, width, depth int) *CountMinSketch[string] {
	b.Helper()

	cms, err := NewComparableCountMinSketch[string](width, depth)
	if err != nil {
		b.Fatalf("NewComparableCountMinSketch() returned error: %v", err)
	}
	return cms
}

func BenchmarkCountMinSketchAdd(b *testing.B) {
	keys := benchKeys("add-", 1024)
	for _, depth := range []int{3, 7} {
		b.Run("depth="+strconv.Itoa(depth), func(b *testing.B) {
			cms := newBenchCountMinSketch(b, 1<<14, depth)
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				cms.Add(keys[i%len(keys)], 1)
				i++
			}
		})
	}
}

func BenchmarkCountMinSketchEstimate(b *testing.B) {
	keys := benchKeys("estimate-", 1024)
	cms := newBenchCountMinSketch(b, 1<<14, 7)
	for _, key := range keys {
		cms.Add(key, 1)
	}

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		cms.Estimate(keys[i%len(keys)])
		i++
	}
}

func BenchmarkCountMinSketchMerge(b *testing.B) {
	src := newBenchCountMinSketch(b, 1<<14, 7)
	for _, key := range benchKeys("merge-", 4096) {
		src.Add(key, 1)
	}
	dst := newBenchCountMinSketch(b, 1<<14, 7)

	b.ReportAllocs()
	for b.Loop() {
		if err := dst.Merge(src); err != nil {
			b.Fatalf("Merge() returned error: %v", err)
		}
	}
}

func BenchmarkStreamCollectCountMinSketch(b *testing.B) {
	hasher := maphash.ComparableHasher[string]{}
	for _, n := range benchSizes {
		keys := benchKeys("collect-", n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := NewStream(slices.Values(keys)).
					CollectCountMinSketch(hasher, 1<<14, 7)
				if err != nil {
					b.Fatalf("CollectCountMinSketch() returned error: %v", err)
				}
			}
		})
	}
}

func BenchmarkStreamCollectCountMinSketchByError(b *testing.B) {
	hasher := maphash.ComparableHasher[string]{}
	for _, n := range benchSizes {
		keys := benchKeys("collect-err-", n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := NewStream(slices.Values(keys)).
					CollectCountMinSketchByError(hasher, 0.001, 0.01)
				if err != nil {
					b.Fatalf("CollectCountMinSketchByError() returned error: %v", err)
				}
			}
		})
	}
}
