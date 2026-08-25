package main

import (
	"hash/maphash"
	"slices"
	"testing"
)

func TestNewBloomFilterValidation(t *testing.T) {
	hasher := maphash.ComparableHasher[string]{}

	if _, err := NewBloomFilter(hasher, 0, 3); err == nil {
		t.Fatalf("expected error for bitSize=0")
	}
	if _, err := NewBloomFilter(hasher, 128, 0); err == nil {
		t.Fatalf("expected error for hashFuncs=0")
	}
	if _, err := NewBloomFilterByError(hasher, 0, 0.01); err == nil {
		t.Fatalf("expected error for expectedItems=0")
	}
	if _, err := NewBloomFilterByError(hasher, 100, 1); err == nil {
		t.Fatalf("expected error for falsePositiveRate=1")
	}
}

func TestNewBloomFilterByErrorDimensions(t *testing.T) {
	bf, err := NewComparableBloomFilterByError[string](1000, 0.01)
	if err != nil {
		t.Fatalf("NewComparableBloomFilterByError() returned error: %v", err)
	}
	if bf.BitSize() <= 0 || bf.HashFuncs() <= 0 {
		t.Fatalf("invalid bloom dimensions: bitSize=%d hashFuncs=%d", bf.BitSize(), bf.HashFuncs())
	}
}

func TestBloomFilterNoFalseNegative(t *testing.T) {
	bf, err := NewComparableBloomFilter[string](8192, 6)
	if err != nil {
		t.Fatalf("NewComparableBloomFilter() returned error: %v", err)
	}

	keys := []string{"apple", "banana", "orange", "grape"}
	for _, k := range keys {
		bf.Add(k)
	}

	if bf.AddedCount() != uint64(len(keys)) {
		t.Fatalf("AddedCount()=%d, expected %d", bf.AddedCount(), len(keys))
	}

	for _, k := range keys {
		if !bf.Test(k) {
			t.Fatalf("Test(%q)=false, expected true", k)
		}
	}
}

// TestBloomFilterAnyElementType covers a non-comparable element type,
// which the pre-1.27 string-keyed filter could not represent directly.
func TestBloomFilterAnyElementType(t *testing.T) {
	bf, err := NewBloomFilter(recordHasher{}, 4096, 5)
	if err != nil {
		t.Fatalf("NewBloomFilter() returned error: %v", err)
	}

	records := [][]string{{"apple", "2"}, {"banana", "1"}}
	for _, r := range records {
		bf.Add(r)
	}

	for _, r := range records {
		if !bf.Test(r) {
			t.Fatalf("Test(%v)=false, expected true", r)
		}
	}
	if bf.Test([]string{"durian", "9"}) {
		t.Errorf("Test(durian) = true, expected false for a well dispersed filter")
	}
}

type recordHasher struct{}

func (recordHasher) Hash(h *maphash.Hash, record []string) {
	maphash.WriteComparable(h, len(record))
	for _, field := range record {
		maphash.WriteComparable(h, len(field))
		h.WriteString(field)
	}
}

func (recordHasher) Equal(x, y []string) bool { return slices.Equal(x, y) }

func TestBloomFilterMergeAndReset(t *testing.T) {
	left, err := NewComparableBloomFilter[string](2048, 4)
	if err != nil {
		t.Fatalf("NewComparableBloomFilter(left) error: %v", err)
	}
	right, err := NewComparableBloomFilter[string](2048, 4)
	if err != nil {
		t.Fatalf("NewComparableBloomFilter(right) error: %v", err)
	}

	left.Add("apple")
	left.Add("banana")
	right.Add("orange")
	right.Add("grape")

	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge() returned error: %v", err)
	}
	if !left.Test("apple") || !left.Test("orange") {
		t.Fatalf("merged filter should include keys from both filters")
	}
	if left.AddedCount() != 4 {
		t.Fatalf("AddedCount()=%d, expected 4", left.AddedCount())
	}

	left.Reset()
	if left.AddedCount() != 0 {
		t.Fatalf("AddedCount()=%d, expected 0 after reset", left.AddedCount())
	}
	if left.Test("apple") {
		t.Fatalf("Test(apple)=true after reset, expected likely false for empty filter")
	}
}

func TestBloomFilterMergeRejectsIncompatibleDimensions(t *testing.T) {
	left, err := NewComparableBloomFilter[string](2048, 4)
	if err != nil {
		t.Fatalf("NewComparableBloomFilter(left) error: %v", err)
	}
	right, err := NewComparableBloomFilter[string](1024, 4)
	if err != nil {
		t.Fatalf("NewComparableBloomFilter(right) error: %v", err)
	}

	if err := left.Merge(right); err == nil {
		t.Fatal("Merge() = nil, expected an incompatibility error")
	}
}

func TestCollectBloomFilterAggregatesStreamItems(t *testing.T) {
	data := []string{"apple", "banana", "apple", "orange", "banana", "apple"}

	filter, err := NewStream(slices.Values(data)).
		CollectBloomFilter(maphash.ComparableHasher[string]{}, 4096, 5)
	if err != nil {
		t.Fatalf("CollectBloomFilter() returned error: %v", err)
	}
	if filter == nil {
		t.Fatalf("CollectBloomFilter() returned nil filter")
	}

	for _, key := range []string{"apple", "banana", "orange"} {
		if !filter.Test(key) {
			t.Fatalf("Test(%q)=false, expected true", key)
		}
	}
}

func TestCollectBloomFilterReportsConstructionError(t *testing.T) {
	data := []string{"apple"}

	if _, err := NewStream(slices.Values(data)).
		CollectBloomFilter(maphash.ComparableHasher[string]{}, 0, 5); err == nil {
		t.Fatal("CollectBloomFilter() = nil error, expected an error for bitSize=0")
	}
}
