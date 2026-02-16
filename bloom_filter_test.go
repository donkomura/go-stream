package main

import (
	"slices"
	"testing"
)

func TestNewBloomFilterValidation(t *testing.T) {
	if _, err := NewBloomFilter(0, 3); err == nil {
		t.Fatalf("expected error for bitSize=0")
	}
	if _, err := NewBloomFilter(128, 0); err == nil {
		t.Fatalf("expected error for hashFuncs=0")
	}
	if _, err := NewBloomFilterByError(0, 0.01); err == nil {
		t.Fatalf("expected error for expectedItems=0")
	}
	if _, err := NewBloomFilterByError(100, 1); err == nil {
		t.Fatalf("expected error for falsePositiveRate=1")
	}
}

func TestNewBloomFilterByErrorDimensions(t *testing.T) {
	bf, err := NewBloomFilterByError(1000, 0.01)
	if err != nil {
		t.Fatalf("NewBloomFilterByError() returned error: %v", err)
	}
	if bf.BitSize() <= 0 || bf.HashFuncs() <= 0 {
		t.Fatalf("invalid bloom dimensions: bitSize=%d hashFuncs=%d", bf.BitSize(), bf.HashFuncs())
	}
}

func TestBloomFilterNoFalseNegative(t *testing.T) {
	bf, err := NewBloomFilter(8192, 6)
	if err != nil {
		t.Fatalf("NewBloomFilter() returned error: %v", err)
	}

	keys := []string{"apple", "banana", "orange", "grape"}
	for _, k := range keys {
		bf.AddString(k)
	}

	if bf.AddedCount() != uint64(len(keys)) {
		t.Fatalf("AddedCount()=%d, expected %d", bf.AddedCount(), len(keys))
	}

	for _, k := range keys {
		if !bf.TestString(k) {
			t.Fatalf("TestString(%q)=false, expected true", k)
		}
	}
}

func TestBloomFilterMergeAndReset(t *testing.T) {
	left, err := NewBloomFilter(2048, 4)
	if err != nil {
		t.Fatalf("NewBloomFilter(left) error: %v", err)
	}
	right, err := NewBloomFilter(2048, 4)
	if err != nil {
		t.Fatalf("NewBloomFilter(right) error: %v", err)
	}

	left.AddString("apple")
	left.AddString("banana")
	right.AddString("orange")
	right.AddString("grape")

	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge() returned error: %v", err)
	}
	if !left.TestString("apple") || !left.TestString("orange") {
		t.Fatalf("merged filter should include keys from both filters")
	}
	if left.AddedCount() != 4 {
		t.Fatalf("AddedCount()=%d, expected 4", left.AddedCount())
	}

	left.Reset()
	if left.AddedCount() != 0 {
		t.Fatalf("AddedCount()=%d, expected 0 after reset", left.AddedCount())
	}
	if left.TestString("apple") {
		t.Fatalf("TestString(apple)=true after reset, expected likely false for empty filter")
	}
}

func TestBloomFilterCollectAggregatesStreamItems(t *testing.T) {
	data := []string{"apple", "banana", "apple", "orange", "banana", "apple"}

	result := Stream(
		slices.Values(data),
		End(BloomFilterCollect(4096, 5, func(s string) string { return s })),
	)
	if result.Err != nil {
		t.Fatalf("BloomFilterCollect() returned error: %v", result.Err)
	}
	if result.Filter == nil {
		t.Fatalf("BloomFilterCollect() returned nil filter")
	}

	for _, key := range []string{"apple", "banana", "orange"} {
		if !result.Filter.TestString(key) {
			t.Fatalf("TestString(%q)=false, expected true", key)
		}
	}
}
