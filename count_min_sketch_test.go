package main

import (
	"hash/maphash"
	"slices"
	"testing"
)

func TestCollectCountMinSketchAggregatesStreamItems(t *testing.T) {
	data := []string{"apple", "banana", "apple", "orange", "banana", "apple"}

	cms, err := NewStream(slices.Values(data)).
		CollectCountMinSketch(maphash.ComparableHasher[string]{}, 128, 5)
	if err != nil {
		t.Fatalf("CollectCountMinSketch() returned error: %v", err)
	}
	if cms == nil {
		t.Fatalf("CollectCountMinSketch() returned nil sketch")
	}
	if cms.TotalCount() != uint64(len(data)) {
		t.Fatalf("TotalCount()=%d, expected %d", cms.TotalCount(), len(data))
	}

	actual := map[string]uint64{
		"apple":  3,
		"banana": 2,
		"orange": 1,
	}
	for key, expectedMin := range actual {
		if estimated := cms.Estimate(key); estimated < expectedMin {
			t.Fatalf("Estimate(%q)=%d, expected >= %d", key, estimated, expectedMin)
		}
	}
}

func TestNewCountMinSketchValidation(t *testing.T) {
	hasher := maphash.ComparableHasher[string]{}

	if _, err := NewCountMinSketch(hasher, 0, 3); err == nil {
		t.Fatalf("expected error for width=0")
	}
	if _, err := NewCountMinSketch(hasher, 10, 0); err == nil {
		t.Fatalf("expected error for depth=0")
	}
	if _, err := NewCountMinSketchByError(hasher, 0, 0.01); err == nil {
		t.Fatalf("expected error for epsilon=0")
	}
	if _, err := NewCountMinSketchByError(hasher, 0.01, 1); err == nil {
		t.Fatalf("expected error for delta=1")
	}
}

func TestNewCountMinSketchByErrorDimensions(t *testing.T) {
	cms, err := NewComparableCountMinSketchByError[string](0.01, 0.01)
	if err != nil {
		t.Fatalf("NewComparableCountMinSketchByError() returned error: %v", err)
	}
	if cms.Width() <= 0 || cms.Depth() <= 0 {
		t.Fatalf("invalid dimensions: width=%d depth=%d", cms.Width(), cms.Depth())
	}
}

func TestCountMinSketchNoUnderestimate(t *testing.T) {
	cms, err := NewComparableCountMinSketch[string](512, 6)
	if err != nil {
		t.Fatalf("NewComparableCountMinSketch() returned error: %v", err)
	}

	actual := map[string]uint64{
		"apple":  5,
		"banana": 3,
		"orange": 7,
		"grape":  2,
	}

	var total uint64
	for k, c := range actual {
		cms.Add(k, c)
		total += c
	}

	if cms.TotalCount() != total {
		t.Fatalf("TotalCount()=%d, expected %d", cms.TotalCount(), total)
	}

	for k, expectedMin := range actual {
		if estimated := cms.Estimate(k); estimated < expectedMin {
			t.Fatalf("Estimate(%q)=%d, expected >= %d", k, estimated, expectedMin)
		}
	}
}

func TestCountMinSketchMergeAndReset(t *testing.T) {
	left, err := NewComparableCountMinSketch[string](256, 5)
	if err != nil {
		t.Fatalf("NewComparableCountMinSketch(left) error: %v", err)
	}
	right, err := NewComparableCountMinSketch[string](256, 5)
	if err != nil {
		t.Fatalf("NewComparableCountMinSketch(right) error: %v", err)
	}

	left.Add("apple", 2)
	left.Add("banana", 4)
	right.Add("apple", 3)
	right.Add("orange", 5)

	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge() returned error: %v", err)
	}

	if left.TotalCount() != 14 {
		t.Fatalf("TotalCount()=%d, expected 14", left.TotalCount())
	}
	if left.Estimate("apple") < 5 {
		t.Fatalf("Estimate(apple)=%d, expected >= 5", left.Estimate("apple"))
	}
	if left.Estimate("banana") < 4 {
		t.Fatalf("Estimate(banana)=%d, expected >= 4", left.Estimate("banana"))
	}
	if left.Estimate("orange") < 5 {
		t.Fatalf("Estimate(orange)=%d, expected >= 5", left.Estimate("orange"))
	}

	left.Reset()
	if left.TotalCount() != 0 {
		t.Fatalf("TotalCount()=%d, expected 0 after reset", left.TotalCount())
	}
	if left.Estimate("apple") != 0 {
		t.Fatalf("Estimate(apple)=%d, expected 0 after reset", left.Estimate("apple"))
	}
}

func TestCountMinSketchMergeRejectsIncompatibleDimensions(t *testing.T) {
	left, err := NewComparableCountMinSketch[string](256, 5)
	if err != nil {
		t.Fatalf("NewComparableCountMinSketch(left) error: %v", err)
	}
	right, err := NewComparableCountMinSketch[string](128, 5)
	if err != nil {
		t.Fatalf("NewComparableCountMinSketch(right) error: %v", err)
	}

	if err := left.Merge(right); err == nil {
		t.Fatal("Merge() = nil, expected an incompatibility error")
	}
}

// TestCountMinSketchAnyElementType covers a non-comparable element type,
// which the pre-1.27 string-keyed sketch could not represent directly.
func TestCountMinSketchAnyElementType(t *testing.T) {
	cms, err := NewCountMinSketch(recordHasher{}, 256, 5)
	if err != nil {
		t.Fatalf("NewCountMinSketch() returned error: %v", err)
	}

	record := []string{"apple", "2"}
	cms.Add(record, 3)

	if got := cms.Estimate([]string{"apple", "2"}); got < 3 {
		t.Fatalf("Estimate(%v)=%d, expected >= 3", record, got)
	}
}
