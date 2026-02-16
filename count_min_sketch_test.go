package main

import (
	"slices"
	"testing"
)

func TestCountMinSketchCollectAggregatesStreamItems(t *testing.T) {
	data := []string{"apple", "banana", "apple", "orange", "banana", "apple"}

	result := Stream(
		slices.Values(data),
		End(CountMinSketchCollect(128, 5, func(s string) string { return s })),
	)
	if result.Err != nil {
		t.Fatalf("CountMinSketchCollect() returned error: %v", result.Err)
	}

	cms := result.Sketch
	if cms == nil {
		t.Fatalf("CountMinSketchCollect() returned nil sketch")
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
		estimated := cms.EstimateString(key)
		if estimated < expectedMin {
			t.Fatalf("EstimateString(%q)=%d, expected >= %d", key, estimated, expectedMin)
		}
	}
}

func TestNewCountMinSketchValidation(t *testing.T) {
	if _, err := NewCountMinSketch(0, 3); err == nil {
		t.Fatalf("expected error for width=0")
	}
	if _, err := NewCountMinSketch(10, 0); err == nil {
		t.Fatalf("expected error for depth=0")
	}
	if _, err := NewCountMinSketchByError(0, 0.01); err == nil {
		t.Fatalf("expected error for epsilon=0")
	}
	if _, err := NewCountMinSketchByError(0.01, 1); err == nil {
		t.Fatalf("expected error for delta=1")
	}
}

func TestNewCountMinSketchByErrorDimensions(t *testing.T) {
	cms, err := NewCountMinSketchByError(0.01, 0.01)
	if err != nil {
		t.Fatalf("NewCountMinSketchByError() returned error: %v", err)
	}
	if cms.Width() <= 0 || cms.Depth() <= 0 {
		t.Fatalf("invalid dimensions: width=%d depth=%d", cms.Width(), cms.Depth())
	}
}

func TestCountMinSketchNoUnderestimate(t *testing.T) {
	cms, err := NewCountMinSketch(512, 6)
	if err != nil {
		t.Fatalf("NewCountMinSketch() returned error: %v", err)
	}

	actual := map[string]uint64{
		"apple":  5,
		"banana": 3,
		"orange": 7,
		"grape":  2,
	}

	var total uint64
	for k, c := range actual {
		cms.AddString(k, c)
		total += c
	}

	if cms.TotalCount() != total {
		t.Fatalf("TotalCount()=%d, expected %d", cms.TotalCount(), total)
	}

	for k, expectedMin := range actual {
		estimated := cms.EstimateString(k)
		if estimated < expectedMin {
			t.Fatalf("EstimateString(%q)=%d, expected >= %d", k, estimated, expectedMin)
		}
	}
}

func TestCountMinSketchMergeAndReset(t *testing.T) {
	left, err := NewCountMinSketch(256, 5)
	if err != nil {
		t.Fatalf("NewCountMinSketch(left) error: %v", err)
	}
	right, err := NewCountMinSketch(256, 5)
	if err != nil {
		t.Fatalf("NewCountMinSketch(right) error: %v", err)
	}

	left.AddString("apple", 2)
	left.AddString("banana", 4)
	right.AddString("apple", 3)
	right.AddString("orange", 5)

	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge() returned error: %v", err)
	}

	if left.TotalCount() != 14 {
		t.Fatalf("TotalCount()=%d, expected 14", left.TotalCount())
	}
	if left.EstimateString("apple") < 5 {
		t.Fatalf("EstimateString(apple)=%d, expected >= 5", left.EstimateString("apple"))
	}
	if left.EstimateString("banana") < 4 {
		t.Fatalf("EstimateString(banana)=%d, expected >= 4", left.EstimateString("banana"))
	}
	if left.EstimateString("orange") < 5 {
		t.Fatalf("EstimateString(orange)=%d, expected >= 5", left.EstimateString("orange"))
	}

	left.Reset()
	if left.TotalCount() != 0 {
		t.Fatalf("TotalCount()=%d, expected 0 after reset", left.TotalCount())
	}
	if left.EstimateString("apple") != 0 {
		t.Fatalf("EstimateString(apple)=%d, expected 0 after reset", left.EstimateString("apple"))
	}
}
