package main

import (
	"errors"
	"hash/maphash"
	"math"
)

var (
	errInvalidWidth      = errors.New("width must be > 0")
	errInvalidDepth      = errors.New("depth must be > 0")
	errInvalidEpsilon    = errors.New("epsilon must be > 0")
	errInvalidDelta      = errors.New("delta must be in (0, 1)")
	errNilCountMinSketch = errors.New("count-min sketch is nil")
	errIncompatibleCMS   = errors.New("count-min sketches are incompatible")
)

// CountMinSketch is a probabilistic frequency estimator over values of type T.
// It never underestimates and may overestimate due to hash collisions.
type CountMinSketch[T any] struct {
	hasher maphash.Hasher[T]
	width  int
	depth  int
	table  [][]uint64
	total  uint64
}

func NewCountMinSketch[T any](hasher maphash.Hasher[T], width, depth int) (*CountMinSketch[T], error) {
	if width <= 0 {
		return nil, errInvalidWidth
	}
	if depth <= 0 {
		return nil, errInvalidDepth
	}

	table := make([][]uint64, depth)
	for i := range table {
		table[i] = make([]uint64, width)
	}

	return &CountMinSketch[T]{
		hasher: hasher,
		width:  width,
		depth:  depth,
		table:  table,
	}, nil
}

func NewComparableCountMinSketch[T comparable](width, depth int) (*CountMinSketch[T], error) {
	return NewCountMinSketch(maphash.ComparableHasher[T]{}, width, depth)
}

// NewCountMinSketchByError creates sketch dimensions from error bounds.
// epsilon is the additive error factor, delta is failure probability.
func NewCountMinSketchByError[T any](hasher maphash.Hasher[T], epsilon, delta float64) (*CountMinSketch[T], error) {
	if epsilon <= 0 {
		return nil, errInvalidEpsilon
	}
	if delta <= 0 || delta >= 1 {
		return nil, errInvalidDelta
	}

	width := int(math.Ceil(math.E / epsilon))
	depth := int(math.Ceil(math.Log(1 / delta)))
	return NewCountMinSketch(hasher, width, depth)
}

func NewComparableCountMinSketchByError[T comparable](epsilon, delta float64) (*CountMinSketch[T], error) {
	return NewCountMinSketchByError(maphash.ComparableHasher[T]{}, epsilon, delta)
}

func (cms *CountMinSketch[T]) Width() int {
	return cms.width
}

func (cms *CountMinSketch[T]) Depth() int {
	return cms.depth
}

func (cms *CountMinSketch[T]) TotalCount() uint64 {
	return cms.total
}

func (cms *CountMinSketch[T]) Add(v T, count uint64) {
	if count == 0 {
		return
	}

	for row := range cms.depth {
		cms.table[row][cms.column(v, row)] += count
	}
	cms.total += count
}

func (cms *CountMinSketch[T]) Estimate(v T) uint64 {
	estimate := uint64(math.MaxUint64)
	for row := range cms.depth {
		estimate = min(estimate, cms.table[row][cms.column(v, row)])
	}
	return estimate
}

func (cms *CountMinSketch[T]) Merge(other *CountMinSketch[T]) error {
	if cms == nil || other == nil {
		return errNilCountMinSketch
	}
	if cms.width != other.width || cms.depth != other.depth {
		return errIncompatibleCMS
	}

	for row := range cms.depth {
		for col := range cms.width {
			cms.table[row][col] += other.table[row][col]
		}
	}
	cms.total += other.total
	return nil
}

func (cms *CountMinSketch[T]) Reset() {
	for row := range cms.depth {
		clear(cms.table[row])
	}
	cms.total = 0
}

func (cms *CountMinSketch[T]) column(v T, row int) int {
	return int(hashRound(cms.hasher, row, v) % uint64(cms.width))
}

func (s Stream[A]) CollectCountMinSketch(hasher maphash.Hasher[A], width, depth int) (*CountMinSketch[A], error) {
	cms, err := NewCountMinSketch(hasher, width, depth)
	if err != nil {
		return nil, err
	}

	for v := range s.seq {
		cms.Add(v, 1)
	}
	return cms, nil
}

func (s Stream[A]) CollectCountMinSketchByError(hasher maphash.Hasher[A], epsilon, delta float64) (*CountMinSketch[A], error) {
	cms, err := NewCountMinSketchByError(hasher, epsilon, delta)
	if err != nil {
		return nil, err
	}

	for v := range s.seq {
		cms.Add(v, 1)
	}
	return cms, nil
}
