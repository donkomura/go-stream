package main

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"iter"
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

// CountMinSketch is a probabilistic frequency estimator.
// It never underestimates and may overestimate due to hash collisions.
type CountMinSketch struct {
	width int
	depth int
	table [][]uint64
	total uint64
}

type CountMinSketchResult struct {
	Sketch *CountMinSketch
	Err    error
}

func NewCountMinSketch(width, depth int) (*CountMinSketch, error) {
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

	return &CountMinSketch{
		width: width,
		depth: depth,
		table: table,
	}, nil
}

// NewCountMinSketchByError creates sketch dimensions from error bounds.
// epsilon is the additive error factor, delta is failure probability.
func NewCountMinSketchByError(epsilon, delta float64) (*CountMinSketch, error) {
	if epsilon <= 0 {
		return nil, errInvalidEpsilon
	}
	if delta <= 0 || delta >= 1 {
		return nil, errInvalidDelta
	}

	width := int(math.Ceil(math.E / epsilon))
	depth := int(math.Ceil(math.Log(1 / delta)))
	return NewCountMinSketch(width, depth)
}

func (cms *CountMinSketch) Width() int {
	return cms.width
}

func (cms *CountMinSketch) Depth() int {
	return cms.depth
}

func (cms *CountMinSketch) TotalCount() uint64 {
	return cms.total
}

func (cms *CountMinSketch) AddString(key string, count uint64) {
	cms.AddBytes([]byte(key), count)
}

func (cms *CountMinSketch) AddBytes(key []byte, count uint64) {
	if count == 0 {
		return
	}

	for row := 0; row < cms.depth; row++ {
		col := cms.column(key, row)
		cms.table[row][col] += count
	}
	cms.total += count
}

func (cms *CountMinSketch) EstimateString(key string) uint64 {
	return cms.EstimateBytes([]byte(key))
}

func (cms *CountMinSketch) EstimateBytes(key []byte) uint64 {
	min := uint64(math.MaxUint64)
	for row := 0; row < cms.depth; row++ {
		col := cms.column(key, row)
		v := cms.table[row][col]
		if v < min {
			min = v
		}
	}
	return min
}

func (cms *CountMinSketch) Merge(other *CountMinSketch) error {
	if cms == nil || other == nil {
		return errNilCountMinSketch
	}
	if cms.width != other.width || cms.depth != other.depth {
		return errIncompatibleCMS
	}

	for row := 0; row < cms.depth; row++ {
		for col := 0; col < cms.width; col++ {
			cms.table[row][col] += other.table[row][col]
		}
	}
	cms.total += other.total
	return nil
}

func (cms *CountMinSketch) Reset() {
	for row := 0; row < cms.depth; row++ {
		clear(cms.table[row])
	}
	cms.total = 0
}

func (cms *CountMinSketch) column(key []byte, row int) int {
	return int(hashRowKey(key, row) % uint64(cms.width))
}

func hashRowKey(key []byte, row int) uint64 {
	var rowPrefix [8]byte
	binary.LittleEndian.PutUint64(rowPrefix[:], uint64(row))

	h := fnv.New64a()
	_, _ = h.Write(rowPrefix[:])
	_, _ = h.Write(key)
	return h.Sum64()
}

func CountMinSketchCollect[A any](width, depth int, keyFn func(A) string) func(iter.Seq[A]) CountMinSketchResult {
	return func(seq iter.Seq[A]) CountMinSketchResult {
		cms, err := NewCountMinSketch(width, depth)
		if err != nil {
			return CountMinSketchResult{Err: err}
		}

		for v := range seq {
			cms.AddString(keyFn(v), 1)
		}
		return CountMinSketchResult{Sketch: cms}
	}
}

func CountMinSketchCollectByError[A any](epsilon, delta float64, keyFn func(A) string) func(iter.Seq[A]) CountMinSketchResult {
	return func(seq iter.Seq[A]) CountMinSketchResult {
		cms, err := NewCountMinSketchByError(epsilon, delta)
		if err != nil {
			return CountMinSketchResult{Err: err}
		}

		for v := range seq {
			cms.AddString(keyFn(v), 1)
		}
		return CountMinSketchResult{Sketch: cms}
	}
}
