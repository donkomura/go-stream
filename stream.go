package main

import (
	"iter"
	"slices"
)

// Stream is a lazy pipeline over an iter.Seq.
//
// Intermediate operations are methods that return a new Stream, so a pipeline
// reads in execution order. Operations that introduce a new type (Map, FlatMap,
// Reduce, GroupBy, DistinctBy) are generic methods, which require Go 1.27.
type Stream[A any] struct {
	seq iter.Seq[A]
	err func() error
}

// NewStream wraps an iterator into a Stream that reports no source error.
func NewStream[A any](seq iter.Seq[A]) Stream[A] {
	return Stream[A]{seq: seq}
}

// derive builds the next stage while keeping the source's error reporting.
func (s Stream[A]) derive[B any](seq iter.Seq[B]) Stream[B] {
	return Stream[B]{seq: seq, err: s.err}
}

func (s Stream[A]) Seq() iter.Seq[A] {
	return s.seq
}

// Err reports the error recorded by the most recent iteration of the source.
func (s Stream[A]) Err() error {
	if s.err == nil {
		return nil
	}
	return s.err()
}

func (s Stream[A]) Filter(pred func(A) bool) Stream[A] {
	return s.derive(func(yield func(A) bool) {
		for v := range s.seq {
			if pred(v) && !yield(v) {
				return
			}
		}
	})
}

func (s Stream[A]) Map[B any](fn func(A) B) Stream[B] {
	return s.derive(func(yield func(B) bool) {
		for v := range s.seq {
			if !yield(fn(v)) {
				return
			}
		}
	})
}

func (s Stream[A]) FlatMap[B any](fn func(A) iter.Seq[B]) Stream[B] {
	return s.derive(func(yield func(B) bool) {
		for v := range s.seq {
			for mapped := range fn(v) {
				if !yield(mapped) {
					return
				}
			}
		}
	})
}

// Sort buffers the whole stream, so it is not usable on unbounded sources.
func (s Stream[A]) Sort(cmp func(A, A) int) Stream[A] {
	return s.derive(func(yield func(A) bool) {
		elements := slices.Collect(s.seq)
		slices.SortFunc(elements, cmp)
		for _, v := range elements {
			if !yield(v) {
				return
			}
		}
	})
}

func (s Stream[A]) Take(n int) Stream[A] {
	return s.derive(func(yield func(A) bool) {
		if n <= 0 {
			return
		}

		count := 0
		for v := range s.seq {
			if !yield(v) {
				return
			}
			count++
			if count >= n {
				return
			}
		}
	})
}

func (s Stream[A]) DistinctBy[K comparable](keyFn func(A) K) Stream[A] {
	return s.derive(func(yield func(A) bool) {
		seen := map[K]struct{}{}
		for v := range s.seq {
			key := keyFn(v)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			if !yield(v) {
				return
			}
		}
	})
}

// Distinct is a function rather than a method because a method cannot narrow
// the stream's element type to comparable.
func Distinct[A comparable](s Stream[A]) Stream[A] {
	return s.DistinctBy(func(v A) A { return v })
}

// Collect returns an empty, non-nil slice for an empty stream.
func (s Stream[A]) Collect() []A {
	result := []A{}
	for v := range s.seq {
		result = append(result, v)
	}
	return result
}

func (s Stream[A]) Reduce[R any](init R, fn func(R, A) R) R {
	result := init
	for v := range s.seq {
		result = fn(result, v)
	}
	return result
}

func (s Stream[A]) Count() int {
	count := 0
	for range s.seq {
		count++
	}
	return count
}

func (s Stream[A]) Any(pred func(A) bool) bool {
	for v := range s.seq {
		if pred(v) {
			return true
		}
	}
	return false
}

func (s Stream[A]) All(pred func(A) bool) bool {
	for v := range s.seq {
		if !pred(v) {
			return false
		}
	}
	return true
}

func (s Stream[A]) First() (A, bool) {
	for v := range s.seq {
		return v, true
	}

	var zero A
	return zero, false
}

func (s Stream[A]) Last() (A, bool) {
	var last A
	ok := false
	for v := range s.seq {
		last = v
		ok = true
	}
	return last, ok
}

func (s Stream[A]) GroupBy[K comparable](keyFn func(A) K) map[K][]A {
	result := map[K][]A{}
	for v := range s.seq {
		key := keyFn(v)
		result[key] = append(result[key], v)
	}
	return result
}
