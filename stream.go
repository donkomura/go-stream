package main

import (
	"iter"
	"slices"
)

type AggregateResult[A any] struct {
	Value A
	OK    bool
}

func Stream[F, A any](seqA iter.Seq[A], cont func(iter.Seq[A]) F) F {
	return cont(seqA)
}

func End[F any](f F) F {
	return f
}

func Sort[F, A any](cmp func(A, A) int, cont func(iter.Seq[A]) F) func(iter.Seq[A]) F {
	return func(seq iter.Seq[A]) F {
		elements := []A{}
		for v := range seq {
			elements = append(elements, v)
		}
		slices.SortFunc(elements, cmp)
		return cont(slices.Values(elements))
	}
}

func Filter[F, A any](fn func(A) bool, cont func(iter.Seq[A]) F) func(iter.Seq[A]) F {
	return func(seq iter.Seq[A]) F {
		return cont(func(yield func(A) bool) {
			for v := range seq {
				if fn(v) {
					if !yield(v) {
						return
					}
				}
			}
		})
	}
}

func Map[F, A, B any](fn func(A) B, cont func(iter.Seq[B]) F) func(iter.Seq[A]) F {
	return func(seq iter.Seq[A]) F {
		return cont(func(yield func(B) bool) {
			for v := range seq {
				if !yield(fn(v)) {
					return
				}
			}
		})
	}
}

func FlatMap[F, A, B any](fn func(A) iter.Seq[B], cont func(iter.Seq[B]) F) func(iter.Seq[A]) F {
	return func(seq iter.Seq[A]) F {
		return cont(func(yield func(B) bool) {
			for v := range seq {
				for mapped := range fn(v) {
					if !yield(mapped) {
						return
					}
				}
			}
		})
	}
}

func Distinct[A comparable, F any](cont func(iter.Seq[A]) F) func(iter.Seq[A]) F {
	return func(seq iter.Seq[A]) F {
		return cont(func(yield func(A) bool) {
			seen := map[A]struct{}{}
			for v := range seq {
				if _, ok := seen[v]; ok {
					continue
				}
				seen[v] = struct{}{}
				if !yield(v) {
					return
				}
			}
		})
	}
}

func Take[A any, F any](n int, cont func(iter.Seq[A]) F) func(iter.Seq[A]) F {
	return func(seq iter.Seq[A]) F {
		return cont(func(yield func(A) bool) {
			if n <= 0 {
				return
			}

			count := 0
			for v := range seq {
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
}

func Collect[E any]() func(iter.Seq[E]) []E {
	return func(seq iter.Seq[E]) []E {
		result := []E{}
		for v := range seq {
			result = append(result, v)
		}
		return result
	}
}

func Reduce[A, R any](init R, fn func(R, A) R) func(iter.Seq[A]) R {
	return func(seq iter.Seq[A]) R {
		result := init
		for v := range seq {
			result = fn(result, v)
		}
		return result
	}
}

func Count[A any]() func(iter.Seq[A]) int {
	return func(seq iter.Seq[A]) int {
		count := 0
		for range seq {
			count++
		}
		return count
	}
}

func Any[A any](pred func(A) bool) func(iter.Seq[A]) bool {
	return func(seq iter.Seq[A]) bool {
		for v := range seq {
			if pred(v) {
				return true
			}
		}
		return false
	}
}

func All[A any](pred func(A) bool) func(iter.Seq[A]) bool {
	return func(seq iter.Seq[A]) bool {
		for v := range seq {
			if !pred(v) {
				return false
			}
		}
		return true
	}
}

func First[A any]() func(iter.Seq[A]) AggregateResult[A] {
	return func(seq iter.Seq[A]) AggregateResult[A] {
		for v := range seq {
			return AggregateResult[A]{Value: v, OK: true}
		}
		return AggregateResult[A]{}
	}
}

func Last[A any]() func(iter.Seq[A]) AggregateResult[A] {
	return func(seq iter.Seq[A]) AggregateResult[A] {
		var last A
		ok := false
		for v := range seq {
			last = v
			ok = true
		}
		return AggregateResult[A]{Value: last, OK: ok}
	}
}

func GroupBy[A any, K comparable](keyFn func(A) K) func(iter.Seq[A]) map[K][]A {
	return func(seq iter.Seq[A]) map[K][]A {
		result := map[K][]A{}
		for v := range seq {
			key := keyFn(v)
			result[key] = append(result[key], v)
		}
		return result
	}
}
