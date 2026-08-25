package main

import (
	"cmp"
	"iter"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestStreamMethodChain(t *testing.T) {
	t.Run("Filter -> Map -> Collect", func(t *testing.T) {
		data := []int{1, 2, 3, 4, 5, 6}

		result := NewStream(slices.Values(data)).
			Filter(func(n int) bool { return n%2 == 0 }).
			Map(func(n int) string { return string(rune('a' + n - 1)) }).
			Collect()

		expected := []string{"b", "d", "f"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})

	t.Run("Sort -> Filter -> Collect", func(t *testing.T) {
		data := []int{3, 1, 4, 1, 5, 9, 2, 6}

		result := NewStream(slices.Values(data)).
			Sort(cmp.Compare[int]).
			Filter(func(n int) bool { return n > 3 }).
			Collect()

		expected := []int{4, 5, 6, 9}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})

	t.Run("Sort descending -> Map -> Collect", func(t *testing.T) {
		data := []int{3, 1, 4, 1, 5}

		result := NewStream(slices.Values(data)).
			Sort(func(a, b int) int { return cmp.Compare(b, a) }).
			Map(func(n int) int { return n * 10 }).
			Collect()

		expected := []int{50, 40, 30, 10, 10}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})

	t.Run("Map -> Sort -> Filter -> Collect", func(t *testing.T) {
		data := []string{"abc", "a", "ab", "abcd"}

		result := NewStream(slices.Values(data)).
			Map(func(s string) int { return len(s) }).
			Sort(cmp.Compare[int]).
			Filter(func(n int) bool { return n >= 2 }).
			Collect()

		expected := []int{2, 3, 4}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})

	t.Run("FlatMap -> Collect", func(t *testing.T) {
		data := []string{"a b", "c"}

		result := NewStream(slices.Values(data)).
			FlatMap(func(s string) iter.Seq[string] { return strings.SplitSeq(s, " ") }).
			Collect()

		expected := []string{"a", "b", "c"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})

	t.Run("Distinct -> Collect", func(t *testing.T) {
		data := []int{1, 2, 1, 3, 2, 4, 3}

		result := Distinct(NewStream(slices.Values(data))).Collect()

		expected := []int{1, 2, 3, 4}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})

	t.Run("DistinctBy -> Take -> Collect", func(t *testing.T) {
		data := []string{"apple", "apple", "banana", "orange", "banana", "grape"}

		result := NewStream(slices.Values(data)).
			DistinctBy(func(s string) string { return s }).
			Take(2).
			Collect()

		expected := []string{"apple", "banana"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})

	t.Run("Filter only", func(t *testing.T) {
		data := []int{1, 2, 3, 4, 5}

		result := NewStream(slices.Values(data)).
			Filter(func(n int) bool { return n > 2 }).
			Collect()

		expected := []int{3, 4, 5}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})

	t.Run("Sort only", func(t *testing.T) {
		data := []int{5, 2, 8, 1, 9}

		result := NewStream(slices.Values(data)).Sort(cmp.Compare[int]).Collect()

		expected := []int{1, 2, 5, 8, 9}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})

	t.Run("Empty stream", func(t *testing.T) {
		data := []int{}

		result := NewStream(slices.Values(data)).
			Filter(func(n int) bool { return n > 0 }).
			Collect()

		expected := []int{}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})

	t.Run("Take stops the upstream source", func(t *testing.T) {
		visited := 0
		source := NewStream(func(yield func(int) bool) {
			for i := range 100 {
				visited++
				if !yield(i) {
					return
				}
			}
		})

		result := source.Take(3).Collect()

		if !reflect.DeepEqual(result, []int{0, 1, 2}) {
			t.Fatalf("Collect() = %v, expected [0 1 2]", result)
		}
		if visited != 3 {
			t.Errorf("visited = %d, expected 3 (Take must stop the source)", visited)
		}
	})
}

func TestStreamTerminalOperations(t *testing.T) {
	t.Run("Reduce sums filtered values", func(t *testing.T) {
		data := []int{1, 2, 3, 4, 5, 6}

		result := NewStream(slices.Values(data)).
			Filter(func(n int) bool { return n%2 == 0 }).
			Reduce(0, func(acc, n int) int { return acc + n })

		if result != 12 {
			t.Errorf("Reduce() = %v, expected 12", result)
		}
	})

	t.Run("Reduce can change the element type", func(t *testing.T) {
		data := []int{1, 2, 3}

		result := NewStream(slices.Values(data)).
			Reduce("", func(acc string, n int) string { return acc + string(rune('0'+n)) })

		if result != "123" {
			t.Errorf("Reduce() = %q, expected %q", result, "123")
		}
	})

	t.Run("Count returns number of elements", func(t *testing.T) {
		data := []string{"a", "b", "c"}

		if got := NewStream(slices.Values(data)).Count(); got != 3 {
			t.Errorf("Count() = %v, expected 3", got)
		}
	})

	t.Run("Any returns true when one element matches", func(t *testing.T) {
		data := []int{1, 3, 4, 7}

		if !NewStream(slices.Values(data)).Any(func(n int) bool { return n%2 == 0 }) {
			t.Error("Any() = false, expected true")
		}
	})

	t.Run("All returns false when one element does not match", func(t *testing.T) {
		data := []int{2, 4, 5, 8}

		if NewStream(slices.Values(data)).All(func(n int) bool { return n%2 == 0 }) {
			t.Error("All() = true, expected false")
		}
	})

	t.Run("First returns first element and true", func(t *testing.T) {
		data := []int{9, 8, 7}

		value, ok := NewStream(slices.Values(data)).First()
		if !ok || value != 9 {
			t.Errorf("First() = (%v, %v), expected (9, true)", value, ok)
		}
	})

	t.Run("Last returns last element and true", func(t *testing.T) {
		data := []int{9, 8, 7}

		value, ok := NewStream(slices.Values(data)).Last()
		if !ok || value != 7 {
			t.Errorf("Last() = (%v, %v), expected (7, true)", value, ok)
		}
	})

	t.Run("First and Last return false for empty stream", func(t *testing.T) {
		data := []int{}

		firstValue, firstOK := NewStream(slices.Values(data)).First()
		if firstOK || firstValue != 0 {
			t.Errorf("First() = (%v, %v), expected (0, false)", firstValue, firstOK)
		}

		lastValue, lastOK := NewStream(slices.Values(data)).Last()
		if lastOK || lastValue != 0 {
			t.Errorf("Last() = (%v, %v), expected (0, false)", lastValue, lastOK)
		}
	})

	t.Run("GroupBy groups values by key", func(t *testing.T) {
		data := []string{"apple", "banana", "apricot", "blueberry", "avocado"}

		result := NewStream(slices.Values(data)).GroupBy(func(s string) byte { return s[0] })

		expected := map[byte][]string{
			'a': {"apple", "apricot", "avocado"},
			'b': {"banana", "blueberry"},
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("GroupBy() = %v, expected %v", result, expected)
		}
	})

	t.Run("Seq exposes the underlying iterator", func(t *testing.T) {
		data := []int{1, 2, 3}

		got := slices.Collect(NewStream(slices.Values(data)).Map(func(n int) int { return n * 2 }).Seq())

		if !reflect.DeepEqual(got, []int{2, 4, 6}) {
			t.Errorf("Seq() = %v, expected [2 4 6]", got)
		}
	})
}
