package main

import (
	"cmp"
	"iter"
	"reflect"
	"slices"
	"testing"
)

func TestStreamContinuationStyle(t *testing.T) {
	t.Run("Filter -> Map -> Collect", func(t *testing.T) {
		data := []int{1, 2, 3, 4, 5, 6}

		result := Stream(
			slices.Values(data),
			Filter(func(n int) bool { return n%2 == 0 },
				Map(func(n int) string { return string(rune('a' + n - 1)) },
					End(Collect[string]()),
				),
			),
		)

		expected := []string{"b", "d", "f"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Stream() = %v, expected %v", result, expected)
		}
	})

	t.Run("Sort -> Filter -> Collect", func(t *testing.T) {
		data := []int{3, 1, 4, 1, 5, 9, 2, 6}

		result := Stream(
			slices.Values(data),
			Sort(cmp.Compare[int],
				Filter(func(n int) bool { return n > 3 },
					End(Collect[int]()),
				),
			),
		)

		expected := []int{4, 5, 6, 9}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Stream() = %v, expected %v", result, expected)
		}
	})

	t.Run("Sort descending -> Map -> Collect", func(t *testing.T) {
		data := []int{3, 1, 4, 1, 5}

		result := Stream(
			slices.Values(data),
			Sort(func(a, b int) int { return cmp.Compare(b, a) },
				Map(func(n int) int { return n * 10 },
					End(Collect[int]()),
				),
			),
		)

		expected := []int{50, 40, 30, 10, 10}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Stream() = %v, expected %v", result, expected)
		}
	})

	t.Run("Map -> Sort -> Filter -> Collect", func(t *testing.T) {
		data := []string{"abc", "a", "ab", "abcd"}

		result := Stream(
			slices.Values(data),
			Map(func(s string) int { return len(s) },
				Sort(cmp.Compare[int],
					Filter(func(n int) bool { return n >= 2 },
						End(Collect[int]()),
					),
				),
			),
		)

		expected := []int{2, 3, 4}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Stream() = %v, expected %v", result, expected)
		}
	})

	t.Run("Distinct -> Collect", func(t *testing.T) {
		data := []int{1, 2, 1, 3, 2, 4, 3}

		result := Stream(
			slices.Values(data),
			Distinct(
				End(Collect[int]()),
			),
		)

		expected := []int{1, 2, 3, 4}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Stream() = %v, expected %v", result, expected)
		}
	})

	t.Run("Distinct -> Take -> Collect", func(t *testing.T) {
		data := []string{"apple", "apple", "banana", "orange", "banana", "grape"}

		result := Stream(
			slices.Values(data),
			Distinct(
				Take(2,
					End(Collect[string]()),
				),
			),
		)

		expected := []string{"apple", "banana"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Stream() = %v, expected %v", result, expected)
		}
	})

	t.Run("Filter only with Collect", func(t *testing.T) {
		data := []int{1, 2, 3, 4, 5}

		result := Stream(
			slices.Values(data),
			Filter(func(n int) bool { return n > 2 },
				End(Collect[int]()),
			),
		)

		expected := []int{3, 4, 5}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Stream() = %v, expected %v", result, expected)
		}
	})

	t.Run("Sort only with Collect", func(t *testing.T) {
		data := []int{5, 2, 8, 1, 9}

		result := Stream(
			slices.Values(data),
			Sort(cmp.Compare[int],
				End(Collect[int]()),
			),
		)

		expected := []int{1, 2, 5, 8, 9}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Stream() = %v, expected %v", result, expected)
		}
	})

	t.Run("Empty stream with Collect", func(t *testing.T) {
		data := []int{}

		result := Stream(
			slices.Values(data),
			Filter(func(n int) bool { return n > 0 },
				End(Collect[int]()),
			),
		)

		expected := []int{}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Stream() = %v, expected %v", result, expected)
		}
	})
}

func TestCollectFunction(t *testing.T) {
	t.Run("Collect converts iter.Seq to slice", func(t *testing.T) {
		data := []int{1, 2, 3, 4, 5}
		seq := slices.Values(data)

		collect := Collect[int]()
		result := collect(seq)

		expected := []int{1, 2, 3, 4, 5}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})

	t.Run("Collect with empty sequence", func(t *testing.T) {
		data := []int{}
		seq := slices.Values(data)

		collect := Collect[int]()
		result := collect(seq)

		expected := []int{}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Collect() = %v, expected %v", result, expected)
		}
	})
}

func TestSortFunction(t *testing.T) {
	t.Run("Sort orders elements", func(t *testing.T) {
		data := []int{3, 1, 4, 1, 5, 9, 2, 6}
		seq := slices.Values(data)

		sortFunc := Sort(cmp.Compare[int],
			func(seq iter.Seq[int]) []int {
				result := []int{}
				for v := range seq {
					result = append(result, v)
				}
				return result
			},
		)

		result := sortFunc(seq)
		expected := []int{1, 1, 2, 3, 4, 5, 6, 9}

		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Sort() = %v, expected %v", result, expected)
		}
	})

	t.Run("Sort with custom comparator", func(t *testing.T) {
		data := []int{1, 2, 3, 4, 5}
		seq := slices.Values(data)

		sortFunc := Sort(func(a, b int) int { return cmp.Compare(b, a) },
			func(seq iter.Seq[int]) []int {
				result := []int{}
				for v := range seq {
					result = append(result, v)
				}
				return result
			},
		)

		result := sortFunc(seq)
		expected := []int{5, 4, 3, 2, 1}

		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Sort() = %v, expected %v", result, expected)
		}
	})
}

func TestAggregateFunctions(t *testing.T) {
	t.Run("Filter -> Reduce sums even numbers", func(t *testing.T) {
		data := []int{1, 2, 3, 4, 5, 6}

		result := Stream(
			slices.Values(data),
			Filter(func(n int) bool { return n%2 == 0 },
				End(Reduce(0, func(acc, n int) int { return acc + n })),
			),
		)

		expected := 12
		if result != expected {
			t.Errorf("Stream() = %v, expected %v", result, expected)
		}
	})

	t.Run("Count returns number of elements", func(t *testing.T) {
		data := []string{"a", "b", "c"}

		result := Stream(
			slices.Values(data),
			End(Count[string]()),
		)

		expected := 3
		if result != expected {
			t.Errorf("Count() = %v, expected %v", result, expected)
		}
	})

	t.Run("Any returns true when one element matches", func(t *testing.T) {
		data := []int{1, 3, 4, 7}

		result := Stream(
			slices.Values(data),
			End(Any(func(n int) bool { return n%2 == 0 })),
		)

		if !result {
			t.Errorf("Any() = %v, expected true", result)
		}
	})

	t.Run("All returns false when one element does not match", func(t *testing.T) {
		data := []int{2, 4, 5, 8}

		result := Stream(
			slices.Values(data),
			End(All(func(n int) bool { return n%2 == 0 })),
		)

		if result {
			t.Errorf("All() = %v, expected false", result)
		}
	})

	t.Run("First returns first element and true", func(t *testing.T) {
		data := []int{9, 8, 7}

		result := Stream(
			slices.Values(data),
			End(First[int]()),
		)

		if !result.OK || result.Value != 9 {
			t.Errorf("First() = (%v, %v), expected (9, true)", result.Value, result.OK)
		}
	})

	t.Run("Last returns last element and true", func(t *testing.T) {
		data := []int{9, 8, 7}

		result := Stream(
			slices.Values(data),
			End(Last[int]()),
		)

		if !result.OK || result.Value != 7 {
			t.Errorf("Last() = (%v, %v), expected (7, true)", result.Value, result.OK)
		}
	})

	t.Run("First and Last return false for empty stream", func(t *testing.T) {
		data := []int{}

		first := Stream(
			slices.Values(data),
			End(First[int]()),
		)
		last := Stream(
			slices.Values(data),
			End(Last[int]()),
		)

		if first.OK || first.Value != 0 {
			t.Errorf("First() = (%v, %v), expected (0, false)", first.Value, first.OK)
		}
		if last.OK || last.Value != 0 {
			t.Errorf("Last() = (%v, %v), expected (0, false)", last.Value, last.OK)
		}
	})

	t.Run("GroupBy groups values by key", func(t *testing.T) {
		data := []string{"apple", "banana", "apricot", "blueberry", "avocado"}

		result := Stream(
			slices.Values(data),
			End(GroupBy(func(s string) byte { return s[0] })),
		)

		expected := map[byte][]string{
			'a': {"apple", "apricot", "avocado"},
			'b': {"banana", "blueberry"},
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("GroupBy() = %v, expected %v", result, expected)
		}
	})
}
