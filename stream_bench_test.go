package main

import (
	"cmp"
	"iter"
	"slices"
	"testing"
)

func BenchmarkStreamFilter(b *testing.B) {
	for _, n := range benchSizes {
		data := sequentialInts(n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				NewStream(slices.Values(data)).
					Filter(func(v int) bool { return v%2 == 0 }).
					Count()
			}
		})
	}
}

func BenchmarkStreamMap(b *testing.B) {
	for _, n := range benchSizes {
		data := sequentialInts(n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				NewStream(slices.Values(data)).
					Map(func(v int) int64 { return int64(v) * 2 }).
					Count()
			}
		})
	}
}

func BenchmarkStreamFlatMap(b *testing.B) {
	for _, n := range benchSizes {
		data := sequentialInts(n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				NewStream(slices.Values(data)).
					FlatMap(func(v int) iter.Seq[int] {
						return slices.Values([]int{v, v + 1, v + 2})
					}).
					Count()
			}
		})
	}
}

func BenchmarkStreamSort(b *testing.B) {
	for _, n := range benchSizes {
		data := shuffledInts(n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				NewStream(slices.Values(data)).
					Sort(cmp.Compare[int]).
					Collect()
			}
		})
	}
}

func BenchmarkStreamTake(b *testing.B) {
	for _, n := range benchSizes {
		data := sequentialInts(n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				NewStream(slices.Values(data)).
					Take(10).
					Collect()
			}
		})
	}
}

func BenchmarkStreamDistinct(b *testing.B) {
	for _, n := range benchSizes {
		data := sequentialInts(n)
		for i := range data {
			data[i] = i % (n/2 + 1)
		}
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				Distinct(NewStream(slices.Values(data))).Count()
			}
		})
	}
}

func BenchmarkStreamGroupBy(b *testing.B) {
	for _, n := range benchSizes {
		data := sequentialInts(n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				NewStream(slices.Values(data)).
					GroupBy(func(v int) int { return v % 8 })
			}
		})
	}
}

func BenchmarkStreamReduce(b *testing.B) {
	for _, n := range benchSizes {
		data := sequentialInts(n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				NewStream(slices.Values(data)).
					Reduce(0, func(acc, v int) int { return acc + v })
			}
		})
	}
}

func BenchmarkStreamCollect(b *testing.B) {
	for _, n := range benchSizes {
		data := sequentialInts(n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				NewStream(slices.Values(data)).Collect()
			}
		})
	}
}

func BenchmarkStreamPipeline(b *testing.B) {
	for _, n := range benchSizes {
		data := sequentialInts(n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				NewStream(slices.Values(data)).
					Filter(func(v int) bool { return v%3 != 0 }).
					Map(func(v int) int { return v * v }).
					Reduce(0, func(acc, v int) int { return acc + v })
			}
		})
	}
}

func BenchmarkPipelineBaselineLoop(b *testing.B) {
	for _, n := range benchSizes {
		data := sequentialInts(n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sum := 0
				for _, v := range data {
					if v%3 == 0 {
						continue
					}
					sum += v * v
				}
				_ = sum
			}
		})
	}
}
