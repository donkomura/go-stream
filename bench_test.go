package main

import (
	"math/rand/v2"
	"strconv"
)

var benchSizes = []int{100, 10000}

func sequentialInts(n int) []int {
	data := make([]int, n)
	for i := range data {
		data[i] = i
	}
	return data
}

func shuffledInts(n int) []int {
	data := sequentialInts(n)
	rng := rand.New(rand.NewPCG(1, 2))
	rng.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})
	return data
}

func benchKeys(prefix string, n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = prefix + strconv.Itoa(i)
	}
	return keys
}

func sizeLabel(n int) string {
	return "n=" + strconv.Itoa(n)
}
