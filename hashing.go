package main

import "hash/maphash"

// hashSeed is process-wide rather than per-instance so that two independently
// constructed filters or sketches with the same dimensions produce the same
// bit positions and can therefore be merged.
var hashSeed = maphash.MakeSeed()

// hashRound derives the round-th independent hash of v from a single seed by
// mixing the round number into the hash before the value itself.
func hashRound[T any](hasher maphash.Hasher[T], round int, v T) uint64 {
	var h maphash.Hash
	h.SetSeed(hashSeed)
	maphash.WriteComparable(&h, round)
	hasher.Hash(&h, v)
	return h.Sum64()
}
