package main

import (
	"errors"
	"hash/maphash"
	"math"
)

var (
	errInvalidBitSize           = errors.New("bitSize must be > 0")
	errInvalidHashFuncs         = errors.New("hashFuncs must be > 0")
	errInvalidExpectedItems     = errors.New("expectedItems must be > 0")
	errInvalidFalsePositiveRate = errors.New("falsePositiveRate must be in (0, 1)")
	errNilBloomFilter           = errors.New("bloom filter is nil")
	errIncompatibleBloomFilter  = errors.New("bloom filters are incompatible")
)

// BloomFilter is a probabilistic set for membership tests over values of type T.
// It can return false positives but never false negatives.
type BloomFilter[T any] struct {
	hasher    maphash.Hasher[T]
	bitSize   int
	hashFuncs int
	bits      []uint64
	added     uint64
}

func NewBloomFilter[T any](hasher maphash.Hasher[T], bitSize, hashFuncs int) (*BloomFilter[T], error) {
	if bitSize <= 0 {
		return nil, errInvalidBitSize
	}
	if hashFuncs <= 0 {
		return nil, errInvalidHashFuncs
	}

	wordCount := (bitSize + 63) / 64
	return &BloomFilter[T]{
		hasher:    hasher,
		bitSize:   bitSize,
		hashFuncs: hashFuncs,
		bits:      make([]uint64, wordCount),
	}, nil
}

func NewComparableBloomFilter[T comparable](bitSize, hashFuncs int) (*BloomFilter[T], error) {
	return NewBloomFilter(maphash.ComparableHasher[T]{}, bitSize, hashFuncs)
}

// NewBloomFilterByError calculates parameters from capacity and false positive rate.
func NewBloomFilterByError[T any](hasher maphash.Hasher[T], expectedItems int, falsePositiveRate float64) (*BloomFilter[T], error) {
	if expectedItems <= 0 {
		return nil, errInvalidExpectedItems
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		return nil, errInvalidFalsePositiveRate
	}

	n := float64(expectedItems)
	p := falsePositiveRate
	ln2 := math.Ln2
	m := int(math.Ceil((-n * math.Log(p)) / (ln2 * ln2)))
	k := max(int(math.Ceil((float64(m)/n)*ln2)), 1)

	return NewBloomFilter(hasher, m, k)
}

func NewComparableBloomFilterByError[T comparable](expectedItems int, falsePositiveRate float64) (*BloomFilter[T], error) {
	return NewBloomFilterByError(maphash.ComparableHasher[T]{}, expectedItems, falsePositiveRate)
}

func (bf *BloomFilter[T]) BitSize() int {
	return bf.bitSize
}

func (bf *BloomFilter[T]) HashFuncs() int {
	return bf.hashFuncs
}

func (bf *BloomFilter[T]) AddedCount() uint64 {
	return bf.added
}

func (bf *BloomFilter[T]) Add(v T) {
	for round := range bf.hashFuncs {
		bf.setBit(bf.bitIndex(v, round))
	}
	bf.added++
}

func (bf *BloomFilter[T]) Test(v T) bool {
	for round := range bf.hashFuncs {
		if !bf.hasBit(bf.bitIndex(v, round)) {
			return false
		}
	}
	return true
}

func (bf *BloomFilter[T]) Merge(other *BloomFilter[T]) error {
	if bf == nil || other == nil {
		return errNilBloomFilter
	}
	if bf.bitSize != other.bitSize || bf.hashFuncs != other.hashFuncs {
		return errIncompatibleBloomFilter
	}

	for i := range bf.bits {
		bf.bits[i] |= other.bits[i]
	}
	bf.added += other.added
	return nil
}

func (bf *BloomFilter[T]) Reset() {
	clear(bf.bits)
	bf.added = 0
}

func (bf *BloomFilter[T]) bitIndex(v T, round int) int {
	return int(hashRound(bf.hasher, round, v) % uint64(bf.bitSize))
}

func (bf *BloomFilter[T]) setBit(index int) {
	word := index / 64
	offset := uint(index % 64)
	bf.bits[word] |= uint64(1) << offset
}

func (bf *BloomFilter[T]) hasBit(index int) bool {
	word := index / 64
	offset := uint(index % 64)
	return bf.bits[word]&(uint64(1)<<offset) != 0
}

func (s Stream[A]) CollectBloomFilter(hasher maphash.Hasher[A], bitSize, hashFuncs int) (*BloomFilter[A], error) {
	bf, err := NewBloomFilter(hasher, bitSize, hashFuncs)
	if err != nil {
		return nil, err
	}

	for v := range s.seq {
		bf.Add(v)
	}
	return bf, nil
}

func (s Stream[A]) CollectBloomFilterByError(hasher maphash.Hasher[A], expectedItems int, falsePositiveRate float64) (*BloomFilter[A], error) {
	bf, err := NewBloomFilterByError(hasher, expectedItems, falsePositiveRate)
	if err != nil {
		return nil, err
	}

	for v := range s.seq {
		bf.Add(v)
	}
	return bf, nil
}
