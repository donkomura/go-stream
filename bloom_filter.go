package main

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"iter"
	"math"
)

var (
	errInvalidBitSize            = errors.New("bitSize must be > 0")
	errInvalidHashFuncs          = errors.New("hashFuncs must be > 0")
	errInvalidExpectedItems      = errors.New("expectedItems must be > 0")
	errInvalidFalsePositiveRate  = errors.New("falsePositiveRate must be in (0, 1)")
	errNilBloomFilter            = errors.New("bloom filter is nil")
	errIncompatibleBloomFilter   = errors.New("bloom filters are incompatible")
)

// BloomFilter is a probabilistic set for membership tests.
// It can return false positives but never false negatives.
type BloomFilter struct {
	bitSize   int
	hashFuncs int
	bits      []uint64
	added     uint64
}

type BloomFilterResult struct {
	Filter *BloomFilter
	Err    error
}

func NewBloomFilter(bitSize, hashFuncs int) (*BloomFilter, error) {
	if bitSize <= 0 {
		return nil, errInvalidBitSize
	}
	if hashFuncs <= 0 {
		return nil, errInvalidHashFuncs
	}

	wordCount := (bitSize + 63) / 64
	return &BloomFilter{
		bitSize:   bitSize,
		hashFuncs: hashFuncs,
		bits:      make([]uint64, wordCount),
	}, nil
}

// NewBloomFilterByError calculates parameters from capacity and false positive rate.
func NewBloomFilterByError(expectedItems int, falsePositiveRate float64) (*BloomFilter, error) {
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
	k := int(math.Ceil((float64(m) / n) * ln2))
	if k <= 0 {
		k = 1
	}

	return NewBloomFilter(m, k)
}

func (bf *BloomFilter) BitSize() int {
	return bf.bitSize
}

func (bf *BloomFilter) HashFuncs() int {
	return bf.hashFuncs
}

func (bf *BloomFilter) AddedCount() uint64 {
	return bf.added
}

func (bf *BloomFilter) AddString(key string) {
	bf.AddBytes([]byte(key))
}

func (bf *BloomFilter) AddBytes(key []byte) {
	for i := 0; i < bf.hashFuncs; i++ {
		idx := bf.hashIndex(key, i)
		bf.setBit(idx)
	}
	bf.added++
}

func (bf *BloomFilter) TestString(key string) bool {
	return bf.TestBytes([]byte(key))
}

func (bf *BloomFilter) TestBytes(key []byte) bool {
	for i := 0; i < bf.hashFuncs; i++ {
		idx := bf.hashIndex(key, i)
		if !bf.hasBit(idx) {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Merge(other *BloomFilter) error {
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

func (bf *BloomFilter) Reset() {
	clear(bf.bits)
	bf.added = 0
}

func (bf *BloomFilter) hashIndex(key []byte, hashRound int) int {
	var prefix [8]byte
	binary.LittleEndian.PutUint64(prefix[:], uint64(hashRound))

	h := fnv.New64a()
	_, _ = h.Write(prefix[:])
	_, _ = h.Write(key)
	return int(h.Sum64() % uint64(bf.bitSize))
}

func (bf *BloomFilter) setBit(index int) {
	word := index / 64
	offset := uint(index % 64)
	bf.bits[word] |= uint64(1) << offset
}

func (bf *BloomFilter) hasBit(index int) bool {
	word := index / 64
	offset := uint(index % 64)
	return bf.bits[word]&(uint64(1)<<offset) != 0
}

func BloomFilterCollect[A any](bitSize, hashFuncs int, keyFn func(A) string) func(iter.Seq[A]) BloomFilterResult {
	return func(seq iter.Seq[A]) BloomFilterResult {
		bf, err := NewBloomFilter(bitSize, hashFuncs)
		if err != nil {
			return BloomFilterResult{Err: err}
		}

		for v := range seq {
			bf.AddString(keyFn(v))
		}
		return BloomFilterResult{Filter: bf}
	}
}

func BloomFilterCollectByError[A any](expectedItems int, falsePositiveRate float64, keyFn func(A) string) func(iter.Seq[A]) BloomFilterResult {
	return func(seq iter.Seq[A]) BloomFilterResult {
		bf, err := NewBloomFilterByError(expectedItems, falsePositiveRate)
		if err != nil {
			return BloomFilterResult{Err: err}
		}

		for v := range seq {
			bf.AddString(keyFn(v))
		}
		return BloomFilterResult{Filter: bf}
	}
}
