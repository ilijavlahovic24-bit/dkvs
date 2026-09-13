package lsm

import (
	"encoding/binary"
	"errors"
	"math"
)

//Bloom filters eliminate most unnecessary disk reads.

// The Bloom Filter is a compact probabilistic structure that answers the question: “Could this key exist in this file?”
type BloomFilter struct {
	bits    []uint64 // pakovan bit array
	numBits uint64   // m
	numHash uint64   // k
}

// Expected values: expectedKeys=10000, falsePositiveRate=0.01 (1%).
// Formula:
//
//	m = -n * ln(p) / (ln(2))^2
//	k = (m/n) * ln(2)
func NewBloomFilter(expectedKeys int, falsePositiveRate float64) *BloomFilter {
	if expectedKeys <= 0 {
		expectedKeys = 1
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}

	m := uint64(math.Ceil(
		-float64(expectedKeys) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2),
	))
	if m < 64 {
		m = 64
	}

	k := uint64(math.Round(float64(m) / float64(expectedKeys) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30 // sanity check to avoid too many hash functions
	}

	return &BloomFilter{
		bits:    make([]uint64, (m+63)/64),
		numBits: m,
		numHash: k,
	}
}

func hashA(s string) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}
func hashB(s string) uint64 {
	h := uint64(5381)
	for i := 0; i < len(s); i++ {
		h = (h << 5) + h + uint64(s[i])
		h ^= h >> 32
	}
	return h
}
func (b *BloomFilter) Add(key string) {
	h1 := hashA(key)
	h2 := hashB(key)
	if h2 == 0 {
		h2 = 1 // Avoid zero to ensure we get k distinct hash values
	}

	for i := uint64(0); i < b.numHash; i++ {
		bit := (h1 + i*h2) % b.numBits
		b.bits[bit/64] |= 1 << (bit % 64)
	}
}
func (b *BloomFilter) MayContain(key string) bool {
	h1 := hashA(key)
	h2 := hashB(key)
	if h2 == 0 {
		h2 = 1
	}

	for i := uint64(0); i < b.numHash; i++ {
		bit := (h1 + i*h2) % b.numBits
		if b.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// Format: [numBits:8][numHash:8][bits...]
func (b *BloomFilter) Encode() []byte {
	buf := make([]byte, 16+len(b.bits)*8)
	binary.BigEndian.PutUint64(buf[0:8], b.numBits)
	binary.BigEndian.PutUint64(buf[8:16], b.numHash)
	for i, w := range b.bits {
		binary.BigEndian.PutUint64(buf[16+i*8:], w)
	}
	return buf
}

// DecodeBloomFilter decodes a BloomFilter from a byte slice. It expects the format produced by Encode.
func DecodeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 16 {
		return nil, errors.New("bloom: data too short")
	}
	numBits := binary.BigEndian.Uint64(data[0:8])
	numHash := binary.BigEndian.Uint64(data[8:16])

	numWords := (numBits + 63) / 64
	if uint64(len(data)-16) < numWords*8 {
		return nil, errors.New("bloom: truncated bits array")
	}

	bits := make([]uint64, numWords)
	for i := range bits {
		bits[i] = binary.BigEndian.Uint64(data[16+i*8:])
	}
	return &BloomFilter{
		bits:    bits,
		numBits: numBits,
		numHash: numHash,
	}, nil
}
