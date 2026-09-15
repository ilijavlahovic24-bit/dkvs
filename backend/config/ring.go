package config

import (
	"fmt"
	"sort"
)

// DefaultVnodes is the number of virtual nodes per physical shard.
// 150 is the standard value (Karger et al.) — enough for even
// distribution without excessive memory.
const DefaultVnodes = 150

// hashKey is FNV-1a 64-bit. Fast, allocation-free, good distribution.
func hashKey(s string) uint64 {
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

// Ring is a consistent hashing ring with virtual nodes.
//
// Advantage over hash(key) % N:
// adding/removing a shard moves only ~1/N of keys,
// while the modulo approach requires redistributing all of them.
type Ring struct {
	vnodes      int
	hashes      []uint64       // sorted hashes of all vnodes
	hashToShard map[uint64]int // vnode hash → physical shard index
	addrs       map[int]string // shard index → address
}

// NewRing builds a ring from the given shards.
//
// addrs: shard index → address (e.g. "127.0.0.1:8080").
// vnodes <= 0 → uses DefaultVnodes.
func NewRing(addrs map[int]string, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVnodes
	}
	r := &Ring{
		vnodes:      vnodes,
		hashToShard: make(map[uint64]int, len(addrs)*vnodes),
		addrs:       addrs,
	}

	// Deterministic order by index — all processes build the same ring.
	indices := make([]int, 0, len(addrs))
	for i := range addrs {
		indices = append(indices, i)
	}
	sort.Ints(indices)

	for _, idx := range indices {
		for v := 0; v < vnodes; v++ {
			h := hashKey(fmt.Sprintf("%d-%d", idx, v))
			r.hashToShard[h] = idx
			r.hashes = append(r.hashes, h)
		}
	}

	sort.Slice(r.hashes, func(i, j int) bool {
		return r.hashes[i] < r.hashes[j]
	})
	return r
}

// GetShardIdx returns the shard index for the given key.
//
// Algorithm:
//  1. hash the key
//  2. binary search for the first vnode hash >= h
//  3. if none exists (hash is greater than all), wrap to the start of the ring
func (r *Ring) GetShardIdx(key string) int {
	if len(r.hashes) == 0 {
		return 0
	}
	h := hashKey(key)
	i := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= h
	})
	if i == len(r.hashes) {
		i = 0 // wrap-around
	}
	return r.hashToShard[r.hashes[i]]
}

// GetShard returns the address of the shard for the given key.
func (r *Ring) GetShard(key string) string {
	return r.addrs[r.GetShardIdx(key)]
}
