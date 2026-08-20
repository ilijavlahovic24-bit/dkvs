package lsm

// SSTables are immutable, sorted files flushed from the MemTable.

type DataBlock struct {
	//Data Blocks hold the actual key-value entries, sorted by key.
	// Each block is typically 4–64 KB and may be independently compressed (using LZ4, Snappy, or Zstd).
	//  Keys within a block often share prefixes, so prefix compression can further reduce size.
}
type IndexBlock struct {
	// The Index Block is a sparse index mapping the first key of each data block to its offset. To find a key,
	// you binary search the index to locate the right block, then scan within the block.
	// This means you read at most one data block per lookup — not the entire file.
}
type Footer struct {
	// The Footer stores metadata like the file’s key range (min and max key),
	// which is critical for compaction decisions and for quickly ruling out files during reads.
	min_key int64
	max_key int64
}

type SSTable struct {
	data_blocks  []DataBlock
	index_block  IndexBlock
	bloom_filter BloomFilter
	footer       Footer
}
