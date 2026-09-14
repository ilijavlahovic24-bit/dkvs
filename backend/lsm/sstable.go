package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
)

// SSTables are immutable, sorted files flushed from the MemTable.
const (
	dataBlockTargetSize = 4 * 1024 // payload bytes pre nego što flush-ujemo blok
	sstableFooterSize   = 40       // 5 × uint64
	sstableBloomFPRate  = 0.01
)

type IndexBlock struct {
	// The Index Block is a sparse index mapping the first key of each data block to its offset. To find a key,
	// you binary search the index to locate the right block, then scan within the block.
	// This means you read at most one data block per lookup - not the entire file.
	Key      string
	BlockOff int64
	BlockLen int64
}
type Footer struct {
	// The Footer stores metadata like the file’s key range (min and max key),
	// which is critical for compaction decisions and for quickly ruling out files during reads.
	min_key int64
	max_key int64
}

type SSTable struct {
	path      string
	file      *os.File
	minKey    string
	maxKey    string
	index     []IndexBlock
	bloom     *BloomFilter
	numBlocks int
}

func WriteSSTable(path string, entries []Entry) (*SSTable, error) {
	if len(entries) == 0 {
		return nil, errors.New("sstable: cannot write empty table")
	}

	if err := writeSSTableFile(path, entries); err != nil {
		os.Remove(path)
		return nil, err
	}
	return OpenSSTable(path)
}

// writeSSTableFile writes all blocks, index, bloom and footer to the file,
// and fsyncs. The file handle is always closed before returning.
func writeSSTableFile(path string, entries []Entry) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	var offset int64
	var index []IndexBlock

	// 1. Data blocks.
	i := 0
	for i < len(entries) {
		firstKey := entries[i].key
		blockOff := offset

		payload := make([]byte, 4)
		count := 0
		for i < len(entries) && len(payload) < dataBlockTargetSize {
			payload = appendEntryToBuf(payload, entries[i])
			count++
			i++
		}
		binary.BigEndian.PutUint32(payload[0:4], uint32(count))

		var lenBuf [8]byte
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(payload)))
		if _, err := w.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := w.Write(payload); err != nil {
			return err
		}

		total := int64(8 + len(payload))
		index = append(index, IndexBlock{
			Key:      firstKey,
			BlockOff: blockOff,
			BlockLen: total,
		})
		offset += total
	}

	numBlocks := len(index)

	// 2. Index block.
	indexOffset := offset
	indexPayload := encodeIndex(entries[0].key, entries[len(entries)-1].key, index)
	{
		var lenBuf [8]byte
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(indexPayload)))
		if _, err := w.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := w.Write(indexPayload); err != nil {
			return err
		}
		offset += int64(8 + len(indexPayload))
	}
	indexLen := offset - indexOffset

	// 3. Bloom block.
	bf := NewBloomFilter(len(entries), sstableBloomFPRate)
	for _, e := range entries {
		bf.Add(e.key)
	}
	bloomData := bf.Encode()
	bloomOffset := offset
	{
		var lenBuf [8]byte
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(bloomData)))
		if _, err := w.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := w.Write(bloomData); err != nil {
			return err
		}
		offset += int64(8 + len(bloomData))
	}
	bloomLen := offset - bloomOffset

	// 4. Footer.
	var footer [sstableFooterSize]byte
	binary.BigEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.BigEndian.PutUint64(footer[8:16], uint64(indexLen))
	binary.BigEndian.PutUint64(footer[16:24], uint64(bloomOffset))
	binary.BigEndian.PutUint64(footer[24:32], uint64(bloomLen))
	binary.BigEndian.PutUint64(footer[32:40], uint64(numBlocks))
	if _, err := w.Write(footer[:]); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

func OpenSSTable(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			f.Close()
		}
	}()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < sstableFooterSize {
		return nil, errors.New("sstable: file too small")
	}

	var footer [sstableFooterSize]byte
	if _, err := f.ReadAt(footer[:], info.Size()-sstableFooterSize); err != nil {
		return nil, err
	}

	indexOffset := int64(binary.BigEndian.Uint64(footer[0:8]))
	indexLen := int64(binary.BigEndian.Uint64(footer[8:16]))
	bloomOffset := int64(binary.BigEndian.Uint64(footer[16:24]))
	bloomLen := int64(binary.BigEndian.Uint64(footer[24:32]))
	numBlocks := int(binary.BigEndian.Uint64(footer[32:40]))

	indexPayload := make([]byte, indexLen-8)
	if _, err := f.ReadAt(indexPayload, indexOffset+8); err != nil {
		return nil, err
	}
	minKey, maxKey, index, err := decodeIndex(indexPayload)
	if err != nil {
		return nil, err
	}

	bloomData := make([]byte, bloomLen-8)
	if _, err := f.ReadAt(bloomData, bloomOffset+8); err != nil {
		return nil, err
	}
	bf, err := DecodeBloomFilter(bloomData)
	if err != nil {
		return nil, err
	}

	success = true
	return &SSTable{
		path:      path,
		file:      f,
		minKey:    minKey,
		maxKey:    maxKey,
		index:     index,
		bloom:     bf,
		numBlocks: numBlocks,
	}, nil
}

func (s *SSTable) Get(key string) ([]byte, bool, error) {
	if key < s.minKey || key > s.maxKey {
		return nil, false, nil
	}
	if !s.bloom.MayContain(key) {
		return nil, false, nil
	}

	// Binary search: nađi poslednji index entry sa Key ≤ target.
	idx := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].Key > key
	}) - 1
	if idx < 0 {
		return nil, false, nil
	}

	entries, err := s.readBlock(s.index[idx])
	if err != nil {
		return nil, false, err
	}

	for _, e := range entries {
		if e.key == key {
			if e.deleted {
				return nil, true, nil
			}
			return e.value, true, nil
		}
		if e.key > key {
			break
		}
	}
	return nil, false, nil
}

func (s *SSTable) readBlock(ie IndexBlock) ([]Entry, error) {
	payload := make([]byte, ie.BlockLen-8)
	if _, err := s.file.ReadAt(payload, ie.BlockOff+8); err != nil {
		return nil, err
	}
	return decodeDataBlock(payload)
}

type SSTIterator struct {
	sst      *SSTable
	blockIdx int
	entries  []Entry
	entryIdx int
	loaded   bool
	err      error
}

func (s *SSTable) Iterator() *SSTIterator {
	return &SSTIterator{sst: s, blockIdx: 0, entryIdx: -1}
}

func (it *SSTIterator) Next() bool {
	if it.err != nil {
		return false
	}
	for {
		if !it.loaded {
			if it.blockIdx >= len(it.sst.index) {
				return false
			}
			entries, err := it.sst.readBlock(it.sst.index[it.blockIdx])
			if err != nil {
				it.err = err
				return false
			}
			it.entries = entries
			it.entryIdx = -1
			it.loaded = true
		}
		it.entryIdx++
		if it.entryIdx < len(it.entries) {
			return true
		}
		it.loaded = false
		it.blockIdx++
	}
}

func (it *SSTIterator) Key() string   { return it.entries[it.entryIdx].key }
func (it *SSTIterator) Value() []byte { return it.entries[it.entryIdx].value }
func (it *SSTIterator) Deleted() bool { return it.entries[it.entryIdx].deleted }
func (it *SSTIterator) Err() error    { return it.err }

// ============================================================
// Lifecycle / metadata
// ============================================================

func (s *SSTable) Close() error {
	return s.file.Close()
}

func (s *SSTable) Remove() error {
	if s.file != nil {
		s.file.Close()
	}
	return os.Remove(s.path)
}

func (s *SSTable) MinKey() string { return s.minKey }
func (s *SSTable) MaxKey() string { return s.maxKey }
func (s *SSTable) Path() string   { return s.path }
func (s *SSTable) NumBlocks() int { return s.numBlocks }

// ============================================================
// Encoding / decoding helpers
// ============================================================

func encodeIndex(minKey, maxKey string, entries []IndexBlock) []byte {
	var buf []byte
	buf = appendUint32String(buf, minKey)
	buf = appendUint32String(buf, maxKey)
	buf = appendUint32(buf, uint32(len(entries)))
	for _, e := range entries {
		buf = appendUint32String(buf, e.Key)
		buf = appendUint64(buf, uint64(e.BlockOff))
		buf = appendUint64(buf, uint64(e.BlockLen))
	}
	return buf
}

func decodeIndex(data []byte) (minKey, maxKey string, entries []IndexBlock, err error) {
	pos := 0
	if minKey, pos, err = readUint32String(data, pos); err != nil {
		return
	}
	if maxKey, pos, err = readUint32String(data, pos); err != nil {
		return
	}
	var count uint32
	if count, pos, err = readUint32(data, pos); err != nil {
		return
	}
	entries = make([]IndexBlock, 0, count)
	for i := uint32(0); i < count; i++ {
		var e IndexBlock
		if e.Key, pos, err = readUint32String(data, pos); err != nil {
			return
		}
		var v uint64
		if v, pos, err = readUint64(data, pos); err != nil {
			return
		}
		e.BlockOff = int64(v)
		if v, pos, err = readUint64(data, pos); err != nil {
			return
		}
		e.BlockLen = int64(v)
		entries = append(entries, e)
	}
	return
}

func appendEntryToBuf(buf []byte, e Entry) []byte {
	op := opPut
	if e.deleted {
		op = opDelete
	}
	buf = append(buf, op)
	buf = appendUint32String(buf, e.key)
	buf = appendUint64(buf, uint64(len(e.value)))
	return append(buf, e.value...)
}

func decodeDataBlock(data []byte) ([]Entry, error) {
	count, pos, err := readUint32(data, 0)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, count)
	for i := uint32(0); i < count; i++ {
		if pos >= len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		op := data[pos]
		pos++

		key, p, err := readUint32String(data, pos)
		if err != nil {
			return nil, err
		}
		pos = p

		valLen, p, err := readUint64(data, pos)
		if err != nil {
			return nil, err
		}
		pos = p

		var value []byte
		if valLen > 0 {
			if pos+int(valLen) > len(data) {
				return nil, io.ErrUnexpectedEOF
			}
			value = make([]byte, valLen)
			copy(value, data[pos:pos+int(valLen)])
			pos += int(valLen)
		} else if op == opPut {
			value = []byte{} // prazna vrednost (non-nil)
		}
		// op == opDelete → value ostaje nil (tombstone)

		entries = append(entries, Entry{
			key:     key,
			value:   value,
			deleted: op == opDelete,
		})
	}
	return entries, nil
}

func appendUint32(buf []byte, v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return append(buf, b[:]...)
}

func appendUint64(buf []byte, v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return append(buf, b[:]...)
}

func appendUint32String(buf []byte, s string) []byte {
	buf = appendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

func readUint32(data []byte, pos int) (uint32, int, error) {
	if pos+4 > len(data) {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return binary.BigEndian.Uint32(data[pos : pos+4]), pos + 4, nil
}

func readUint64(data []byte, pos int) (uint64, int, error) {
	if pos+8 > len(data) {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return binary.BigEndian.Uint64(data[pos : pos+8]), pos + 8, nil
}

func readUint32String(data []byte, pos int) (string, int, error) {
	n, pos, err := readUint32(data, pos)
	if err != nil {
		return "", 0, err
	}
	if pos+int(n) > len(data) {
		return "", 0, io.ErrUnexpectedEOF
	}
	return string(data[pos : pos+int(n)]), pos + int(n), nil
}
