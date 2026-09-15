package lsm

import (
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
)

// Compaction merges SSTables, discards obsolete data, and controls amplification.
const compactThreshold = 4

type heapItem struct {
	key    string
	sstIdx int // index in sstables smaller=newer
	iter   *SSTIterator
}
type mergeHeap []heapItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	return h[i].sstIdx < h[j].sstIdx
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(heapItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
func (l *LSM) Compact() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.compactLocked()
}

// compactLocked merges all SSTables into one.
// Caller must hold l.mu.Lock().
// Order:
// 1. K-way merge of all SSTables into a sorted list of entries.
// 2. Write a new SSTable.
// 3. Replace l.sstables — only the new one remains.
// 4. Delete old files.
//
// If step 2 fails, the old SSTables are intact — no data loss.
func (l *LSM) compactLocked() error {
	if len(l.sstables) < 2 {
		return nil // nema šta da se spaja
	}

	// 1. K-way merge.
	merged, err := l.mergeAllLocked()
	if err != nil {
		return err
	}

	// Special case: all were tombstones.
	if len(merged) == 0 {
		return l.dropAllSSTablesLocked()
	}

	// Write a new SSTable
	newPath := filepath.Join(l.sstDir, fmt.Sprintf("%06d.sst", l.nextSST))
	newSST, err := WriteSSTable(newPath, merged)
	if err != nil {
		return fmt.Errorf("lsm: compaction write: %w", err)
	}

	// 4. Replace and delete old files.
	old := l.sstables
	l.sstables = []*SSTable{newSST}
	l.nextSST++

	for _, sst := range old {
		sst.Close()
		if err := os.Remove(sst.Path()); err != nil {
			return fmt.Errorf("lsm: compaction cleanup %s: %w", sst.Path(), err)
		}
	}
	return nil
}
func (l *LSM) mergeAllLocked() ([]Entry, error) {
	h := &mergeHeap{}

	for idx, sst := range l.sstables {
		it := sst.Iterator()
		if it.Next() {
			*h = append(*h, heapItem{
				key:    it.Key(),
				sstIdx: idx,
				iter:   it,
			})
		} else if err := it.Err(); err != nil {
			return nil, err
		}
	}
	heap.Init(h)

	var (
		merged   []Entry
		prevKey  string
		havePrev bool
	)

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)

		// Read the value before advance (the iterator is currently on item.key).
		key := item.key
		deleted := item.iter.Deleted()
		value := item.iter.Value()

		// Move the iterator and push it back to the heap if there are more entries.
		if item.iter.Next() {
			heap.Push(h, heapItem{
				key:    item.iter.Key(),
				sstIdx: item.sstIdx,
				iter:   item.iter,
			})
		} else if err := item.iter.Err(); err != nil {
			return nil, err
		}

		// Deduplication: If we've already broadcast this key, this is an older version.
		if havePrev && key == prevKey {
			continue
		}
		prevKey = key
		havePrev = true

		// Tombstone is discarded — we merge everything, there is no older level below.
		if deleted {
			continue
		}

		merged = append(merged, Entry{
			key:   key,
			value: value,
		})
	}

	return merged, nil
}

func (l *LSM) dropAllSSTablesLocked() error {
	old := l.sstables
	l.sstables = nil
	for _, sst := range old {
		sst.Close()
		if err := os.Remove(sst.Path()); err != nil {
			return err
		}
	}
	return nil
}
