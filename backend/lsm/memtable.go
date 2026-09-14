package lsm

import (
	"sync"

	"github.com/google/btree"
)

type memEntry struct {
	Key     string
	Value   []byte
	Deleted bool
}

// Less implements btree.Item. The comparison is lexicographic in key.
func (e *memEntry) Less(than btree.Item) bool {
	return e.Key < than.(*memEntry).Key
}

// MemTable absorbs writes in sorted order in memory.
type Memtable struct {
	BTree *btree.BTree
	mu    sync.RWMutex
	size  int // len(Key) + len(Value);used for flush threshold
}

func NewMemtable() *Memtable {
	return &Memtable{
		BTree: btree.New(32),
	}
}
func (m *Memtable) Set(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Defensive copy — uvek non-nil, čak i za prazan value.
	cp := make([]byte, len(value))
	copy(cp, value)

	if old := m.BTree.Get(&memEntry{Key: key}); old != nil {
		oe := old.(*memEntry)
		m.size -= len(oe.Key) + len(oe.Value)
	}

	e := &memEntry{Key: key, Value: cp, Deleted: false}
	m.BTree.ReplaceOrInsert(e)
	m.size += len(e.Key) + len(e.Value)
}

func (m *Memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if old := m.BTree.Get(&memEntry{Key: key}); old != nil {
		oe := old.(*memEntry)
		m.size -= len(oe.Key) + len(oe.Value)
	}

	e := &memEntry{Key: key, Deleted: true}
	m.BTree.ReplaceOrInsert(e)
	m.size += len(e.Key)
}
func (m *Memtable) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item := m.BTree.Get(&memEntry{Key: key})
	if item == nil {
		return nil, false
	}
	e := item.(*memEntry)
	if e.Deleted {
		return nil, true // tombstone
	}
	return e.Value, true
}
func (m *Memtable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.BTree.Len()
}
func (m *Memtable) Flush() []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries := make([]Entry, 0, m.BTree.Len())
	m.BTree.Ascend(func(item btree.Item) bool {
		e := item.(*memEntry)
		entries = append(entries, Entry{
			key:     e.Key,
			value:   e.Value,
			deleted: e.Deleted,
		})
		return true
	})

	m.BTree.Clear(true)
	m.size = 0
	return entries
}

// Snapshot returns a sorted copy of all records without modifying the memtable.
// Used in LSM flush: first we record the state, then we write the SSTable,
// only then do we reset the memtable. If write fails, the memtable is intact.
func (m *Memtable) Snapshot() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]Entry, 0, m.BTree.Len())
	m.BTree.Ascend(func(item btree.Item) bool {
		e := item.(*memEntry)
		entries = append(entries, Entry{
			key:     e.Key,
			value:   e.Value,
			deleted: e.Deleted,
		})
		return true
	})
	return entries
}
