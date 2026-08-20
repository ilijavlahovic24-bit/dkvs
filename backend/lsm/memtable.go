package lsm

import "github.com/google/btree"

//MemTable absorbs writes in sorted order in memory.
type Memtable struct {
	BTree *btree.BTree
}
