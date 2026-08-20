package lsm

import "sync"

type LSM struct {
	memtable *Memtable
	wal      *WAL
	sstables []*SSTable
	mu       sync.RWMutex
	dataDir  string
}

func NewLSM(dataDir string) (*LSM, error)
func (l *LSM) Set(key string, value []byte) error
func (l *LSM) Get(key string) ([]byte, error) {
	//1. Check the active MemTable. If the key is found, return it — this is the freshest data.

	//2. Check any frozen (immutable) MemTables waiting to be flushed.

	//3. Check SSTables from newest to oldest. For each file, consult the bloom filter first. If the bloom filter says “definitely not here,” skip the file entirely.

	// If it says “maybe here,” check the sparse index and read the relevant data block.
	//4. Return the first match found, since searching newest-to-oldest guarantees it is the most recent version.

	//5. If no match is found anywhere, the key does not exist.
	return nil, nil
}
func (l *LSM) Delete(key string) error
func (l *LSM) Close() error
