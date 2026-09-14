package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

var ErrNotFound = errors.New("lsm: key not found")

const (
	defaultFlushThreshold = 4 * 1024 * 1024 // 4MB
	walFileName           = "wal.log"
	sstableSubdir         = "sstables"
)

type LSM struct {
	memtable       *Memtable
	wal            *WAL
	sstables       []*SSTable
	mu             sync.RWMutex
	dataDir        string
	sstDir         string
	nextSST        int
	flushThreshold int
	closed         bool
}

func NewLSM(dataDir string) (*LSM, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	sstDir := filepath.Join(dataDir, sstableSubdir)
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		return nil, err
	}

	wal, err := NewWAL(filepath.Join(dataDir, walFileName))
	if err != nil {
		return nil, err
	}

	l := &LSM{
		memtable:       NewMemtable(),
		wal:            wal,
		dataDir:        dataDir,
		sstDir:         sstDir,
		flushThreshold: defaultFlushThreshold,
	}

	if err := l.loadSSTables(); err != nil {
		wal.Close()
		return nil, err
	}

	if err := l.recoverFromWAL(); err != nil {
		l.closeSSTables()
		wal.Close()
		return nil, err
	}

	return l, nil
}
func (l *LSM) loadSSTables() error {
	dirEntries, err := os.ReadDir(l.sstDir)
	if err != nil {
		return err
	}

	var names []string
	for _, e := range dirEntries {
		if strings.HasSuffix(e.Name(), ".sst") {
			names = append(names, e.Name())
		}
	}
	// Names are zero-padded (000000.sst), so the sorting is lexicographic
	// equivalent to numeric for up to 999999 files.
	sort.Sort(sort.Reverse(sort.StringSlice(names)))

	for _, name := range names {
		path := filepath.Join(l.sstDir, name)
		sst, err := OpenSSTable(path)
		if err != nil {
			l.closeSSTables()
			return fmt.Errorf("sstable: opening %s: %w", path, err)
		}
		l.sstables = append(l.sstables, sst)

		var seq int
		if _, err := fmt.Sscanf(name, "%06d.sst", &seq); err == nil {
			if seq >= l.nextSST {
				l.nextSST = seq + 1
			}
		}
	}
	return nil
}
func (l *LSM) recoverFromWAL() error {
	entries, err := l.wal.Replay()
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.deleted {
			l.memtable.Delete(e.key)
		} else {
			l.memtable.Set(e.key, e.value)
		}
	}
	return nil
}
func (l *LSM) Set(key string, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return errors.New("lsm: closed")
	}

	if err := l.wal.Append(key, value); err != nil {
		return err
	}
	l.memtable.Set(key, value)

	if l.memtable.Size() >= l.flushThreshold {
		return l.flushLocked()
	}
	return nil
}
func (l *LSM) Get(key string) ([]byte, error) {
	//1. Check the active MemTable. If the key is found, return it — this is the freshest data.
	//2. Check any frozen (immutable) MemTables waiting to be flushed.
	//3. Check SSTables from newest to oldest. For each file, consult the bloom filter first. If the bloom filter says “definitely not here,” skip the file entirely.
	// If it says “maybe here,” check the sparse index and read the relevant data block.
	//4. Return the first match found, since searching newest-to-oldest guarantees it is the most recent version.
	//5. If no match is found anywhere, the key does not exist.

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, errors.New("lsm: closed")
	}

	if v, found := l.memtable.Get(key); found {
		if v == nil {
			return nil, ErrNotFound // tombstone
		}
		return v, nil
	}

	for _, sst := range l.sstables {
		v, found, err := sst.Get(key)
		if err != nil {
			return nil, err
		}
		if found {
			if v == nil {
				return nil, ErrNotFound // tombstone
			}
			return v, nil
		}
	}

	return nil, ErrNotFound
}
func (l *LSM) Delete(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return errors.New("lsm: closed")
	}

	if err := l.wal.AppendDelete(key); err != nil {
		return err
	}
	l.memtable.Delete(key)

	if l.memtable.Size() >= l.flushThreshold {
		return l.flushLocked()
	}
	return nil
}

func (l *LSM) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true

	var firstErr error
	if err := l.closeSSTables(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := l.wal.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (l *LSM) closeSSTables() error {
	var firstErr error
	for _, sst := range l.sstables {
		if err := sst.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
func (l *LSM) flushLocked() error {
	if l.memtable.Len() == 0 {
		return nil
	}

	entries := l.memtable.Snapshot()
	path := filepath.Join(l.sstDir, fmt.Sprintf("%06d.sst", l.nextSST))

	sst, err := WriteSSTable(path, entries)
	if err != nil {
		return fmt.Errorf("lsm: flush: %w", err)
	}

	// Tek sada je bezbedno resetovati memtable.
	l.memtable.Flush() // briše sadržaj, ignorišemo return

	// Novi SSTable ide na početak (najnoviji prvi).
	l.sstables = append([]*SSTable{sst}, l.sstables...)
	l.nextSST++

	// Podaci su sada durable u SSTable-u — WAL može da se obriše.
	return l.wal.Clear()
}

// SetFlushThreshold changes the flush threshold (useful in tests).
// Value must be > 0.
func (l *LSM) SetFlushThreshold(bytes int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if bytes > 0 {
		l.flushThreshold = bytes
	}
}
