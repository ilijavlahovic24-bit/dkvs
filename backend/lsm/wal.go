package lsm

import (
	"os"
	"sync"
)

const (
	opPut    byte = 0
	opDelete byte = 1
)

type Entry struct {
	key     string
	value   []byte
	deleted bool
}

// WAL ensures durability before data reaches the MemTable.
type WAL struct {
	entries     []Entry
	path        string
	file        *os.File
	mu          sync.Mutex
	syncRequest chan chan struct{}
	done        chan struct{}
	loopDone    chan struct{}
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		path:        path,
		file:        f,
		syncRequest: make(chan chan struct{}, 1024),
		done:        make(chan struct{}),
		loopDone:    make(chan struct{}),
	}
	go w.syncLoop()
	return w, nil
}

func (w *WAL) syncLoop() {}

func (w *WAL) Append(key string, value []byte) error {
	Entry := Entry{key: key, value: value}
	w.entries = append(w.entries, Entry)
	return nil
}
func (w *WAL) Replay() ([]Entry, error) {
	// Implementation for replaying WAL
	return w.entries, nil
}
func (w *WAL) Clear() error {
	// Implementation for clearing WAL
	w.entries = nil
	return nil
}
