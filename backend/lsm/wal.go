package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
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
	path        string
	file        *os.File
	mu          sync.Mutex
	syncRequest chan chan struct{}
	done        chan struct{}
	loopDone    chan struct{}
	closeOnce   sync.Once //so that it dosn't break if Close is called multiple times
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

func (w *WAL) syncLoop() {
	defer close(w.loopDone)

	for {
		select {
		case <-w.done:
			return
		case first := <-w.syncRequest:
			batch := []chan struct{}{first}

			// Drain: collects all writes that have arrived in the meantime.
		drain:
			for {
				select {
				case ch := <-w.syncRequest:
					batch = append(batch, ch)
				default:
					break drain
				}
			}

			// One fsync for entire batch.
			_ = w.file.Sync()

			// Broadcast: all writers from batch-a are now durable.
			for _, ch := range batch {
				close(ch)
			}
		}
	}
}

func (w *WAL) Append(key string, value []byte) error {
	return w.writeEntry(opPut, key, value)
}
func (w *WAL) writeEntry(op byte, key string, value []byte) error {
	buf := encodeEntry(op, key, value)
	w.mu.Lock()
	_, err := w.file.Write(buf)
	w.mu.Unlock()
	if err != nil {
		return err
	}
	ch := make(chan struct{})
	select {
	case w.syncRequest <- ch:
	case <-w.done:
		return errors.New("wal: closed")
	}
	select {
	case <-ch:
		return nil
	case <-w.done:
		return errors.New("wal: closed while waiting for sync")
	}
}
func (w *WAL) Replay() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []Entry
	r := bufio.NewReader(w.file)

	for {
		op, err := r.ReadByte()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return entries, err
		}

		var keyLenBuf [4]byte
		if _, err := io.ReadFull(r, keyLenBuf[:]); err != nil {
			break
		}
		keyLen := binary.BigEndian.Uint32(keyLenBuf[:])

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			break
		}

		var valLenBuf [8]byte
		if _, err := io.ReadFull(r, valLenBuf[:]); err != nil {
			break
		}
		valLen := binary.BigEndian.Uint64(valLenBuf[:])

		var value []byte
		if valLen > 0 {
			value = make([]byte, valLen)
			if _, err := io.ReadFull(r, value); err != nil {
				break
			}
		}

		entries = append(entries, Entry{
			key:     string(key),
			value:   value,
			deleted: op == opDelete,
		})
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return entries, nil
}
func (w *WAL) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.done)
		<-w.loopDone
		err = w.file.Close()
	})
	return err
}

// Format: [op:1][key_len:4][key][value_len:8][value]
func encodeEntry(op byte, key string, value []byte) []byte {
	buf := make([]byte, 0, 1+4+len(key)+8+len(value))
	buf = append(buf, op)

	var tmp [8]byte
	binary.BigEndian.PutUint32(tmp[:4], uint32(len(key)))
	buf = append(buf, tmp[:4]...)
	buf = append(buf, key...)

	binary.BigEndian.PutUint64(tmp[:8], uint64(len(value)))
	buf = append(buf, tmp[:8]...)
	buf = append(buf, value...)
	return buf
}
func (w *WAL) AppendDelete(key string) error {
	return w.writeEntry(opDelete, key, nil)
}
