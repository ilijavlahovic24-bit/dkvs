package lsm

type Entry struct {
	key   string
	value []byte
}

// WAL ensures durability before data reaches the MemTable.
type WAL struct {
	entries []Entry
}

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
