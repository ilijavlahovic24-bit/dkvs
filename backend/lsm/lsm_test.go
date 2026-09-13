package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// newTestWAL otvara WAL u temp direktorijumu i registruje cleanup.
func newTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, path
}

// --- Osnovni testovi ---

func TestWALAppendAndReplay(t *testing.T) {
	w, _ := newTestWAL(t)

	if err := w.Append("foo", []byte("bar")); err != nil {
		t.Fatalf("Append foo: %v", err)
	}
	if err := w.Append("hello", []byte("world")); err != nil {
		t.Fatalf("Append hello: %v", err)
	}

	got, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries, got %d", len(got))
	}
	if got[0].key != "foo" || !bytes.Equal(got[0].value, []byte("bar")) || got[0].deleted {
		t.Errorf("entry 0 mismatch: %+v", got[0])
	}
	if got[1].key != "hello" || !bytes.Equal(got[1].value, []byte("world")) || got[1].deleted {
		t.Errorf("entry 1 mismatch: %+v", got[1])
	}
}

func TestWALEmptyValue(t *testing.T) {
	w, _ := newTestWAL(t)

	if err := w.Append("empty", []byte{}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1, got %d", len(got))
	}
	if got[0].key != "empty" || len(got[0].value) != 0 {
		t.Errorf("mismatch: %+v", got[0])
	}
}

func TestWALDelete(t *testing.T) {
	w, _ := newTestWAL(t)

	if err := w.Append("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendDelete("k"); err != nil {
		t.Fatal(err)
	}

	got, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2, got %d", len(got))
	}
	if got[0].deleted {
		t.Errorf("first entry should be PUT")
	}
	if !got[1].deleted {
		t.Errorf("second entry should be tombstone")
	}
	if got[1].key != "k" || len(got[1].value) != 0 {
		t.Errorf("tombstone mismatch: %+v", got[1])
	}
}

func TestWALOverwrite(t *testing.T) {
	w, _ := newTestWAL(t)

	w.Append("k", []byte("v1"))
	w.Append("k", []byte("v2"))
	w.Append("k", []byte("v3"))

	got, _ := w.Replay()
	if len(got) != 3 {
		t.Fatalf("want 3, got %d", len(got))
	}
	// WAL ne deduplicira — sva tri zapisa moraju biti tu.
	// Deduplikacija je posao memtable-a / SSTable-a.
	if string(got[2].value) != "v3" {
		t.Errorf("last value should be v3, got %q", got[2].value)
	}
}

// --- Clear ---

func TestWALClear(t *testing.T) {
	w, _ := newTestWAL(t)
	w.Append("a", []byte("1"))
	w.Append("b", []byte("2"))

	if err := w.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	got, _ := w.Replay()
	if len(got) != 0 {
		t.Fatalf("after Clear want 0, got %d", len(got))
	}
}

func TestWALAppendAfterClear(t *testing.T) {
	w, _ := newTestWAL(t)
	w.Append("a", []byte("1"))
	w.Clear()

	if err := w.Append("b", []byte("2")); err != nil {
		t.Fatalf("Append after Clear: %v", err)
	}

	got, _ := w.Replay()
	if len(got) != 1 || got[0].key != "b" {
		t.Fatalf("want [b], got %+v", got)
	}
}

// --- Truncated tail (simulacija pada tokom write-a) ---

func TestWALTruncatedTailIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trunc.wal")

	w, _ := NewWAL(path)
	w.Append("good", []byte("value"))
	w.Close()

	// Ručno dodaj nekompletan zapis.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// opPut + key_len=10 (samo 4 bajta header-a), fali key.
	f.Write([]byte{opPut, 0, 0, 0, 10})
	f.Close()

	w2, _ := NewWAL(path)
	defer w2.Close()

	got, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay should not error on truncated tail: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 good entry, got %d", len(got))
	}
	if got[0].key != "good" {
		t.Errorf("got %q", got[0].key)
	}
}

func TestWALTruncatedInMiddleOfValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trunc2.wal")

	w, _ := NewWAL(path)
	w.Append("k1", []byte("complete"))
	w.Append("k2", []byte("partial"))
	w.Close()

	// Skrati fajl tako da drugi zapis bude polovičan.
	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	w2, _ := NewWAL(path)
	defer w2.Close()

	got, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 (partial dropped), got %d", len(got))
	}
	if got[0].key != "k1" {
		t.Errorf("want k1, got %q", got[0].key)
	}
}

// --- Persistence ---

func TestWALPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.wal")

	w, _ := NewWAL(path)
	w.Append("persist", []byte("me"))
	w.Append("also", []byte("this"))
	w.Close()

	w2, _ := NewWAL(path)
	defer w2.Close()

	got, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2, got %d", len(got))
	}
	if got[0].key != "persist" || got[1].key != "also" {
		t.Errorf("keys mismatch: %+v", got)
	}
}

// --- Group commit ---

func TestWALGroupCommitSingleWriter(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(filepath.Join(dir, "g1.wal"))
	defer w.Close()

	const N = 100
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := w.Append(key, []byte("v")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	got, _ := w.Replay()
	if len(got) != N {
		t.Fatalf("want %d, got %d", N, len(got))
	}
	// Redosled mora biti očuvan.
	for i, e := range got {
		want := fmt.Sprintf("key-%d", i)
		if e.key != want {
			t.Fatalf("entry %d: want %q, got %q", i, want, e.key)
		}
	}
}

func TestWALGroupCommitConcurrent(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(filepath.Join(dir, "gc.wal"))
	defer w.Close()

	const writers = 100
	var wg sync.WaitGroup
	wg.Add(writers)

	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start // start-barrier: svi krenu u isto vreme
			key := fmt.Sprintf("k-%d", i)
			if err := w.Append(key, []byte("v")); err != nil {
				t.Errorf("Append %d: %v", i, err)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	got, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != writers {
		t.Fatalf("want %d entries, got %d", writers, len(got))
	}
}

func TestWALConcurrentMixedOps(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(filepath.Join(dir, "mixed.wal"))
	defer w.Close()

	const perGoroutine = 50
	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := fmt.Sprintf("g%d-k%d", g, i)
				if i%2 == 0 {
					w.Append(key, []byte("v"))
				} else {
					w.AppendDelete(key)
				}
			}
		}(g)
	}
	wg.Wait()

	got, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != perGoroutine*goroutines {
		t.Fatalf("want %d, got %d", perGoroutine*goroutines, len(got))
	}
}

// --- Close ---

func TestWALCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(filepath.Join(dir, "close.wal"))
	w.Append("k", []byte("v"))

	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close should be no-op, got: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("third Close should be no-op, got: %v", err)
	}
}

func TestWALAppendAfterCloseFails(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(filepath.Join(dir, "afterclose.wal"))
	w.Close()

	err := w.Append("k", []byte("v"))
	if err == nil {
		t.Fatal("Append after Close should return error")
	}
}

// --- Replay je idempotentan ---

func TestWALReplayTwice(t *testing.T) {
	w, _ := newTestWAL(t)
	w.Append("a", []byte("1"))
	w.Append("b", []byte("2"))

	got1, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	got2, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got1) != len(got2) || len(got1) != 2 {
		t.Fatalf("replay mismatch: %d vs %d", len(got1), len(got2))
	}
}

// --- Benchmark ---

func BenchmarkWALAppend(b *testing.B) {
	dir := b.TempDir()
	w, err := NewWAL(filepath.Join(dir, "bench.wal"))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Append("k", []byte("v"))
	}
}

func BenchmarkWALAppendParallel(b *testing.B) {
	dir := b.TempDir()
	w, err := NewWAL(filepath.Join(dir, "benchp.wal"))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w.Append("k", []byte("v"))
		}
	})
}
