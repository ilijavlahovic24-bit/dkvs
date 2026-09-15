package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// --- WAL Tests ---//

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

// --- Memtable Tests ---//

func TestMemtableSetGet(t *testing.T) {
	m := NewMemtable()

	m.Set("foo", []byte("bar"))
	m.Set("hello", []byte("world"))

	v, ok := m.Get("foo")
	if !ok || string(v) != "bar" {
		t.Fatalf("Get(foo) = %q, %v", v, ok)
	}
	v, ok = m.Get("hello")
	if !ok || string(v) != "world" {
		t.Fatalf("Get(hello) = %q, %v", v, ok)
	}
}

func TestMemtableGetMissing(t *testing.T) {
	m := NewMemtable()
	v, ok := m.Get("nope")
	if ok {
		t.Fatalf("expected not found, got %q", v)
	}
}

func TestMemtableEmptyValue(t *testing.T) {
	m := NewMemtable()
	m.Set("empty", []byte{})

	v, ok := m.Get("empty")
	if !ok {
		t.Fatal("expected found")
	}
	if v == nil {
		t.Fatal("empty value should be non-nil slice, not nil (that's tombstone)")
	}
	if len(v) != 0 {
		t.Fatalf("want empty, got %q", v)
	}
}

func TestMemtableNilValueNormalized(t *testing.T) {
	m := NewMemtable()
	m.Set("k", nil) // nil se normalizuje u []byte{}

	v, ok := m.Get("k")
	if !ok {
		t.Fatal("expected found")
	}
	if v == nil {
		t.Fatal("nil value must be normalized, otherwise it looks like tombstone")
	}
}

func TestMemtableOverwrite(t *testing.T) {
	m := NewMemtable()
	m.Set("k", []byte("v1"))
	m.Set("k", []byte("v2"))

	v, ok := m.Get("k")
	if !ok || string(v) != "v2" {
		t.Fatalf("want v2, got %q (ok=%v)", v, ok)
	}
	if m.Len() != 1 {
		t.Fatalf("overwrite should not grow Len: %d", m.Len())
	}
}

func TestMemtableDelete(t *testing.T) {
	m := NewMemtable()
	m.Set("k", []byte("v"))
	m.Delete("k")

	v, ok := m.Get("k")
	if !ok {
		t.Fatal("tombstone should be 'found' (ok=true)")
	}
	if v != nil {
		t.Fatalf("tombstone should return nil value, got %q", v)
	}
	if m.Len() != 1 {
		t.Fatalf("tombstone should stay in memtable: %d", m.Len())
	}
}

func TestMemtableDeleteMissing(t *testing.T) {
	m := NewMemtable()
	m.Delete("nope") // mora da upiše tombstone čak i ako ključ ne postoji

	v, ok := m.Get("nope")
	if !ok || v != nil {
		t.Fatalf("want tombstone, got %q (ok=%v)", v, ok)
	}
}

func TestMemtableResurrectAfterDelete(t *testing.T) {
	m := NewMemtable()
	m.Set("k", []byte("v1"))
	m.Delete("k")
	m.Set("k", []byte("v2"))

	v, ok := m.Get("k")
	if !ok || string(v) != "v2" {
		t.Fatalf("want v2 after resurrect, got %q (ok=%v)", v, ok)
	}
}

func TestMemtableSize(t *testing.T) {
	m := NewMemtable()
	if m.Size() != 0 {
		t.Fatalf("empty memtable should have size 0, got %d", m.Size())
	}

	m.Set("abc", []byte("de")) // 3 + 2 = 5
	if m.Size() != 5 {
		t.Fatalf("want 5, got %d", m.Size())
	}

	m.Set("abc", []byte("defg")) // overwrite: 3 + 4 = 7
	if m.Size() != 7 {
		t.Fatalf("after overwrite want 7, got %d", m.Size())
	}

	m.Delete("abc") // 3 + 0 = 3
	if m.Size() != 3 {
		t.Fatalf("after delete want 3, got %d", m.Size())
	}
}

func TestMemtableFlush(t *testing.T) {
	m := NewMemtable()
	m.Set("banana", []byte("yellow"))
	m.Set("apple", []byte("red"))
	m.Set("cherry", []byte("dark"))

	entries := m.Flush()
	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(entries))
	}

	// Sortirano po ključu.
	want := []string{"apple", "banana", "cherry"}
	for i, e := range entries {
		if e.key != want[i] {
			t.Fatalf("entry %d: want %q, got %q", i, want[i], e.key)
		}
	}

	// Flush resetuje memtable.
	if m.Len() != 0 || m.Size() != 0 {
		t.Fatalf("after Flush: Len=%d Size=%d", m.Len(), m.Size())
	}
}

func TestMemtableFlushEmpty(t *testing.T) {
	m := NewMemtable()
	entries := m.Flush()
	if len(entries) != 0 {
		t.Fatalf("want 0, got %d", len(entries))
	}
}

func TestMemtableFlushPreservesTombstone(t *testing.T) {
	m := NewMemtable()
	m.Set("alive", []byte("v"))
	m.Set("dead", []byte("v"))
	m.Delete("dead")

	entries := m.Flush()
	if len(entries) != 2 {
		t.Fatalf("want 2, got %d", len(entries))
	}
	// Sortirano: "alive", "dead"
	if entries[0].key != "alive" || entries[0].deleted {
		t.Errorf("entry 0 mismatch: %+v", entries[0])
	}
	if entries[1].key != "dead" || !entries[1].deleted {
		t.Errorf("entry 1 should be tombstone: %+v", entries[1])
	}
}

func TestMemtableConcurrent(t *testing.T) {
	m := NewMemtable()

	const goroutines = 8
	const perG = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("g%d-k%d", g, i)
				m.Set(key, []byte("v"))
				m.Get(key)
				m.Size()
			}
		}(g)
	}
	wg.Wait()

	if m.Len() != goroutines*perG {
		t.Fatalf("want %d, got %d", goroutines*perG, m.Len())
	}
}

func TestMemtableValueCopy(t *testing.T) {
	m := NewMemtable()
	buf := []byte("original")
	m.Set("k", buf)

	// Menjaj caller-ov slice — memtable ne sme da vidi promenu.
	buf[0] = 'X'

	v, _ := m.Get("k")
	if string(v) != "original" {
		t.Fatalf("memtable should keep a copy, got %q", v)
	}
}

// --- Benchmark ---

func BenchmarkMemtableSet(b *testing.B) {
	m := NewMemtable()
	key := []byte("some-key")
	val := []byte("some-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(string(key), val)
	}
}

func BenchmarkMemtableGet(b *testing.B) {
	m := NewMemtable()
	for i := 0; i < 10000; i++ {
		m.Set(fmt.Sprintf("key-%d", i), []byte("v"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get("key-5000")
	}
}

func TestBloomNoFalseNegatives(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	for i := 0; i < 1000; i++ {
		bf.Add(fmt.Sprintf("key-%d", i))
	}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if !bf.MayContain(key) {
			t.Fatalf("false negative for %q — filter mora uvek vraćati true za dodate ključeve", key)
		}
	}
}

func TestBloomEmptyFilter(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	// Prazan filter mora vraćati false za sve (svi bitovi su 0).
	for i := 0; i < 100; i++ {
		if bf.MayContain(fmt.Sprintf("anything-%d", i)) {
			t.Fatalf("empty filter returned true for absent key")
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	const n = 1000
	const target = 0.01
	bf := NewBloomFilter(n, target)
	for i := 0; i < n; i++ {
		bf.Add(fmt.Sprintf("present-%d", i))
	}

	const trials = 20000
	fp := 0
	for i := 0; i < trials; i++ {
		if bf.MayContain(fmt.Sprintf("absent-%d", i)) {
			fp++
		}
	}
	rate := float64(fp) / trials
	t.Logf("false positive rate: %.4f (target %.4f)", rate, target)

	// Dozvoljavamo 3x target zbog statističke varijanse.
	if rate > target*3 {
		t.Fatalf("false positive rate too high: %.4f", rate)
	}
}

func TestBloomEncodeDecode(t *testing.T) {
	bf := NewBloomFilter(500, 0.01)
	for i := 0; i < 500; i++ {
		bf.Add(fmt.Sprintf("k-%d", i))
	}

	encoded := bf.Encode()
	decoded, err := DecodeBloomFilter(encoded)
	if err != nil {
		t.Fatalf("DecodeBloomFilter: %v", err)
	}

	// Svi ključevi iz originala moraju biti u dekodiranom.
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("k-%d", i)
		if !decoded.MayContain(key) {
			t.Fatalf("decoded filter lost key %q", key)
		}
	}
}

func TestBloomDecodeTruncated(t *testing.T) {
	_, err := DecodeBloomFilter([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for truncated data")
	}
}

func TestBloomLongKeys(t *testing.T) {
	bf := NewBloomFilter(10, 0.01)
	longKey := string(make([]byte, 10000)) // 10KB ključ
	bf.Add(longKey)

	if !bf.MayContain(longKey) {
		t.Fatal("long key lost")
	}
}

func TestBloomDuplicateAdd(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.Add("k")
	bf.Add("k")
	bf.Add("k")

	if !bf.MayContain("k") {
		t.Fatal("duplicate add broke filter")
	}
}

// --- Benchmark ---

func BenchmarkBloomAdd(b *testing.B) {
	bf := NewBloomFilter(100000, 0.01)
	key := "some-key-value"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(key)
	}
}

func BenchmarkBloomMayContain(b *testing.B) {
	bf := NewBloomFilter(100000, 0.01)
	for i := 0; i < 100000; i++ {
		bf.Add(fmt.Sprintf("key-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.MayContain("key-50000")
	}
}

// --- SSTable tests ---

// writeTestSSTable pravi SSTable od map-e i vraća otvorenu instancu.
func writeTestSSTable(t *testing.T, entries []Entry) (*SSTable, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	sst, err := WriteSSTable(path, entries)
	if err != nil {
		t.Fatalf("WriteSSTable: %v", err)
	}
	t.Cleanup(func() { sst.Close() })
	return sst, path
}

func sortedEntries(pairs map[string]string) []Entry {
	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	entries := make([]Entry, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, Entry{key: k, value: []byte(pairs[k])})
	}
	return entries
}

func TestSSTableWriteReadBasic(t *testing.T) {
	data := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "dark",
	}
	sst, _ := writeTestSSTable(t, sortedEntries(data))

	for k, want := range data {
		v, found, err := sst.Get(k)
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if !found {
			t.Fatalf("Get(%q): not found", k)
		}
		if string(v) != want {
			t.Fatalf("Get(%q) = %q, want %q", k, v, want)
		}
	}
}

func TestSSTableGetMissing(t *testing.T) {
	sst, _ := writeTestSSTable(t, sortedEntries(map[string]string{
		"a": "1", "c": "3", "e": "5",
	}))

	for _, k := range []string{"b", "d", "f", "0", "z"} {
		_, found, err := sst.Get(k)
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if found {
			t.Fatalf("Get(%q): unexpected found", k)
		}
	}
}

func TestSSTableTombstone(t *testing.T) {
	entries := []Entry{
		{key: "alive", value: []byte("v")},
		{key: "dead", value: nil, deleted: true},
	}
	sst, _ := writeTestSSTable(t, entries)

	_, found, err := sst.Get("dead")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("tombstone should be found")
	}
	v, found, _ := sst.Get("dead")
	if v != nil {
		t.Fatalf("tombstone should return nil, got %q", v)
	}
	if !found {
		t.Fatal("tombstone should be found=true")
	}

	v, found, _ = sst.Get("alive")
	if !found || string(v) != "v" {
		t.Fatalf("alive key mismatch: v=%q found=%v", v, found)
	}
}

func TestSSTableEmptyValue(t *testing.T) {
	sst, _ := writeTestSSTable(t, []Entry{
		{key: "empty", value: []byte{}},
	})

	v, found, _ := sst.Get("empty")
	if !found {
		t.Fatal("empty value should be found")
	}
	if v == nil {
		t.Fatal("empty value must be non-nil (that's tombstone)")
	}
	if len(v) != 0 {
		t.Fatalf("want empty, got %q", v)
	}
}

func TestSSTableManyBlocks(t *testing.T) {
	// Napravi dovoljno entries da pređe 4KB po bloku → više blokova.
	const n = 500
	data := make(map[string]string, n)
	for i := 0; i < n; i++ {
		data[fmt.Sprintf("key-%04d", i)] = fmt.Sprintf("value-%04d", i)
	}
	sst, _ := writeTestSSTable(t, sortedEntries(data))

	if sst.NumBlocks() < 2 {
		t.Fatalf("want multiple blocks, got %d", sst.NumBlocks())
	}

	// Svi ključevi moraju biti čitljivi.
	for k, want := range data {
		v, found, err := sst.Get(k)
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if !found || string(v) != want {
			t.Fatalf("Get(%q) = %q, %v; want %q", k, v, found, want)
		}
	}
}

func TestSSTableIterator(t *testing.T) {
	data := map[string]string{
		"a": "1", "b": "2", "c": "3", "d": "4",
	}
	sst, _ := writeTestSSTable(t, sortedEntries(data))

	it := sst.Iterator()
	var got []string
	for it.Next() {
		got = append(got, it.Key())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}

	want := []string{"a", "b", "c", "d"}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d: want %q, got %q", i, want[i], got[i])
		}
	}
}

func TestSSTableIteratorManyBlocks(t *testing.T) {
	const n = 1000
	entries := make([]Entry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, Entry{
			key:   fmt.Sprintf("k-%05d", i),
			value: []byte("v"),
		})
	}
	sst, _ := writeTestSSTable(t, entries)

	it := sst.Iterator()
	count := 0
	for it.Next() {
		count++
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}
	if count != n {
		t.Fatalf("want %d, got %d", n, count)
	}
}

func TestSSTableIteratorTombstones(t *testing.T) {
	entries := []Entry{
		{key: "a", value: []byte("1")},
		{key: "b", deleted: true},
		{key: "c", value: []byte("3")},
	}
	sst, _ := writeTestSSTable(t, entries)

	it := sst.Iterator()
	var got []Entry
	for it.Next() {
		got = append(got, Entry{
			key:     it.Key(),
			value:   it.Value(),
			deleted: it.Deleted(),
		})
	}
	if len(got) != 3 {
		t.Fatalf("want 3, got %d", len(got))
	}
	if got[1].key != "b" || !got[1].deleted {
		t.Errorf("middle should be tombstone: %+v", got[1])
	}
}

func TestSSTablePersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.sst")

	entries := []Entry{
		{key: "a", value: []byte("1")},
		{key: "b", value: []byte("2")},
	}
	sst, err := WriteSSTable(path, entries)
	if err != nil {
		t.Fatal(err)
	}
	sst.Close()

	// Reopen.
	sst2, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sst2.Close()

	v, found, _ := sst2.Get("a")
	if !found || string(v) != "1" {
		t.Fatalf("after reopen: %q, %v", v, found)
	}
}

// --- Benchmark ---

func BenchmarkSSTableGet(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.sst")

	const n = 10000
	entries := make([]Entry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, Entry{
			key:   fmt.Sprintf("key-%05d", i),
			value: []byte("value"),
		})
	}
	sst, err := WriteSSTable(path, entries)
	if err != nil {
		b.Fatal(err)
	}
	defer sst.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sst.Get("key-05000")
	}
}

func BenchmarkSSTableWrite(b *testing.B) {
	const n = 10000
	entries := make([]Entry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, Entry{
			key:   fmt.Sprintf("key-%05d", i),
			value: []byte("value"),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		path := filepath.Join(dir, "bench.sst")
		sst, err := WriteSSTable(path, entries)
		if err != nil {
			b.Fatal(err)
		}
		sst.Close()
	}
}

// --- LSM engine tests ---

func newTestLSM(t *testing.T) (*LSM, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := NewLSM(dir)
	if err != nil {
		t.Fatalf("NewLSM: %v", err)
	}
	// Mali threshold za testove — inače čekamo 4MB da flush-uje.
	l.SetFlushThreshold(8 * 1024) // 8KB
	t.Cleanup(func() { l.Close() })
	return l, dir
}

func TestLSMSetGet(t *testing.T) {
	l, _ := newTestLSM(t)
	if err := l.Set("foo", []byte("bar")); err != nil {
		t.Fatal(err)
	}
	v, err := l.Get("foo")
	if err != nil || string(v) != "bar" {
		t.Fatalf("want bar, got %q, %v", v, err)
	}
}

func TestLSMGetMissing(t *testing.T) {
	l, _ := newTestLSM(t)
	_, err := l.Get("nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestLSMDelete(t *testing.T) {
	l, _ := newTestLSM(t)
	l.Set("k", []byte("v"))
	if err := l.Delete("k"); err != nil {
		t.Fatal(err)
	}
	_, err := l.Get("k")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestLSMOverwrite(t *testing.T) {
	l, _ := newTestLSM(t)
	l.Set("k", []byte("v1"))
	l.Set("k", []byte("v2"))
	v, err := l.Get("k")
	if err != nil || string(v) != "v2" {
		t.Fatalf("want v2, got %q, %v", v, err)
	}
}

func TestLSMEmptyValue(t *testing.T) {
	l, _ := newTestLSM(t)
	l.Set("empty", []byte{})
	v, err := l.Get("empty")
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Fatal("empty value must be non-nil")
	}
	if len(v) != 0 {
		t.Fatalf("want empty, got %q", v)
	}
}

func TestLSMFlushToSSTable(t *testing.T) {
	l, dir := newTestLSM(t)

	// 8KB threshold + ~40B/entry → flush svakih ~200 entries.
	const n = 1000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		if err := l.Set(key, []byte("value-padding-padding-padding")); err != nil {
			t.Fatal(err)
		}
	}

	// Proveri da je bar jedan SSTable nastao.
	entries, _ := os.ReadDir(filepath.Join(dir, "sstables"))
	if len(entries) == 0 {
		t.Fatal("expected at least one SSTable after flush")
	}

	// Svi ključevi moraju biti čitljivi.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		v, err := l.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if string(v) != "value-padding-padding-padding" {
			t.Fatalf("Get(%q) mismatch", key)
		}
	}
}

func TestLSMMultipleFlushes(t *testing.T) {
	l, dir := newTestLSM(t)

	const batches = 3
	const perBatch = 500
	for b := 0; b < batches; b++ {
		for i := 0; i < perBatch; i++ {
			l.Set(fmt.Sprintf("b%d-k%05d", b, i), []byte("value-padding-padding-padding"))
		}
	}

	entries, _ := os.ReadDir(filepath.Join(dir, "sstables"))
	if len(entries) < 2 {
		t.Fatalf("expected multiple SSTables, got %d", len(entries))
	}

	// Svi ključevi iz svih batcheva čitljivi (search ide kroz sve SSTables).
	for b := 0; b < batches; b++ {
		for i := 0; i < perBatch; i += 50 {
			k := fmt.Sprintf("b%d-k%05d", b, i)
			if _, err := l.Get(k); err != nil {
				t.Fatalf("Get(%q): %v", k, err)
			}
		}
	}
}

func TestLSMOverwriteAcrossFlush(t *testing.T) {
	l, _ := newTestLSM(t)

	for i := 0; i < 500; i++ {
		l.Set(fmt.Sprintf("key-%05d", i), []byte("value-padding-padding-padding"))
	}
	// Ovim je verovatno već flush-ovano.

	// Overwrite ključa koji je možda u SSTable-u.
	l.Set("key-00042", []byte("NEW"))

	v, err := l.Get("key-00042")
	if err != nil || string(v) != "NEW" {
		t.Fatalf("want NEW, got %q, %v", v, err)
	}
}

func TestLSMDeleteAcrossFlush(t *testing.T) {
	l, _ := newTestLSM(t)

	for i := 0; i < 500; i++ {
		l.Set(fmt.Sprintf("key-%05d", i), []byte("value-padding-padding-padding"))
	}

	l.Delete("key-00042")

	_, err := l.Get("key-00042")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestLSMPersistenceViaWAL(t *testing.T) {
	dir := t.TempDir()

	// Sesija 1: upiši, zatvori (bez flush-a).
	l, err := NewLSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	l.Set("persist1", []byte("v1"))
	l.Set("persist2", []byte("v2"))
	l.Close()

	// Sesija 2: otvori, proveri (WAL replay).
	l2, err := NewLSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()

	v, err := l2.Get("persist1")
	if err != nil || string(v) != "v1" {
		t.Fatalf("after reopen: %q, %v", v, err)
	}
	v, err = l2.Get("persist2")
	if err != nil || string(v) != "v2" {
		t.Fatalf("after reopen: %q, %v", v, err)
	}
}

func TestLSMPersistenceAfterFlush(t *testing.T) {
	dir := t.TempDir()

	l, err := NewLSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	l.SetFlushThreshold(8 * 1024)

	const n = 1000
	for i := 0; i < n; i++ {
		l.Set(fmt.Sprintf("k-%05d", i), []byte("value-padding-padding-padding"))
	}
	l.Close()

	// Reopen i proveri da su podaci u SSTable-ovima.
	l2, err := NewLSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()

	for i := 0; i < n; i += 100 {
		k := fmt.Sprintf("k-%05d", i)
		v, err := l2.Get(k)
		if err != nil {
			t.Fatalf("after reopen Get(%q): %v", k, err)
		}
		if string(v) != "value-padding-padding-padding" {
			t.Fatalf("after reopen Get(%q) mismatch", k)
		}
	}
}

func TestLSMDeletePersistsViaWAL(t *testing.T) {
	dir := t.TempDir()

	l, _ := NewLSM(dir)
	l.Set("k", []byte("v"))
	l.Delete("k")
	l.Close()

	l2, _ := NewLSM(dir)
	defer l2.Close()

	_, err := l2.Get("k")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestLSMConcurrentWrites(t *testing.T) {
	l, _ := newTestLSM(t)

	const goroutines = 8
	const perG = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("g%d-k%d", g, i)
				if err := l.Set(key, []byte("v")); err != nil {
					t.Errorf("Set(%q): %v", key, err)
				}
			}
		}(g)
	}
	wg.Wait()

	for g := 0; g < goroutines; g++ {
		for i := 0; i < perG; i++ {
			k := fmt.Sprintf("g%d-k%d", g, i)
			if _, err := l.Get(k); err != nil {
				t.Fatalf("Get(%q): %v", k, err)
			}
		}
	}
}

func TestLSMConcurrentReads(t *testing.T) {
	l, _ := newTestLSM(t)
	for i := 0; i < 100; i++ {
		l.Set(fmt.Sprintf("k%d", i), []byte("v"))
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				l.Get(fmt.Sprintf("k%d", i%100))
			}
		}()
	}
	wg.Wait()
}

func TestLSMCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLSM(dir)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// --- Benchmark ---

func BenchmarkLSMSet(b *testing.B) {
	dir := b.TempDir()
	l, err := NewLSM(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Set(fmt.Sprintf("key-%d", i), []byte("value"))
	}
}

func BenchmarkLSMGet(b *testing.B) {
	dir := b.TempDir()
	l, err := NewLSM(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()

	for i := 0; i < 10000; i++ {
		l.Set(fmt.Sprintf("key-%05d", i), []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Get("key-05000")
	}
}

// --- Compaction tests ---

func TestCompactionMergesTables(t *testing.T) {
	l, dir := newTestLSM(t)
	l.SetFlushThreshold(2 * 1024)

	const n = 3000
	for i := 0; i < n; i++ {
		l.Set(fmt.Sprintf("k-%05d", i), []byte("value-padding-padding-padding-padding"))
	}

	entries, _ := os.ReadDir(filepath.Join(dir, "sstables"))
	if len(entries) >= compactThreshold {
		t.Fatalf("auto-compaction should have reduced SSTables, got %d", len(entries))
	}

	for i := 0; i < n; i += 100 {
		k := fmt.Sprintf("k-%05d", i)
		if _, err := l.Get(k); err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
	}
}

func TestCompactionKeepsNewest(t *testing.T) {
	l, _ := newTestLSM(t)
	l.SetFlushThreshold(2 * 1024)

	// Prvo upiši v1 — neka se flush-uje.
	for i := 0; i < 100; i++ {
		l.Set(fmt.Sprintf("pad-%05d", i), []byte("value-padding-padding-padding"))
	}
	l.Set("target", []byte("v1"))

	// Zatim još podataka da se flush-uje i target završi u starijem SSTable-u.
	for i := 0; i < 200; i++ {
		l.Set(fmt.Sprintf("pad2-%05d", i), []byte("value-padding-padding-padding"))
	}

	// Sada overwrite target — novija verzija.
	l.Set("target", []byte("v2"))

	// Forsiraj compaction.
	if err := l.Compact(); err != nil {
		t.Fatal(err)
	}

	v, err := l.Get("target")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "v2" {
		t.Fatalf("want v2, got %q", v)
	}
}
func TestCompactionDropsTombstones(t *testing.T) {
	l, _ := newTestLSM(t)
	l.SetFlushThreshold(2 * 1024)

	for i := 0; i < 100; i++ {
		l.Set(fmt.Sprintf("pad-%05d", i), []byte("value-padding-padding-padding"))
	}
	l.Set("doomed", []byte("value"))

	for i := 0; i < 200; i++ {
		l.Set(fmt.Sprintf("pad2-%05d", i), []byte("value-padding-padding-padding"))
	}

	l.Delete("doomed")

	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := l.Compact(); err != nil {
		t.Fatal(err)
	}

	_, err := l.Get("doomed")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	entries, _ := os.ReadDir(l.sstDir)
	for _, e := range entries {
		path := filepath.Join(l.sstDir, e.Name())
		sst, err := OpenSSTable(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		it := sst.Iterator()
		for it.Next() {
			if it.Key() == "doomed" {
				t.Errorf("key %q survived compaction (deleted=%v)", "doomed", it.Deleted())
			}
		}
		sst.Close()
	}
}

func TestCompactionPersistence(t *testing.T) {
	dir := t.TempDir()

	l, err := NewLSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	l.SetFlushThreshold(2 * 1024)

	const n = 2000
	for i := 0; i < n; i++ {
		l.Set(fmt.Sprintf("k-%05d", i), []byte("value-padding-padding-padding"))
	}
	if err := l.Compact(); err != nil {
		t.Fatal(err)
	}
	l.Close()

	// Reopen i proveri.
	l2, err := NewLSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()

	for i := 0; i < n; i += 50 {
		k := fmt.Sprintf("k-%05d", i)
		if _, err := l2.Get(k); err != nil {
			t.Fatalf("after reopen Get(%q): %v", k, err)
		}
	}
}

func TestCompactionAllTombstones(t *testing.T) {
	l, dir := newTestLSM(t)
	l.SetFlushThreshold(2 * 1024)

	const n = 200
	for i := 0; i < n; i++ {
		l.Set(fmt.Sprintf("k-%05d", i), []byte("value-padding-padding-padding"))
	}
	for i := 0; i < n; i++ {
		l.Delete(fmt.Sprintf("k-%05d", i))
	}

	for i := 0; i < 200; i++ {
		l.Set(fmt.Sprintf("pad-%05d", i), []byte("value-padding-padding-padding"))
	}
	for i := 0; i < 200; i++ {
		l.Delete(fmt.Sprintf("pad-%05d", i))
	}

	if err := l.Compact(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k-%05d", i)
		_, err := l.Get(k)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(%q): want ErrNotFound, got %v", k, err)
		}
	}

	entries, _ := os.ReadDir(filepath.Join(dir, "sstables"))
	t.Logf("SSTables after compaction: %d", len(entries))
}

func TestCompactionReducesFileCount(t *testing.T) {
	l, dir := newTestLSM(t)
	l.SetFlushThreshold(2 * 1024)

	const n = 10000
	for i := 0; i < n; i++ {
		l.Set(fmt.Sprintf("k-%06d", i), []byte("value-padding-padding-padding-padding"))
	}

	entries, _ := os.ReadDir(filepath.Join(dir, "sstables"))
	// Threshold is 4, so after auto-compaction there should be no more than 3.
	if len(entries) >= compactThreshold {
		t.Fatalf("expected < %d SSTables, got %d", compactThreshold, len(entries))
	}

	// All keys must be readable.
	for i := 0; i < n; i += 500 {
		k := fmt.Sprintf("k-%06d", i)
		if _, err := l.Get(k); err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
	}
}
