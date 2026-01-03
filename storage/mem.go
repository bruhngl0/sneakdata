package storage

import (
	"bytes"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"sync"

	"rsc.io/omap"
	"rsc.io/ordered"
	"rsc.io/top"
)

// A MemLocker is a single-process implementation
// of the database Lock and Unlock methods,
// suitable if there is only one process accessing the
// database at a time.
//
// The zero value for a MemLocker
// is a valid MemLocker with no locks held.
// It must not be copied after first use.
type MemLocker struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// Lock locks the mutex with the given name.
func (l *MemLocker) Lock(name string) {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = make(map[string]*sync.Mutex)
	}
	mu := l.locks[name]
	if mu == nil {
		mu = new(sync.Mutex)
		l.locks[name] = mu
	}
	l.mu.Unlock()
	mu.Lock()
}

// Unlock unlocks the mutex with the given name.
func (l *MemLocker) Unlock(name string) {
	l.mu.Lock()
	mu := l.locks[name]
	l.mu.Unlock()
	if mu == nil {

		panic("Unlock of never locked key")
	}

	mu.Unlock()
}

// A memDB is an in-memory DB implementation,.
type memDB struct {
	MemLocker
	mu   sync.RWMutex
	data omap.Map[string, []byte]
}

func MemDB() DB {
	return new(memDB)
}
func (*memDB) Close() {}

func (*memDB) Panic(msg string, args ...any) {
	Panic(msg, args...)
}

// Get returns the value associated with the key.
func (db *memDB) Get(key []byte) (val []byte, ok bool) {
	db.mu.RLock()
	v, ok := db.data.Get(string(key))
	db.mu.RUnlock()
	if ok {
		v = bytes.Clone(v)
	}
	return v, ok
}

// Scan returns an iterator over all key-value pairs
// in the range start ≤ key ≤ end.
func (db *memDB) Scan(start, end []byte) iter.Seq2[[]byte, func() []byte] {
	lo := string(start)
	hi := string(end)
	return func(yield func(key []byte, val func() []byte) bool) {
		db.mu.RLock()
		locked := true
		defer func() {
			if locked {
				db.mu.RUnlock()
			}
		}()
		for k, v := range db.data.Scan(lo, hi) {
			key := []byte(k)
			val := func() []byte { return bytes.Clone(v) }
			db.mu.RUnlock()
			locked = false
			if !yield(key, val) {
				return
			}
			db.mu.RLock()
			locked = true
		}
	}
}

// Delete deletes any entry with the given key
func (db *memDB) Delete(key []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data.Delete(string(key))
}

// Delete range deletes all the entries with start ≤ key ≤ end.
func (db *memDB) DeleteRange(start, end []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data.DeleteRange(string(start), string(end))
}

// Set sets the value assoicated with key to val
func (db *memDB) Set(key, val []byte) {
	if len(key) == 0 {
		db.Panic("mem set: empty key")
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	db.data.Set(string(key), bytes.Clone(val))
}

// Batch returns a new batch
func (db *memDB) Batch() Batch {
	return &memBatch{db: db}
}

// Flush flushes everything into a persistent storage.
// Since it is an in memory database, the memory is as persistent as it gets
func (db *memDB) Flush() {}

// A memBatch is a batch for memDB
type memBatch struct {
	db  *memDB   //underlying databse
	ops []func() //operations to appy
}

func (b *memBatch) Set(key, val []byte) {
	if len(key) == 0 {
		b.db.Panic("memdb batch set: empty key")
	}
	k := string(key)
	v := bytes.Clone(val)
	b.ops = append(b.ops, func() { b.db.data.Set(k, v) })
}

func (b *memBatch) Delete(key []byte) {
	k := string(key)
	b.ops = append(b.ops, func() { b.db.data.Delete(k) })
}

func (b *memBatch) DeleteRange(start, end []byte) {
	s := string(start)
	e := string(end)
	b.ops = append(b.ops, func() { b.db.data.DeleteRange(s, e) })
}

func (b *memBatch) MaybeApply() bool {
	return false
}

func (b *memBatch) Apply() {
	b.db.mu.Lock()
	defer b.db.mu.Unlock()
	for _, op := range b.ops {
		op()
	}
	b.ops = nil
}
