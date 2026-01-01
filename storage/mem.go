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

type MemLocker struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

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

// Get returns the value associated with the key
func (db *memDB) Get(key []byte) (val []byte, ok bool) {
	db.mu.RLock()
	v, ok := db.data.Get(string(key))
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
