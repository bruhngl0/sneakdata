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

func MemDB() DB {
	return new(memDB)
}

// A memDB is an in-memory DB implementation,.
type memDB struct {
	MemLocker
	mu   sync.RWMutex
	data omap.Map[string, []byte]
}

func (*memDB) Close() {}

func (*memDB) Panic(msg string, args ...any) {
	Panic(msg, args...)
}
