package fcache

import (
	"sync"
)

// Locker provides a key based locking mechanism
type Locker struct {
	holderPool sync.Pool
	mu         sync.Mutex
	locks      map[uint64]*lockHolder
}

type lockHolder struct {
	lock sync.RWMutex
	// users is the number of users currently interacting with the lock
	users int32
}

// NewLocker creates a new Locker ready to be used
func NewLocker() *Locker {
	return &Locker{
		holderPool: sync.Pool{New: func() any { return new(lockHolder) }},
		locks:      make(map[uint64]*lockHolder),
	}
}

func (l *Locker) prepareToLock(key uint64) *sync.RWMutex {
	l.mu.Lock()

	holder, exists := l.locks[key]
	if !exists {
		holder = l.holderPool.Get().(*lockHolder)
		l.locks[key] = holder
	}
	holder.users += 1

	l.mu.Unlock()

	return &holder.lock
}

// Lock locks a mutex with the given key for writing. If no mutex for that key exists it is created.
func (l *Locker) Lock(key uint64) {
	lock := l.prepareToLock(key)
	lock.Lock()
}

// RLock locks a mutex with the given key for reading. If no mutex for that key exists it is created.
func (l *Locker) RLock(key uint64) {
	lock := l.prepareToLock(key)
	lock.RLock()
}

func (l *Locker) prepareToUnlock(key uint64) *sync.RWMutex {
	l.mu.Lock()

	holder, exists := l.locks[key]
	if !exists {
		l.mu.Unlock()
		return nil
	}

	holder.users -= 1      // about to unlock so we can decrement the number of users for this lock
	if holder.users == 0 { // if this was the last user we also remove it from the map
		delete(l.locks, key)
		l.holderPool.Put(holder) // potential early reuse by another key before final unlock should not be a problem
	}

	l.mu.Unlock()

	return &holder.lock
}

// Unlock unlocks the mutex with the given key
func (l *Locker) Unlock(key uint64) {
	lock := l.prepareToUnlock(key)
	if lock != nil {
		lock.Unlock()
	}
}

// RUnlock unlocks the mutex with the given key
func (l *Locker) RUnlock(key uint64) {
	lock := l.prepareToUnlock(key)
	if lock != nil {
		lock.RUnlock()
	}
}
