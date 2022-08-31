package fcache

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestLockerLock(t *testing.T) {
	l := NewLocker()
	l.Lock(1)
	holder := l.locks[1]

	if holder.users != 1 {
		t.Fatalf("expected users to be 1, got :%d", holder.users)
	}

	chDone := make(chan struct{})
	go func() {
		l.Lock(1)
		close(chDone)
	}()

	chWaiting := make(chan struct{})
	go func() {
		for range time.Tick(1 * time.Millisecond) {
			if holder.users == 2 {
				close(chWaiting)
				break
			}
		}
	}()

	select {
	case <-chWaiting:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for lock users to be incremented")
	}

	select {
	case <-chDone:
		t.Fatal("lock should not have returned while it was still held")
	default:
	}

	l.Unlock(1)

	select {
	case <-chDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("lock should have completed")
	}

	if holder.users != 1 {
		t.Fatalf("expected users to be 1, got: %d", holder.users)
	}
}

func TestLockerUnlock(t *testing.T) {
	l := NewLocker()

	l.Lock(1)
	l.Unlock(1)

	chDone := make(chan struct{})
	go func() {
		l.Lock(1)
		close(chDone)
	}()

	select {
	case <-chDone:
	case <-time.After(1 * time.Second):
		t.Fatalf("lock should not be blocked")
	}
}

func TestLockerConcurrency(t *testing.T) {
	l := NewLocker()

	expectedWrites := 0
	writes := 0

	var wg sync.WaitGroup
	for i := 0; i <= 1000; i++ {
		wg.Add(1)
		r := rand.Intn(2)
		if r == 0 {
			expectedWrites += 1
		}
		go func(r int) {
			defer wg.Done()
			if r == 0 {
				l.Lock(1)
				writes += 1
				defer l.Unlock(1)
			} else {
				l.RLock(1)
				defer l.RUnlock(1)
			}
			s := time.Now()
			for time.Now().Sub(s) < 2*time.Millisecond {
				// busy waiting, with time.Sleep it takes way longer for some reason
			}
		}(r)
	}

	chDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(chDone)
	}()

	select {
	case <-chDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for locks to complete")
	}

	if holder, exists := l.locks[1]; exists {
		t.Fatalf("lock should not exist: %v", holder)
	}

	if expectedWrites != writes {
		t.Fatalf("expected %d writes but got %d", expectedWrites, writes)
	}
}

func BenchmarkLocker(b *testing.B) {
	l := NewLocker()
	for i := 0; i < b.N; i++ {
		l.Lock(1)
		l.Unlock(1)
	}
}

func BenchmarkLockerParallel(b *testing.B) {
	l := NewLocker()
	b.SetParallelism(128)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Lock(1)
			l.Unlock(1)
		}
	})
}

func BenchmarkLockerMoreKeys(b *testing.B) {
	l := NewLocker()
	b.SetParallelism(128)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := uint64(rand.Intn(64))
			l.Lock(k)
			time.Sleep(5 * time.Millisecond)
			l.Unlock(k)
		}
	})
}
