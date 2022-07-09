package fcache

import (
	"errors"
	"io"
	"time"
)

type Size int64

const (
	Byte Size = 1
	KB        = 1000 * Byte
	KiB       = 1024 * Byte
	MB        = 1000 * KB
	MiB       = 1024 * KiB
	GB        = 1000 * MB
	GiB       = 1024 * MiB
	TB        = 1000 * GB
	TiB       = 1024 * GiB
)

var ErrNotFound = errors.New("entry not found")

type Filler interface {
	// WriteCacheData is used by GetOrPut and GetReaderOrPut to atomically insert cache entries.
	// Write the content for key into sink and return the number of written bytes.
	// Errors are passed through to GetOrPut or GetReaderOrPut.
	WriteCacheData(key uint64, sink io.Writer) (written int64, err error)
}

// FillerFunc is used by GetOrPut and GetReaderOrPut to atomically insert cache entries.
// Write the content for key into sink and return the number of written bytes.
// Errors are passed through to GetOrPut or GetReaderOrPut.
type FillerFunc func(key uint64, sink io.Writer) (written int64, err error)

func (f FillerFunc) WriteCacheData(key uint64, sink io.Writer) (written int64, err error) {
	return f(key, sink)
}

type Stats struct {
	Items          int
	Bytes          int64
	Has            int64
	Gets           int64
	Hits           int64
	Puts           int64
	Deletes        int64
	Evictions      int64
	EvictionErrors []EvictionError
}

type EntryInfo struct {
	Size    int64
	Mtime   time.Time
	Expires time.Time
}

type EvictionError struct {
	Time  time.Time
	Error error
}

type Cache interface {
	// Stats returns statistics about the cache.
	// Like number of items, used bytes, number of hits and more.
	// It also returns a list of recent eviction errors if there are any.
	Stats() Stats

	// Has returns the EntryInfo of a cache entry or ErrNotFound if no entry for that key exists.
	Has(key uint64) (info *EntryInfo, err error)

	// Put inserts a byte slice under the provided key into the cache.
	// If an entry with the provided key already exists it gets overwritten.
	// Set the time to live (ttl) to 0 if the entry should not expire.
	Put(key uint64, data []byte, ttl time.Duration) (info *EntryInfo, err error)

	// PutReader inserts the content of reader under the provided key into the cache.
	// If an entry with the provided key already exists it gets overwritten.
	// Set the time to live (ttl) to 0 if the entry should not expire.
	PutReader(key uint64, reader io.Reader, ttl time.Duration) (info *EntryInfo, err error)

	// Get returns the content of the cache entry at key as byte slice.
	// If no cache entry exists for key ErrNotFound is returned.
	Get(key uint64) (data []byte, info *EntryInfo, err error)

	// GetReader returns the content of the cache entry at key as io.ReadSeekCloser.
	// If no cache entry exists for key ErrNotFound is returned.
	// Makes sure to call Close() on the reader when you are done reading.
	GetReader(key uint64) (reader io.ReadSeekCloser, info *EntryInfo, err error)

	// GetOrPut atomically gets or inserts a cache entry as byte slice.
	// If no entry exists at key the Filler is used to insert the entry before returning its content.
	// This functions locks the key for the duration of the insert, so all other calls to this key will block.
	// Using only GetOrPut or GetReaderOrPut guarantees that the insert only happens exactly once.
	GetOrPut(key uint64, ttl time.Duration, filler Filler) (data []byte, info *EntryInfo, hit bool, err error)

	// GetReaderOrPut atomically gets or inserts a cache entry as reader.
	// If no entry exists at key the Filler is used to insert the entry before returning its content.
	// This functions locks the key for the duration of the insert, so all other calls to this key will block.
	// Using only GetReaderOrPut or GetOrPut guarantees that the insert only happens exactly once.
	GetReaderOrPut(key uint64, ttl time.Duration, filler Filler) (reader io.ReadSeekCloser, info *EntryInfo, hit bool, err error)

	// Delete removes the cache entry at key.
	// If no cache entry exists at key nothing happens.
	Delete(key uint64) (info *EntryInfo, err error)

	// Clear removes all cache entries and optionally resets statistics.
	Clear(resetStats bool) error
}
