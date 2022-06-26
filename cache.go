package fcache

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type cacheEntry struct {
	key     uint64
	lock    sync.RWMutex
	onDisk  bool
	path    string
	size    int64
	mtime   time.Time
	expires time.Time
}

func (e *cacheEntry) isExpired() bool {
	return !e.expires.IsZero() && e.expires.Before(time.Now())
}

func (e *cacheEntry) isValid() bool {
	return e.onDisk && !e.isExpired()
}

type cache struct {
	cacheDir   string
	targetSize int64

	usedSize  int64
	has       int64
	gets      int64
	hits      int64
	puts      int64
	deletes   int64
	evictions int64

	lock        sync.RWMutex
	entriesList *list.List
	entriesMap  map[uint64]*list.Element

	evictionLock     sync.RWMutex
	evictionInterval time.Duration
	evictionTime     time.Time
	evictionErrors   []EvictionError
}

// Stats returns some metrics about the cache and the last eviction error if there is any.
func (c *cache) Stats() Stats {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return Stats{
		Items:          c.entriesList.Len(),
		Bytes:          c.usedSize,
		Has:            c.has,
		Gets:           c.gets,
		Hits:           c.hits,
		Puts:           c.puts,
		Deletes:        c.deletes,
		Evictions:      c.evictions,
		EvictionErrors: c.evictionErrors,
	}
}

func (c *cache) Has(key uint64) (*EntryInfo, error) {
	c.lock.RLock()
	item, ok := c.entriesMap[key]
	c.has += 1
	c.lock.RUnlock()

	if ok {
		entry := item.Value.(*cacheEntry)
		entry.lock.RLock()
		defer entry.lock.RUnlock()

		if entry.isValid() {
			return &EntryInfo{Size: entry.size, Mtime: entry.mtime, Expires: entry.expires}, nil
		}
	}

	return nil, ErrNotFound
}

// Put adds a byte slice as a blob to the cache against the given key.
func (c *cache) Put(key uint64, val []byte, ttl time.Duration) (*EntryInfo, error) {
	return c.PutReader(key, bytes.NewReader(val), ttl)
}

// PutReader adds the contents of a reader as a blob to the cache against the given key.
func (c *cache) PutReader(key uint64, r io.Reader, ttl time.Duration) (*EntryInfo, error) {
	entry := c.putEntry(key, ttl, true)

	entry.lock.Lock()

	removedBytes, addedBytes, err := c.writeEntry(entry, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
		return io.Copy(sink, r)
	}))

	defer c.finalizePut(entry, removedBytes, addedBytes, err)

	if err != nil {
		return nil, err
	}

	info := &EntryInfo{Size: entry.size, Mtime: entry.mtime, Expires: entry.expires}
	return info, nil
}

// Get returns a byte slice for a blob in the cache, or NotFound otherwise.
func (c *cache) Get(key uint64) ([]byte, *EntryInfo, error) {
	r, info, err := c.GetReader(key)
	if err != nil {
		return nil, info, err
	}
	defer func() {
		_ = r.Close()
	}()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, info, err
	}
	return data, info, nil
}

// GetReader returns a reader for a blob in the cache, or NotFound otherwise.
func (c *cache) GetReader(key uint64) (ReadSeekCloser, *EntryInfo, error) {
	entry := c.getEntry(key, true)
	if entry == nil {
		return nil, nil, ErrNotFound
	}

	entry.lock.RLock()

	if !entry.isValid() {
		entry.lock.RUnlock()
		return nil, nil, ErrNotFound
	}
	defer func() {
		c.lock.Lock()
		c.hits += 1
		c.lock.Unlock()
	}()

	f, err := os.Open(entry.path)
	if err != nil {
		entry.lock.RUnlock()
		return nil, nil, err
	}
	info := &EntryInfo{Size: entry.size, Mtime: entry.mtime, Expires: entry.expires}

	entry.lock.RUnlock()
	return f, info, nil
}

// GetOrPut returns a byte slice for a blob in the cache, or atomically updates it with the data from Filler and then returns the new data.
func (c *cache) GetOrPut(key uint64, ttl time.Duration, filler Filler) (data []byte, info *EntryInfo, hit bool, err error) {
	r, info, hit, err := c.GetReaderOrPut(key, ttl, filler)
	if err != nil {
		return nil, info, hit, err
	}
	defer func() {
		err = r.Close()
	}()

	data, err = io.ReadAll(r)
	if err != nil {
		return nil, info, hit, err
	}
	return data, info, hit, nil
}

// GetReaderOrPut returns a reader for a blob in the cache, or atomically updates it with the data from Filler and then returns the new data.
func (c *cache) GetReaderOrPut(key uint64, ttl time.Duration, filler Filler) (r ReadSeekCloser, info *EntryInfo, hit bool, err error) {
	c.lock.Lock()
	entry := c.getEntry(key, false)
	if entry != nil {
		entry.lock.RLock()

		if entry.isValid() {
			c.hits += 1
			// we are sure entry exits & is valid,
			// so we can release cache lock and continue like a normal get
			c.lock.Unlock()
			defer entry.lock.RUnlock()

			r, err = os.Open(entry.path)
			if err != nil {
				return nil, nil, false, err
			}

			return r, &EntryInfo{Size: entry.size, Mtime: entry.mtime, Expires: entry.expires}, true, nil
		}

		// entry was deleted or is expired, so we need to do a put.
		// release read lock, so we can upgrade to write lock on entry after the putEntry call
		// we still hold cache lock which should prevent races
		entry.lock.RUnlock()
	}

	// update or create the entry and lock it for writing
	entry = c.putEntry(key, ttl, false)
	entry.lock.Lock()

	// now we always have an entry write lock and can release the cache lock
	// before actually filling the entry with data
	c.lock.Unlock()

	removedBytes, addedBytes, err := c.writeEntry(entry, filler)
	defer c.finalizePut(entry, removedBytes, addedBytes, err)

	if err != nil {
		return nil, nil, false, err
	}

	r, err = os.Open(entry.path)
	if err != nil {
		return nil, nil, false, err
	}
	info = &EntryInfo{Size: entry.size, Mtime: entry.mtime, Expires: entry.expires}

	return r, info, false, nil
}

// Delete deletes the specified cache entry if it exists. If it does not exit this is a noop.
func (c *cache) Delete(key uint64) (*EntryInfo, error) {
	c.lock.Lock()
	c.deletes += 1
	item, ok := c.entriesMap[key]
	var entry *cacheEntry
	if ok {
		entry = item.Value.(*cacheEntry)

		c.entriesList.Remove(item)
		delete(c.entriesMap, key)
		c.usedSize -= entry.size
	}
	c.lock.Unlock()

	if ok {
		err := c.removeFile(entry)
		if err != nil {
			return nil, err
		}
		return &EntryInfo{Size: entry.size, Mtime: entry.mtime, Expires: entry.expires}, nil
	}

	return nil, nil
}

func (c *cache) getEntry(key uint64, lock bool) *cacheEntry {
	if lock {
		c.lock.RLock()
		defer c.lock.RUnlock()
	}

	c.gets += 1

	item, ok := c.entriesMap[key]
	if ok {
		entry := item.Value.(*cacheEntry)
		c.entriesList.MoveToFront(item)
		return entry
	}

	return nil
}

func (c *cache) putEntry(key uint64, ttl time.Duration, lock bool) *cacheEntry {
	if lock {
		c.lock.Lock()
		defer c.lock.Unlock()
	}

	c.puts += 1

	mtime := time.Now()
	var expires time.Time
	if ttl > 0 {
		expires = mtime.Add(ttl)
	}

	var entry *cacheEntry
	item, exists := c.entriesMap[key]
	if exists {
		entry = item.Value.(*cacheEntry)
	} else {
		entry = &cacheEntry{key: key}
	}

	entry.mtime = mtime
	entry.expires = expires
	entry.onDisk = false

	if exists {
		c.entriesList.MoveToFront(item)
	} else {
		c.entriesMap[entry.key] = c.entriesList.PushFront(entry)
	}

	return entry
}

func toFilename(key uint64, mtime time.Time, expires time.Time) (string, string) {
	var expiresStr string
	if expires.IsZero() {
		expiresStr = "+"
	} else {
		expiresStr = strconv.FormatInt(expires.UnixMilli(), 36)
	}
	keyStr := strconv.FormatUint(key, 36)
	var shard string
	if len(keyStr) > 2 {
		shard = keyStr[len(keyStr)-2:]
	} else {
		shard = fmt.Sprintf("%02s", keyStr)
	}
	return shard, keyStr + "_" + strconv.FormatInt(mtime.UnixMilli(), 36) + "_" + expiresStr
}

func fromFilename(filename string) (key uint64, mtime time.Time, expires time.Time, err error) {
	parts := strings.Split(filename, "_")
	if len(parts) != 3 {
		return key, mtime, expires, fmt.Errorf("%s should contain 3 pipe symbols but has %d", filename, len(parts))
	}

	key, err = strconv.ParseUint(parts[0], 36, 64)
	if err != nil {
		return key, mtime, expires, fmt.Errorf("failed to restore key from %s: %w", parts[0], err)
	}

	mtimeEpochMillis, err := strconv.ParseInt(parts[1], 36, 64)
	if err != nil {
		return key, mtime, expires, fmt.Errorf("failed to restore mtime from %s: %w", parts[1], err)
	}
	mtime = time.UnixMilli(mtimeEpochMillis)

	if parts[2] != "+" {
		expiresEpochMillis, err := strconv.ParseInt(parts[2], 36, 64)
		if err != nil {
			return key, mtime, expires, fmt.Errorf("failed to restore expires from %s: %w", parts[2], err)
		}
		expires = time.UnixMilli(expiresEpochMillis)
	}

	return key, mtime, expires, nil
}

func (c *cache) writeEntry(entry *cacheEntry, filler Filler) (removedBytes int64, addedBytes int64, err error) {
	var f *os.File
	defer func() {
		if f != nil {
			if err != nil {
				// close file but do not overwrite other error
				_ = f.Close()
			} else {
				err = f.Close()
			}
		}
	}()

	shard, name := toFilename(entry.key, entry.mtime, entry.expires)
	newPath := filepath.Join(c.cacheDir, shard, name)

	if entry.path != "" {
		err = os.Remove(entry.path)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return removedBytes, addedBytes, err
		}
		removedBytes = entry.size
	}
	entry.path = newPath

	dir := filepath.Join(c.cacheDir, shard)
	err = os.MkdirAll(dir, 0750)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return removedBytes, addedBytes, err
	}

	f, err = os.OpenFile(entry.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	if err != nil {
		return removedBytes, addedBytes, err
	}
	entry.onDisk = true

	addedBytes, err = filler.WriteCacheData(entry.key, f)
	if err != nil {
		return removedBytes, addedBytes, err
	}
	entry.size = addedBytes

	return removedBytes, addedBytes, nil
}

// finalizePut updates usedSize or removes the invalid entry when an error occurred during writing.
// It must be called with a locked entry and will unlock that entry.
// It also triggers eviction.
func (c *cache) finalizePut(entry *cacheEntry, removedBytes int64, addedBytes int64, err error) {
	// Note: make sure to release entry lock before requesting cache lock otherwise GetReaderOrPut deadlocks

	if err != nil { // cleanup failed puts
		_ = os.Remove(entry.path)
		entry.onDisk = false
		entry.lock.Unlock()

		c.lock.Lock()
		item, ok := c.entriesMap[entry.key]
		if ok {
			c.entriesList.Remove(item)
		}
		delete(c.entriesMap, entry.key)
		c.lock.Unlock()

	} else { // update usedSize
		entry.lock.Unlock()

		c.lock.Lock()
		if removedBytes > 0 {
			c.usedSize -= removedBytes
		}
		if addedBytes > 0 {
			c.usedSize += addedBytes
		}
		c.lock.Unlock()
	}

	// do eviction in background after put
	go c.evict()
}

func (c *cache) removeFile(entry *cacheEntry) error {
	entry.lock.Lock()
	defer entry.lock.Unlock()

	if entry.onDisk {
		err := os.Remove(entry.path)
		if err != nil {
			return err
		}
		entry.onDisk = false
	}

	return nil
}

func (c *cache) loadEntries() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	shardDirs, err := os.ReadDir(c.cacheDir)
	if err != nil {
		return err
	}

	for _, shardDir := range shardDirs {
		if shardDir.IsDir() {
			shardDirPath := filepath.Join(c.cacheDir, shardDir.Name())
			entries, err := os.ReadDir(shardDirPath)
			if err != nil {
				return err
			}

			for _, entry := range entries {
				if !entry.IsDir() {
					path := filepath.Join(shardDirPath, entry.Name())
					key, mtime, expires, err := fromFilename(entry.Name())
					if err != nil {
						return fmt.Errorf("failed to restore meta from %s: %w", path, err)
					}

					info, err := entry.Info()
					if err != nil {
						return fmt.Errorf("failed to restore size from %s: %w", path, err)
					}

					ce := &cacheEntry{key: key, onDisk: true, path: path, size: info.Size(), mtime: mtime, expires: expires}
					c.entriesMap[ce.key] = c.entriesList.PushFront(ce)
					c.usedSize += ce.size
				}
			}
		}
	}

	return nil
}

func (c *cache) evict() {
	c.evictionLock.Lock()
	defer c.evictionLock.Unlock()

	if !c.evictionTime.IsZero() && time.Now().Sub(c.evictionTime) < c.evictionInterval {
		return
	}

	c.lock.Lock()

	if c.usedSize > c.targetSize {

		var expired []*list.Element
		expiredSize := int64(0)

		var candidates []*list.Element
		candidatesSize := int64(0)

		for item := c.entriesList.Back(); item != nil; item = item.Prev() {
			entry := item.Value.(*cacheEntry)

			if entry.isExpired() {
				expired = append(expired, item)
				expiredSize += entry.size
			} else {
				if c.usedSize-(expiredSize+candidatesSize) > c.targetSize {
					candidates = append(candidates, item)
					candidatesSize += entry.size
				}
			}

			if c.usedSize-expiredSize < c.targetSize {
				break
			}
		}

		var deleted []*cacheEntry

		// remove all expired
		for _, item := range expired {
			entry := c.entriesList.Remove(item).(*cacheEntry)
			delete(c.entriesMap, entry.key)
			c.usedSize -= entry.size
			c.evictions += 1

			deleted = append(deleted, entry)
		}

		// if target size is still not reached remove lru candidates until it is
		for _, item := range candidates {
			if c.usedSize < c.targetSize {
				break
			}

			entry := c.entriesList.Remove(item).(*cacheEntry)
			delete(c.entriesMap, entry.key)
			c.usedSize -= entry.size
			c.evictions += 1

			deleted = append(deleted, entry)
		}

		// we can release the cache lock now and delete the actual files in "background"
		c.lock.Unlock()

		for _, entry := range deleted {
			err := c.removeFile(entry)
			if err != nil {
				c.evictionErrors = append(c.evictionErrors, EvictionError{
					Time:  time.Now(),
					Error: err,
				})
			}
		}
		maxErrors := 1000
		numErrors := len(c.evictionErrors)
		if numErrors > maxErrors {
			c.evictionErrors = c.evictionErrors[numErrors-maxErrors:]
		}
	} else {
		c.lock.Unlock()
	}

	c.evictionTime = time.Now()
}
