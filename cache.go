package fcache

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type cacheEntry struct {
	key      uint64
	sequence uint64
	size     int64
	mtime    int64
	expires  int64
}

func (e *cacheEntry) getExpires() time.Time {
	if e.expires <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(e.expires)
}

func (e *cacheEntry) getMtime() time.Time {
	return time.UnixMilli(e.mtime)
}

func (e *cacheEntry) isExpired() bool {
	return e.expires > 0 && time.Now().UnixMilli() > e.expires
}

func (e *cacheEntry) isValid() bool {
	return !e.isExpired()
}

func (e *cacheEntry) Info() *EntryInfo {
	info := &EntryInfo{Size: e.size, Mtime: time.UnixMilli(e.mtime)}
	if e.expires > 0 {
		info.Expires = time.UnixMilli(e.expires)
	}
	return info
}

type cache struct {
	cacheDir   string
	targetSize int64
	dirMode    os.FileMode
	fileMode   os.FileMode

	usedSize  int64
	has       atomic.Int64
	gets      atomic.Int64
	hits      atomic.Int64
	puts      atomic.Int64
	deletes   atomic.Int64
	evictions int64

	sequence    atomic.Uint64
	lock        sync.RWMutex
	entriesList *list.List
	entriesMap  map[uint64]*list.Element
	locker      *Locker

	evictionLock     sync.RWMutex
	evictionInterval time.Duration
	evictionTime     time.Time
	evictionDuration time.Duration
	evictionErrors   []EvictionError

	clearOrEvictDoingDeletes atomic.Int32
}

var _ Cache = (*cache)(nil)

func (c *cache) Stats() Stats {
	stats := Stats{
		Has:     c.has.Load(),
		Gets:    c.gets.Load(),
		Hits:    c.hits.Load(),
		Puts:    c.puts.Load(),
		Deletes: c.deletes.Load(),
		Locks:   c.locker.Size(),
	}
	c.lock.RLock()
	stats.Items = c.entriesList.Len()
	stats.Bytes = c.usedSize
	c.lock.RUnlock()

	c.evictionLock.RLock()
	stats.Evictions = c.evictions
	stats.EvictionTime = c.evictionTime
	stats.EvictionDuration = c.evictionDuration
	stats.EvictionErrors = c.evictionErrors
	c.evictionLock.RUnlock()

	return stats
}

func (c *cache) Has(key uint64) (*EntryInfo, error) {
	c.has.Add(1)

	c.lock.RLock()
	item, ok := c.entriesMap[key]
	c.lock.RUnlock()

	if ok {
		entry := item.Value.(*cacheEntry)
		c.locker.RLock(entry.key)
		defer c.locker.RUnlock(entry.key)

		if entry.isValid() {
			return entry.Info(), nil
		}
	}

	return nil, ErrNotFound
}

func (c *cache) Put(key uint64, val []byte, ttl time.Duration) (*EntryInfo, error) {
	return c.PutReader(key, bytes.NewReader(val), ttl)
}

func (c *cache) PutReader(key uint64, r io.Reader, ttl time.Duration) (*EntryInfo, error) {
	c.locker.Lock(key)
	defer c.locker.Unlock(key)

	entry, _, err := c.putEntry(key, ttl, false, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
		return io.Copy(sink, r)
	}))
	if err != nil {
		return nil, err
	}

	return entry.Info(), nil
}

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

func (c *cache) GetReader(key uint64) (io.ReadSeekCloser, *EntryInfo, error) {
	entry := c.getEntry(key)
	if entry == nil {
		return nil, nil, ErrNotFound
	}

	c.locker.RLock(key)
	defer c.locker.RUnlock(key)

	if !entry.isValid() {
		return nil, nil, ErrNotFound
	}

	c.hits.Add(1)

	f, err := os.Open(c.buildEntryPath(entry.key, entry.mtime, entry.expires, entry.sequence))
	if err != nil {
		// pretend the entry does not exist, its file was likely just removed by a clear or evict
		if c.clearOrEvictDoingDeletes.Load() > 0 && errors.Is(err, os.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	return f, entry.Info(), nil
}

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

func (c *cache) GetReaderOrPut(key uint64, ttl time.Duration, filler Filler) (r io.ReadSeekCloser, info *EntryInfo, hit bool, err error) {
retry:
	c.locker.RLock(key)
	entry := c.getEntry(key)
	if entry != nil && entry.isValid() {
		c.hits.Add(1)

		r, err = os.Open(c.buildEntryPath(entry.key, entry.mtime, entry.expires, entry.sequence))
		if err != nil {
			// the file was likely just removed by a clear or evict, so we ignore the err
			if c.clearOrEvictDoingDeletes.Load() > 0 && errors.Is(err, os.ErrNotExist) {
				c.hits.Add(-1) // don't count this as hit
				goto put
			}
			c.locker.RUnlock(key)
			return nil, nil, false, err
		}
		info = entry.Info()
		c.locker.RUnlock(key)
		return r, info, true, nil
	}
put:
	// entry does not exist or is expired, so we need to do a put.
	// Upgrade the read lock to a write lock, if this fails another goroutine is already doing a put.
	if !c.locker.Upgrade(key) {
		c.locker.RUnlock(key)
		goto retry
	}
	defer c.locker.Unlock(key)

	entry, r, err = c.putEntry(key, ttl, true, filler)
	if err != nil {
		return nil, nil, false, err
	}

	_, err = r.Seek(0, io.SeekStart)
	if err != nil {
		_ = r.Close()
		return nil, nil, false, err
	}

	return r, entry.Info(), false, nil
}

func (c *cache) Delete(key uint64) (*EntryInfo, error) {
	c.locker.Lock(key)
	defer c.locker.Unlock(key)

	c.deletes.Add(1)

	c.lock.RLock()
	item, ok := c.entriesMap[key]
	c.lock.RUnlock()
	if ok {
		entry := item.Value.(*cacheEntry)

		err := os.Remove(c.buildEntryPath(entry.key, entry.mtime, entry.expires, entry.sequence))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}

		c.lock.Lock()
		c.entriesList.Remove(item)
		delete(c.entriesMap, key)
		c.usedSize -= entry.size
		c.lock.Unlock()

		return entry.Info(), nil
	}

	return nil, nil
}

func (c *cache) Clear(resetStats bool) error {
	c.lock.Lock()
	deletedEntries := c.entriesMap
	c.deletes.Add(int64(len(c.entriesMap)))
	c.entriesMap = make(map[uint64]*list.Element)
	c.entriesList.Init()
	c.usedSize = 0
	c.lock.Unlock()

	if resetStats {
		c.has.Store(0)
		c.gets.Store(0)
		c.hits.Store(0)
		c.puts.Store(0)
		c.deletes.Store(0)
		c.evictionLock.Lock()
		c.evictions = 0
		c.evictionLock.Unlock()
	}

	// delete actual files in "background" so cache can be used again before Clear returns
	c.clearOrEvictDoingDeletes.Add(1)
	defer c.clearOrEvictDoingDeletes.Add(-1)

	delErrors := 0
	var delErr error
	for _, element := range deletedEntries {
		entry := element.Value.(*cacheEntry)
		err := os.Remove(c.buildEntryPath(entry.key, entry.mtime, entry.expires, entry.sequence))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			delErrors++
			delErr = err
		}
	}
	if delErr != nil {
		if delErrors > 1 {
			return fmt.Errorf("encountered %d errors during clearing. The last one was: %w", delErrors, delErr)
		} else {
			return delErr
		}
	}

	return nil
}

// createShardDirs creates all 36^2 possible shard dirs
func (c *cache) createShardDirs() error {
	for i := 0; i < 1296; i++ {
		_, shard := keyToShard(uint64(i))
		shardDir := filepath.Join(c.cacheDir, shard)
		err := os.MkdirAll(shardDir, c.dirMode)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cache) buildEntryPath(key uint64, mtime int64, expires int64, sequence uint64) string {
	shard, name := toFilename(key, mtime, expires, sequence)
	return filepath.Join(c.cacheDir, shard, name)
}

func (c *cache) getEntry(key uint64) *cacheEntry {
	c.gets.Add(1)

	c.lock.Lock()
	defer c.lock.Unlock()

	item, ok := c.entriesMap[key]
	if ok {
		entry := item.Value.(*cacheEntry)
		c.entriesList.MoveToFront(item)
		return entry
	}

	return nil
}

func keyToShard(key uint64) (keyStr string, shard string) {
	keyStr = strconv.FormatUint(key, 36)
	if len(keyStr) > 2 {
		return keyStr, keyStr[len(keyStr)-2:]
	} else {
		return keyStr, fmt.Sprintf("%02s", keyStr)
	}
}

func toFilename(key uint64, mtime int64, expires int64, sequence uint64) (string, string) {
	var expiresStr string
	if expires == 0 {
		expiresStr = "+"
	} else {
		expiresStr = strconv.FormatInt(expires, 36)
	}
	keyStr, shard := keyToShard(key)
	name := keyStr + "_" + strconv.FormatInt(mtime, 36) + "_" + expiresStr
	if sequence > 0 {
		name += "_" + strconv.FormatUint(sequence, 36)
	}
	return shard, name
}

func fromFilename(filename string) (key uint64, mtime int64, expires int64, sequence uint64, err error) {
	parts := strings.Split(filename, "_")
	if len(parts) != 3 && len(parts) != 4 {
		return key, mtime, expires, sequence, fmt.Errorf("%s should contain 4 underscore symbols but has %d", filename, len(parts))
	}

	key, err = strconv.ParseUint(parts[0], 36, 64)
	if err != nil {
		return key, mtime, expires, sequence, fmt.Errorf("failed to restore key from %s: %w", parts[0], err)
	}

	mtime, err = strconv.ParseInt(parts[1], 36, 64)
	if err != nil {
		return key, mtime, expires, sequence, fmt.Errorf("failed to restore mtime from %s: %w", parts[1], err)
	}

	if parts[2] != "+" {
		expires, err = strconv.ParseInt(parts[2], 36, 64)
		if err != nil {
			return key, mtime, expires, sequence, fmt.Errorf("failed to restore expires from %s: %w", parts[2], err)
		}
	}

	if len(parts) == 4 {
		sequence, err = strconv.ParseUint(parts[3], 36, 64)
		if err != nil {
			return key, mtime, expires, sequence, fmt.Errorf("failed to restore sequence from %s: %w", parts[3], err)
		}
	}

	return key, mtime, expires, sequence, nil
}

func (c *cache) putEntry(key uint64, ttl time.Duration, keepOpen bool, filler Filler) (entry *cacheEntry, file *os.File, err error) {
	c.puts.Add(1)

	mtime := time.Now().UnixMilli()
	var expires int64
	if ttl != 0 {
		expires = mtime + ttl.Milliseconds()
	}
	sequence := c.sequence.Add(1)

	newPath := c.buildEntryPath(key, mtime, expires, sequence)

	defer func() {
		if file != nil {
			if err != nil {
				// close & cleanup file but do not overwrite original error
				_ = file.Close()
				_ = os.Remove(newPath)
			} else {
				if !keepOpen {
					err = file.Close()
				}
			}
		}
	}()

	file, err = os.OpenFile(newPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, c.fileMode)
	if err != nil {
		return nil, file, err
	}

	var addedBytes int64
	addedBytes, err = filler.WriteCacheData(key, file)
	if err != nil {
		return nil, file, err
	}

	c.lock.RLock()
	item, exists := c.entriesMap[key]
	if exists {
		entry = item.Value.(*cacheEntry)
	} else {
		entry = &cacheEntry{key: key}
	}
	c.lock.RUnlock()

	if exists {
		oldPath := c.buildEntryPath(key, entry.mtime, entry.expires, entry.sequence)
		oldDelErr := os.Remove(oldPath)
		if oldDelErr != nil && !errors.Is(oldDelErr, os.ErrNotExist) {
			err = oldDelErr
			return nil, file, err
		}
	}

	c.lock.Lock()
	if exists {
		c.usedSize -= entry.size
		c.entriesList.MoveToFront(item)
	} else {
		c.entriesMap[key] = c.entriesList.PushFront(entry)
	}
	c.usedSize += addedBytes
	c.lock.Unlock()

	entry.size = addedBytes
	entry.mtime = mtime
	entry.expires = expires
	entry.sequence = sequence

	// do eviction in background after put
	go c.evict()

	return entry, file, nil
}

func readDirWithoutSort(name string) ([]os.DirEntry, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	dirs, err := f.ReadDir(-1)
	_ = f.Close()

	return dirs, err
}

func (c *cache) loadEntries() error {
	shardDirs, err := readDirWithoutSort(c.cacheDir)
	if err != nil {
		return err
	}

	for _, shardDir := range shardDirs {
		if shardDir.IsDir() {
			shardDirPath := filepath.Join(c.cacheDir, shardDir.Name())
			entries, err := readDirWithoutSort(shardDirPath)
			if err != nil {
				return err
			}

			for _, entry := range entries {
				if !entry.IsDir() {
					path := filepath.Join(shardDirPath, entry.Name())
					key, mtime, expires, sequence, err := fromFilename(entry.Name())
					if err != nil {
						return fmt.Errorf("failed to restore meta from %s: %w", path, err)
					}

					info, err := entry.Info()
					if err != nil {
						return fmt.Errorf("failed to restore size from %s: %w", path, err)
					}

					ce := &cacheEntry{key: key, sequence: sequence, size: info.Size(), mtime: mtime, expires: expires}

					c.lock.Lock()
					existing, exists := c.entriesMap[ce.key]
					if exists {
						c.lock.Unlock()
						existingEntry := existing.Value.(*cacheEntry)
						newPath := c.buildEntryPath(existingEntry.key, existingEntry.mtime, existingEntry.expires, existingEntry.sequence)
						if path != newPath {
							// Remove old file in case the key was written to by a normal put during loading.
							delErr := os.Remove(path)
							if delErr != nil && !errors.Is(delErr, os.ErrNotExist) {
								err = delErr
							}
						}
						if err != nil {
							return fmt.Errorf("failed to remove overwritten entry at %s: %w", path, err)
						}
					} else {
						c.entriesMap[ce.key] = c.entriesList.PushFront(ce)
						c.usedSize += ce.size
						c.lock.Unlock()
					}
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

	c.lock.RLock()

	start := time.Now()

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

		c.lock.RUnlock()
		c.lock.Lock()

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

		c.clearOrEvictDoingDeletes.Add(1)
		defer c.clearOrEvictDoingDeletes.Add(-1)

		for _, entry := range deleted {
			err := os.Remove(c.buildEntryPath(entry.key, entry.mtime, entry.expires, entry.sequence))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				c.evictionErrors = append(c.evictionErrors, EvictionError{
					Time:  time.Now(),
					Error: err,
				})
				if len(c.evictionErrors) > 1000 {
					c.evictionErrors = c.evictionErrors[1:]
				}
			}
		}
	} else {
		c.lock.RUnlock()
	}

	c.evictionTime = time.Now()
	c.evictionDuration = time.Since(start)
}
