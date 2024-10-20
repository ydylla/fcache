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
	dirMode    fs.FileMode
	fileMode   fs.FileMode

	usedSize  int64
	has       int64
	gets      int64
	hits      int64
	puts      int64
	deletes   int64
	evictions int64

	sequence    atomic.Uint64
	lock        sync.RWMutex
	entriesList *list.List
	entriesMap  map[uint64]*list.Element
	locker      *Locker

	evictionLock     sync.RWMutex
	evictionInterval time.Duration
	evictionTime     time.Time
	evictionErrors   []EvictionError
}

var _ Cache = (*cache)(nil)

func (c *cache) Stats() Stats {
	c.lock.Lock()
	defer c.lock.Unlock()

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
		Locks:          c.locker.Size(),
	}
}

func (c *cache) Has(key uint64) (*EntryInfo, error) {
	c.lock.Lock()
	item, ok := c.entriesMap[key]
	c.has += 1
	c.lock.Unlock()

	if ok {
		entry := item.Value.(*cacheEntry)
		c.locker.RLock(entry.key)
		defer c.locker.RUnlock(entry.key)

		if entry.isValid() {
			return &EntryInfo{Size: entry.size, Mtime: entry.getMtime(), Expires: entry.getExpires()}, nil
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

	mtime, expires, sequence, newPath := c.preparePut(key, ttl)

	addedBytes, err := c.writeEntry(key, newPath, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
		return io.Copy(sink, r)
	}))
	if err != nil {
		return nil, err
	}

	entry, err := c.finalizePut(key, mtime, expires, sequence, addedBytes)
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
	c.locker.RLock(key)
	defer c.locker.RUnlock(key)

	entry := c.getEntry(key)
	if entry == nil {
		return nil, nil, ErrNotFound
	}

	if !entry.isValid() {
		return nil, nil, ErrNotFound
	}

	c.lock.Lock()
	c.hits += 1
	c.lock.Unlock()

	f, err := os.Open(c.buildEntryPath(entry.key, entry.mtime, entry.expires, entry.sequence))
	if err != nil {
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
		c.hits += 1
		// we are sure entry exits & is valid,
		// so we can continue like a normal get
		defer c.locker.RUnlock(key)

		r, err = os.Open(c.buildEntryPath(entry.key, entry.mtime, entry.expires, entry.sequence))
		if err != nil {
			return nil, nil, false, err
		}

		return r, entry.Info(), true, nil
	}

	// entry does not exist or is expired, so we need to do a put.
	// Upgrade the read lock to a write lock, if this fails another goroutine is already doing a put.
	if !c.locker.Upgrade(key) {
		c.locker.RUnlock(key)
		goto retry
	}
	defer c.locker.Unlock(key)

	mtime, expires, sequence, newPath := c.preparePut(key, ttl)

	addedBytes, err := c.writeEntry(key, newPath, filler)
	if err != nil {
		return nil, nil, false, err
	}
	entry, err = c.finalizePut(key, mtime, expires, sequence, addedBytes)
	if err != nil {
		return nil, nil, false, err
	}

	r, err = os.Open(newPath)
	if err != nil {
		return nil, nil, false, err
	}

	return r, entry.Info(), false, nil
}

func (c *cache) Delete(key uint64) (*EntryInfo, error) {
	c.locker.Lock(key)
	defer c.locker.Unlock(key)

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
		err := os.Remove(c.buildEntryPath(entry.key, entry.mtime, entry.expires, entry.sequence))
		if err != nil {
			return nil, err
		}
		return entry.Info(), nil
	}

	return nil, nil
}

func (c *cache) Clear(resetStats bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.deletes += int64(len(c.entriesMap))

	c.entriesMap = make(map[uint64]*list.Element)
	c.entriesList.Init()

	err := os.RemoveAll(c.cacheDir)
	if err != nil {
		return err
	}
	err = c.createShardDirs()
	if err != nil {
		return err
	}

	c.usedSize = 0

	if resetStats {
		c.has = 0
		c.gets = 0
		c.hits = 0
		c.puts = 0
		c.deletes = 0
		c.evictions = 0
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
	c.lock.Lock()
	defer c.lock.Unlock()

	c.gets += 1

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

func (c *cache) preparePut(key uint64, ttl time.Duration) (mtime int64, expires int64, sequence uint64, newPath string) {
	mtime = time.Now().UnixMilli()
	if ttl != 0 {
		expires = mtime + ttl.Milliseconds()
	}
	sequence = c.sequence.Add(1)
	newPath = c.buildEntryPath(key, mtime, expires, sequence)
	return mtime, expires, sequence, newPath
}

func (c *cache) writeEntry(key uint64, newPath string, filler Filler) (addedBytes int64, err error) {
	var f *os.File
	defer func() {
		if f != nil {
			if err != nil {
				// close & cleanup file but do not overwrite original error
				_ = f.Close()
				_ = os.Remove(newPath)
			} else {
				err = f.Close()
			}
		}
	}()

	f, err = os.OpenFile(newPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, c.fileMode)
	if err != nil {
		return addedBytes, err
	}

	addedBytes, err = filler.WriteCacheData(key, f)
	if err != nil {
		return addedBytes, err
	}

	return addedBytes, nil
}

// finalizePut removes the old file, inserts the new entry and updates the cache stats.
// It must be called with a locked entry.
// It also triggers eviction.
func (c *cache) finalizePut(key uint64, mtime int64, expires int64, sequence uint64, addedBytes int64) (entry *cacheEntry, err error) {
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
		err := os.Remove(oldPath)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			// cleanup new just created entry
			_ = os.Remove(c.buildEntryPath(key, mtime, expires, sequence))
			return nil, err
		}
	}
	entry.mtime = mtime
	entry.expires = expires
	entry.sequence = sequence

	c.lock.Lock()

	c.puts += 1

	if exists {
		c.usedSize -= entry.size
		c.entriesList.MoveToFront(item)
	} else {
		c.entriesMap[entry.key] = c.entriesList.PushFront(entry)
	}

	entry.size = addedBytes
	c.usedSize += addedBytes

	c.lock.Unlock()

	// do eviction in background after put
	go c.evict()

	return entry, nil
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
							err = os.Remove(path)
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

		for _, entry := range deleted {
			err := os.Remove(c.buildEntryPath(entry.key, entry.mtime, entry.expires, entry.sequence))
			if err != nil {
				c.lock.Lock()
				c.evictionErrors = append(c.evictionErrors, EvictionError{
					Time:  time.Now(),
					Error: err,
				})
				if len(c.evictionErrors) > 1000 {
					c.evictionErrors = c.evictionErrors[1:]
				}
				c.lock.Unlock()
			}
		}
	} else {
		c.lock.RUnlock()
	}

	c.evictionTime = time.Now()
}
