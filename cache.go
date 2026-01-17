package fcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type cache struct {
	cacheDir   string
	targetSize int64
	dirMode    os.FileMode
	fileMode   os.FileMode
	random     *rand.Rand

	usedSize  int64
	has       atomic.Int64
	gets      atomic.Int64
	hits      atomic.Int64
	puts      atomic.Int64
	deletes   atomic.Int64
	evictions int64

	sequence  atomic.Uint64
	lock      sync.RWMutex
	keyToIdx  map[uint64]int
	keys      []uint64
	sequences []uint64
	sizes     []int64
	mtimes    []int64
	expires   []int64
	locker    *Locker

	evictionLock              sync.RWMutex
	evictionInterval          time.Duration
	evictionTime              time.Time
	evictionDuration          time.Duration
	evictionReadLockDuration  time.Duration
	evictionWriteLockDuration time.Duration
	evictionErrors            []EvictionError

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
	stats.Items = len(c.keys)
	stats.Bytes = c.usedSize
	c.lock.RUnlock()

	c.evictionLock.RLock()
	stats.Evictions = c.evictions
	stats.EvictionTime = c.evictionTime
	stats.EvictionDuration = c.evictionDuration
	stats.EvictionReadLockDuration = c.evictionReadLockDuration
	stats.EvictionWriteLockDuration = c.evictionWriteLockDuration
	stats.EvictionErrors = c.evictionErrors
	c.evictionLock.RUnlock()

	return stats
}

func isValid(expires int64) bool {
	return expires == 0 || time.Now().UnixMilli() < expires
}

func buildInfo(size int64, mtime int64, expires int64) *EntryInfo {
	info := &EntryInfo{Size: size, Mtime: time.UnixMilli(mtime)}
	if expires > 0 {
		info.Expires = time.UnixMilli(expires)
	}
	return info
}

func (c *cache) Has(key uint64) (*EntryInfo, error) {
	c.locker.RLock(key)
	defer c.locker.RUnlock(key)

	c.has.Add(1)

	c.lock.RLock()
	_, size, mtime, expires, ok := c.lookup(key)
	c.lock.RUnlock()

	if ok && isValid(expires) {
		return buildInfo(size, mtime, expires), nil
	}

	return nil, ErrNotFound
}

func (c *cache) Put(key uint64, val []byte, ttl time.Duration) (*EntryInfo, error) {
	return c.PutReader(key, bytes.NewReader(val), ttl)
}

func (c *cache) PutReader(key uint64, r io.Reader, ttl time.Duration) (*EntryInfo, error) {
	c.locker.Lock(key)
	defer c.locker.Unlock(key)

	_, size, mtime, expires, _, err := c.put(key, ttl, false, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
		return io.Copy(sink, r)
	}))
	if err != nil {
		return nil, err
	}

	return buildInfo(size, mtime, expires), nil
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

	sequence, size, mtime, expires, ok := c.get(key)
	if !ok {
		return nil, nil, ErrNotFound
	}

	f, err := os.Open(c.buildEntryPath(key, mtime, expires, sequence))
	if err != nil {
		// pretend the entry does not exist, its file was likely just removed by a clear or evict
		if c.clearOrEvictDoingDeletes.Load() > 0 && errors.Is(err, os.ErrNotExist) {
			c.hits.Add(-1) // don't count this as hit
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	return f, buildInfo(size, mtime, expires), nil
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
	sequence, size, mtime, expires, ok := c.get(key)
	if ok {
		r, err = os.Open(c.buildEntryPath(key, mtime, expires, sequence))
		if err != nil {
			// the file was likely just removed by a clear or evict, so we ignore the err
			if c.clearOrEvictDoingDeletes.Load() > 0 && errors.Is(err, os.ErrNotExist) {
				c.hits.Add(-1) // don't count this as hit
				goto put
			}
			c.locker.RUnlock(key)
			return nil, nil, false, err
		}
		c.locker.RUnlock(key)
		return r, buildInfo(size, mtime, expires), true, nil
	}
put:
	// entry does not exist or is expired, so we need to do a put.
	// Upgrade the read lock to a write lock, if this fails another goroutine is already doing a put.
	if !c.locker.Upgrade(key) {
		c.locker.RUnlock(key)
		goto retry
	}
	defer c.locker.Unlock(key)

	sequence, size, mtime, expires, r, err = c.put(key, ttl, true, filler)
	if err != nil {
		return nil, nil, false, err
	}

	_, err = r.Seek(0, io.SeekStart)
	if err != nil {
		_ = r.Close()
		return nil, nil, false, err
	}

	return r, buildInfo(size, mtime, expires), false, nil
}

func (c *cache) Delete(key uint64) (*EntryInfo, error) {
	c.locker.Lock(key)
	defer c.locker.Unlock(key)

	c.deletes.Add(1)

	c.lock.RLock()
	sequence, size, mtime, expires, ok := c.lookup(key)
	c.lock.RUnlock()
	if ok {
		err := os.Remove(c.buildEntryPath(key, mtime, expires, sequence))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}

		c.lock.Lock()
		c.remove(key)
		c.lock.Unlock()

		return buildInfo(size, mtime, expires), nil
	}

	return nil, nil
}

func (c *cache) Clear(resetStats bool) error {
	c.lock.Lock()
	deletedEntries := c.keyToIdx
	deletedMtimes := c.mtimes
	deleteExpires := c.expires
	deletedSequences := c.sequences
	c.deletes.Add(int64(len(c.keys)))
	c.keyToIdx = make(map[uint64]int)
	c.keys = make([]uint64, 0)
	c.sequences = make([]uint64, 0)
	c.sizes = make([]int64, 0)
	c.mtimes = make([]int64, 0)
	c.expires = make([]int64, 0)
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
	for key, idx := range deletedEntries {
		err := os.Remove(c.buildEntryPath(key, deletedMtimes[idx], deleteExpires[idx], deletedSequences[idx]))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			delErrors++
			delErr = err
		}
	}
	if delErr != nil {
		if delErrors > 1 {
			return fmt.Errorf("encountered %d errors during clearing. The last one was: %w", delErrors, delErr)
		}
		return delErr
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

// move key to the top while keeping the current top in the top half
func (c *cache) moveToFront(key uint64) {
	idx := c.keyToIdx[key]
	topKey := c.keys[0]

	// if only 3 items or current position is in top half just swap top and current
	if len(c.keys) < 4 || idx <= len(c.keys)/2 {
		c.keyToIdx[topKey] = idx
		c.keyToIdx[key] = 0

		c.keys[0] = key
		c.keys[idx] = topKey

		sequence := c.sequences[idx]
		c.sequences[idx] = c.sequences[0]
		c.sequences[0] = sequence

		size := c.sizes[idx]
		c.sizes[idx] = c.sizes[0]
		c.sizes[0] = size

		mtime := c.mtimes[idx]
		c.mtimes[idx] = c.mtimes[0]
		c.mtimes[0] = mtime

		expires := c.expires[idx]
		c.expires[idx] = c.expires[0]
		c.expires[0] = expires

	} else { // move random top half index to current and top to random index
		randomIdx := c.random.IntN(max(1, len(c.keys)/2-1)) + 1
		randomKey := c.keys[randomIdx]

		c.keyToIdx[randomKey] = idx
		c.keyToIdx[topKey] = randomIdx
		c.keyToIdx[key] = 0

		c.keys[randomIdx] = topKey
		c.keys[0] = key
		c.keys[idx] = randomKey

		sequence := c.sequences[randomIdx]
		c.sequences[randomIdx] = c.sequences[0]
		c.sequences[0] = c.sequences[idx]
		c.sequences[idx] = sequence

		size := c.sizes[randomIdx]
		c.sizes[randomIdx] = c.sizes[0]
		c.sizes[0] = c.sizes[idx]
		c.sizes[idx] = size

		mtime := c.mtimes[randomIdx]
		c.mtimes[randomIdx] = c.mtimes[0]
		c.mtimes[0] = c.mtimes[idx]
		c.mtimes[idx] = mtime

		expires := c.expires[randomIdx]
		c.expires[randomIdx] = c.expires[0]
		c.expires[0] = c.expires[idx]
		c.expires[idx] = expires
	}
}

func (c *cache) remove(key uint64) (idx int, sequence uint64, size int64, mtime int64, expires int64, removed bool) {
	idx, removed = c.keyToIdx[key]
	if removed {
		sequence = c.sequences[idx]
		size = c.sizes[idx]
		mtime = c.mtimes[idx]
		expires = c.expires[idx]

		c.usedSize -= c.sizes[idx]

		lastIdx := len(c.keys) - 1
		c.sequences[idx] = c.sequences[lastIdx]
		c.sizes[idx] = c.sizes[lastIdx]
		c.mtimes[idx] = c.mtimes[lastIdx]
		c.expires[idx] = c.expires[lastIdx]

		lastKey := c.keys[lastIdx]
		c.keyToIdx[lastKey] = idx
		c.keys[idx] = lastKey

		delete(c.keyToIdx, key)
		c.keys = c.keys[:lastIdx]
		c.sequences = c.sequences[:lastIdx]
		c.sizes = c.sizes[:lastIdx]
		c.mtimes = c.mtimes[:lastIdx]
		c.expires = c.expires[:lastIdx]
	}
	return
}

func (c *cache) append(key uint64, sequence uint64, size int64, mtime int64, expires int64) {
	idx := len(c.keys)
	c.keyToIdx[key] = idx
	c.keys = append(c.keys, key)
	c.sequences = append(c.sequences, sequence)
	c.sizes = append(c.sizes, size)
	c.mtimes = append(c.mtimes, mtime)
	c.expires = append(c.expires, expires)

	c.usedSize += size
}

func (c *cache) lookup(key uint64) (sequence uint64, size int64, mtime int64, expires int64, ok bool) {
	var idx int
	idx, ok = c.keyToIdx[key]
	if ok {
		sequence = c.sequences[idx]
		size = c.sizes[idx]
		mtime = c.mtimes[idx]
		expires = c.expires[idx]
	}
	return
}

func (c *cache) get(key uint64) (sequence uint64, size int64, mtime int64, expires int64, ok bool) {
	c.gets.Add(1)

	c.lock.Lock()
	defer c.lock.Unlock()

	sequence, size, mtime, expires, ok = c.lookup(key)
	if ok {
		ok = isValid(expires)
		if ok {
			c.hits.Add(1)
			c.moveToFront(key)
		}
	}
	return
}

func keyToShard(key uint64) (keyStr string, shard string) {
	keyStr = strconv.FormatUint(key, 36)
	if len(keyStr) > 2 {
		return keyStr, keyStr[len(keyStr)-2:]
	}
	return keyStr, fmt.Sprintf("%02s", keyStr)
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

func (c *cache) put(key uint64, ttl time.Duration, keepOpen bool, filler Filler) (sequence uint64, size int64, mtime int64, expires int64, file *os.File, err error) {
	c.puts.Add(1)

	mtime = time.Now().UnixMilli()
	if ttl != 0 {
		expires = mtime + ttl.Milliseconds()
	}
	sequence = c.sequence.Add(1)

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
		return
	}

	size, err = filler.WriteCacheData(key, file)
	if err != nil {
		return
	}

	c.lock.RLock()
	existingSequence, existingSize, existingMtime, existingExpires, exists := c.lookup(key)
	c.lock.RUnlock()
	if exists {
		oldPath := c.buildEntryPath(key, existingMtime, existingExpires, existingSequence)
		oldDelErr := os.Remove(oldPath)
		if oldDelErr != nil && !errors.Is(oldDelErr, os.ErrNotExist) {
			err = oldDelErr
			return
		}
	}

	c.lock.Lock()
	if exists {
		// lookup idx after c.lock.Lock() because it could have been swapped between now and c.lookup() above
		idx := c.keyToIdx[key]
		c.sequences[idx] = sequence
		c.sizes[idx] = size
		c.mtimes[idx] = mtime
		c.expires[idx] = expires

		c.usedSize -= existingSize
		c.usedSize += size
	} else {
		c.append(key, sequence, size, mtime, expires)
	}
	c.moveToFront(key)
	c.lock.Unlock()

	// do eviction in background after put
	go c.evict()

	return sequence, size, mtime, expires, file, nil
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

func (c *cache) loadEntries() []error {
	shardDirs, err := readDirWithoutSort(c.cacheDir)
	if err != nil {
		return []error{err}
	}

	workers := runtime.NumCPU()
	wg := sync.WaitGroup{}
	queue := make(chan string, workers)
	errsChan := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for shardDirPath := range queue {
				entries, err := readDirWithoutSort(shardDirPath)
				if err != nil {
					errsChan <- err
					continue
				}

				for _, entry := range entries {
					if !entry.IsDir() {
						path := filepath.Join(shardDirPath, entry.Name())

						key, mtime, expires, sequence, err := fromFilename(entry.Name())
						if err != nil {
							errsChan <- fmt.Errorf("failed to restore meta from %s: %w", path, err)
							continue
						}

						info, err := entry.Info()
						if err != nil {
							errsChan <- fmt.Errorf("failed to restore size from %s: %w", path, err)
							continue
						}
						size := info.Size()

						c.lock.Lock()
						existingSequence, _, existingMtime, existingExpires, exists := c.lookup(key)
						if exists {
							c.lock.Unlock()
							existingPath := c.buildEntryPath(key, existingMtime, existingExpires, existingSequence)
							if path != existingPath {
								// Remove old file in case the key was written to by a normal put during loading.
								delErr := os.Remove(path)
								if delErr != nil && !errors.Is(delErr, os.ErrNotExist) {
									err = delErr
								}
							}
							if err != nil {
								errsChan <- fmt.Errorf("failed to remove overwritten entry at %s: %w", path, err)
							}
						} else {
							c.append(key, sequence, size, mtime, expires)
							c.lock.Unlock()
						}
					}
				}
			}
		}()
	}

	go func() {
		for _, shardDir := range shardDirs {
			if shardDir.IsDir() {
				shardDirPath := filepath.Join(c.cacheDir, shardDir.Name())
				queue <- shardDirPath
			}
		}
		close(queue)
	}()

	go func() {
		wg.Wait()
		close(errsChan)
	}()

	var errs []error
	for err := range errsChan {
		errs = append(errs, err)
	}

	return errs
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

		expired := make([]uint64, 0)
		expiredSize := int64(0)

		candidates := make([]uint64, 0)
		candidatesSize := int64(0)

		for i := len(c.keys) - 1; i >= 0; i-- {
			key := c.keys[i]
			size := c.sizes[i]
			expires := c.expires[i]
			if !isValid(expires) {
				expired = append(expired, key)
				expiredSize += size
			} else {
				if c.usedSize-(expiredSize+candidatesSize) > c.targetSize {
					candidates = append(candidates, key)
					candidatesSize += size
				}
			}

			if c.usedSize-expiredSize < c.targetSize {
				break
			}
		}

		c.lock.RUnlock()
		c.evictionReadLockDuration = time.Since(start)
		writeStart := time.Now()
		c.lock.Lock()

		var filesToDelete []string

		// remove all expired
		for _, key := range expired {
			_, sequence, _, mtime, expires, removed := c.remove(key)
			if removed {
				c.evictions += 1

				filesToDelete = append(filesToDelete, c.buildEntryPath(key, mtime, expires, sequence))
			}
		}

		// if target size is still not reached remove lru candidates until it is
		for _, key := range candidates {
			if c.usedSize < c.targetSize {
				break
			}
			_, sequence, _, mtime, expires, removed := c.remove(key)
			if removed {
				c.evictions += 1

				filesToDelete = append(filesToDelete, c.buildEntryPath(key, mtime, expires, sequence))
			}
		}

		// we can release the cache lock now and delete the actual files in "background"
		c.lock.Unlock()
		c.evictionWriteLockDuration = time.Since(writeStart)

		c.clearOrEvictDoingDeletes.Add(1)
		defer c.clearOrEvictDoingDeletes.Add(-1)

		for _, file := range filesToDelete {
			err := os.Remove(file)
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
		c.evictionReadLockDuration = time.Since(start)
		c.evictionWriteLockDuration = 0
	}

	c.evictionTime = time.Now()
	c.evictionDuration = time.Since(start)
}
