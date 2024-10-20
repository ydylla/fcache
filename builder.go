package fcache

import (
	"container/list"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Builder configures & builds a new cache.
// cacheDir is the base directory of the cache.
// targetSize is target size of the cache. Depending on the eviction interval and insert load it may grow larger.
func Builder(cacheDir string, targetSize Size) *builder {
	return &builder{cacheDir: cacheDir, targetSize: targetSize}
}

type builder struct {
	cacheDir           string
	targetSize         Size
	evictionConfigured bool
	evictionInterval   time.Duration
	fileMode           fs.FileMode
	backgroundInit     bool
	initCallback       InitCallback
}

// WithEvictionInterval configures how much time has to pass between evictions.
// By default its 10 minutes.
func (b *builder) WithEvictionInterval(evictionInterval time.Duration) *builder {
	b.evictionInterval = evictionInterval
	b.evictionConfigured = true
	return b
}

// WithFileMode configures which file mode is used for the cache files.
// By default 0600 is used.
// Remember to also check your umask when you change this.
func (b *builder) WithFileMode(perm fs.FileMode) *builder {
	b.fileMode = perm
	return b
}

// InitCallback is called after the cache finished restoring all entries from disk.
// cache is the Cache that was doing the init.
// err indicates if the init was successful or not.
type InitCallback func(cache Cache, err error)

// WithBackgroundInit restores the cache state in background instead of in Build.
// If initCallback is not nil it will be called once when all cache entries are restored or an error occurred.
func (b *builder) WithBackgroundInit(initCallback InitCallback) *builder {
	b.backgroundInit = true
	b.initCallback = initCallback
	return b
}

// Build initializes the cache and also loads the state of existing entries from disk.
func (b *builder) Build() (Cache, error) {
	if b.targetSize <= 0 {
		return nil, errors.New("targetSize has to be > 0")
	}

	if !b.evictionConfigured {
		b.evictionInterval = 10 * time.Minute
	}

	if b.fileMode == 0 {
		b.fileMode = 0600
	} else {
		if b.fileMode != (b.fileMode | 0600) {
			return nil, errors.New("fileMode has to be at least 0600")
		}
	}
	dirMode := b.fileMode | 0700

	err := os.MkdirAll(b.cacheDir, dirMode)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("failed to create cacheDir at %s: %w", b.cacheDir, err)
	}
	writeTestPath := filepath.Join(b.cacheDir, "test")
	f, err := os.OpenFile(writeTestPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, b.fileMode)
	if err != nil {
		return nil, fmt.Errorf("failed to create test file %s: %w", writeTestPath, err)
	}
	_, err = f.WriteString("test")
	if err != nil {
		return nil, fmt.Errorf("failed to write test file %s: %w", writeTestPath, err)
	}
	err = f.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close test file %s: %w", writeTestPath, err)
	}
	err = os.Remove(writeTestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to remove test file %s: %w", writeTestPath, err)
	}

	c := &cache{
		cacheDir:         b.cacheDir,
		targetSize:       int64(b.targetSize),
		entriesList:      list.New(),
		entriesMap:       map[uint64]*list.Element{},
		evictionInterval: b.evictionInterval,
		dirMode:          dirMode,
		fileMode:         b.fileMode,
		locker:           NewLocker(),
	}

	err = c.createShardDirs()
	if err != nil {
		return nil, fmt.Errorf("failed to create shard dirs: %w", err)
	}

	if b.backgroundInit {
		go func() {
			err = c.loadEntries()
			if b.initCallback != nil {
				if err != nil {
					err = fmt.Errorf("failed to restore cache: %w", err)
				}
				b.initCallback(c, err)
			}
		}()
	} else {
		err = c.loadEntries()
		if err != nil {
			return nil, fmt.Errorf("failed to restore cache: %w", err)
		}
	}

	return c, nil
}
