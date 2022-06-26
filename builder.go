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

func Builder(cacheDir string, targetSize Size) *builder {
	return &builder{cacheDir: cacheDir, targetSize: targetSize}
}

type builder struct {
	cacheDir           string
	targetSize         Size
	evictionConfigured bool
	evictionInterval   time.Duration
	fileMode           fs.FileMode
}

func (b *builder) WithEvictionInterval(evictionInterval time.Duration) *builder {
	b.evictionInterval = evictionInterval
	b.evictionConfigured = true
	return b
}

func (b *builder) Build() (Cache, error) {
	if b.targetSize <= 0 {
		return nil, fmt.Errorf("targetSize has to be > 0")
	}

	if !b.evictionConfigured {
		b.evictionInterval = 10 * time.Minute
	}

	err := os.MkdirAll(b.cacheDir, 0750)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("failed to create cacheDir at %s: %w", b.cacheDir, err)
	}
	writeTestPath := filepath.Join(b.cacheDir, "test")
	f, err := os.Create(writeTestPath)
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
	}
	err = c.loadEntries()
	if err != nil {
		return nil, fmt.Errorf("failed to restore cache: %w", err)
	}

	return c, nil
}
