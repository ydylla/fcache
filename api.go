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
	WriteCacheData(key uint64, sink io.Writer) (written int64, err error)
}

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

type ReadSeekCloser interface {
	io.ReadSeekCloser
	io.ReaderAt
}

type EvictionError struct {
	Time  time.Time
	Error error
}

type Cache interface {
	Stats() Stats
	Has(key uint64) (info *EntryInfo, err error)
	Put(key uint64, val []byte, ttl time.Duration) (info *EntryInfo, err error)
	PutReader(key uint64, r io.Reader, ttl time.Duration) (info *EntryInfo, err error)
	Get(key uint64) (val []byte, info *EntryInfo, err error)
	GetReader(key uint64) (r ReadSeekCloser, info *EntryInfo, err error)
	GetOrPut(key uint64, ttl time.Duration, filler Filler) (data []byte, info *EntryInfo, hit bool, err error)
	GetReaderOrPut(key uint64, ttl time.Duration, filler Filler) (r ReadSeekCloser, info *EntryInfo, hit bool, err error)
	Delete(key uint64) (info *EntryInfo, err error)
}
