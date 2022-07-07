# fcache
![coverage](https://img.shields.io/badge/coverage-91%25-green) [![go-report-card](https://goreportcard.com/badge/github.com/ydylla/fcache)](https://goreportcard.com/report/github.com/ydylla/fcache) [![reference](https://pkg.go.dev/badge/github.com/ydylla/fcache.svg)](https://pkg.go.dev/github.com/ydylla/fcache)

fcache is a file based persistent blob cache. It can be used to bring remote files closer to applications.


## Features
* Least recently used (LRU) eviction strategy
* Optional time to live (TTL) per entry
* Configurable size limit
* Request coalescing, to atomically query and insert entries and avoid [cache stampedes](https://en.wikipedia.org/wiki/Cache_stampede)
* Persistent: reloads cache state from disk after restart
* Usage statistics
* Bring your own key hashing algorithm (for example [xxHash](https://github.com/cespare/xxhash))


## Installation
`go get github.com/ydylla/fcache`


## Usage
To build a new cache use the [Builder](https://pkg.go.dev/github.com/ydylla/fcache#Builder):
```go
cache, err := fcache.Builder("/tmp/fcache", 10*fcache.GiB).Build()
```
The main functions are documented at the [Cache](https://pkg.go.dev/github.com/ydylla/fcache#Cache) interface.

## How it Works
Each cache entry is saved in its own file. On a successful get query the cache responds with a `io.ReadSeekCloser` backed by an `io.File`.
The caller is responsible for closing the file handle after he is done reading.
On each insert the old file is removed and a new one is created.
Eviction happens in background after each insert and only if the amount of time specified by the eviction interval has passed.


## Limitations
* This cache has only limited Windows support, since it assumes it is possible to delete files that are still open.  
  On Windows that is not the case. When you hold the `io.ReadSeekCloser` from a get query open over a long period of time and do a `Put` or `Delete` on the same key you will receive an error.

* If you save millions of small entries the actual storage usage will be higher than the configured limit, due to how most file systems work.


## Examples
### Simple example
This is a simple insert & query example.
```go
package main

import (
	"errors"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/ydylla/fcache"
	"io"
	"time"
)

func main() {
	// build a new cache
	cache, err := fcache.Builder("/tmp/fcache", 10*fcache.GiB).Build()
	if err != nil {
		fmt.Println("builder failed to initialize the cache:", err)
		return
	}

	// prepare test key and data
	key := xxhash.Sum64String("test")
	data := []byte("Hello World")

	// insert entry without expiration (ttl)
	info, err := cache.Put(key, data, 0)
	if err != nil {
		fmt.Println("insert failed:", err)
		return
	}

	fmt.Printf("Cache entry was modified at %s\n", info.Mtime.Format(time.RFC3339))

	// query the cache
	reader, info, err := cache.GetReader(key)
	if err != nil && !errors.Is(err, fcache.ErrNotFound) {
		fmt.Println("get failed for some reason:", err)
		return
	}
	defer reader.Close() // remember to close the reader

	buf, _ := io.ReadAll(reader)
	fmt.Printf("received '%s' form cache\n", buf)
}

```

### Coalescing example
This example demonstrates the usage of `GetReaderOrPut` which is used to ensure that the cache fill operation is only executed once.
```go
package main

import (
	"errors"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/ydylla/fcache"
	"io"
	"net/http"
)

// download downloads an url at most once, even if called concurrently
func download(cache fcache.Cache, url string) (reader io.ReadSeekCloser, info *fcache.EntryInfo, hit bool, err error) {
	// calculate the key
	key := xxhash.Sum64String(url)
	// atomically insert & query
	return cache.GetReaderOrPut(key, 0, fcache.FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
		fmt.Println("downloading", url)
		response, err := http.Get(url)
		if err != nil {
			return 0, err // abort insert
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			return 0, errors.New(response.Status)
		}
		// copy response body into cache & report how many bytes where written
		return io.Copy(sink, response.Body)
	}))
}

func main() {
	// build a new cache
	cache, err := fcache.Builder("/tmp/fcache", 10*fcache.GiB).Build()
	if err != nil {
		fmt.Println("builder failed to initialize the cache:", err)
		return
	}

	url := "https://example.org"

	reader, _, hit, err := download(cache, url)
	if err != nil {
		fmt.Println("download http request or cache query failed:", err)
		return
	}
	defer reader.Close() // remember to close the reader

	buf, _ := io.ReadAll(reader)
	fmt.Println("loaded", url, "from cache:", hit)
	fmt.Println(string(buf[:63]))
}
```