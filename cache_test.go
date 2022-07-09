package fcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var DATA = []byte("TEST-TEST-TEST-TEST-TEST-TEST-TEST-TEST-TEST-TEST-TEST-TEST")

type ReaderFunc func(p []byte) (n int, err error)

func (f ReaderFunc) Read(p []byte) (n int, err error) {
	return f(p)
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected error: %s\n", err)
	}
}

func assertString(t *testing.T, expected string, value string) {
	t.Helper()
	if value != expected {
		t.Fatalf("Expected '%s' but got '%s'\n", expected, value)
	}
}

func assertTime(t *testing.T, expected time.Time, value time.Time) {
	t.Helper()
	if value != expected {
		t.Fatalf("Expected '%s' but got '%s'\n", expected, value)
	}
}

func assertStruct(t *testing.T, expected interface{}, value interface{}) {
	t.Helper()
	expectedStr := fmt.Sprintf("%+v", expected)
	valueStr := fmt.Sprintf("%+v", value)
	if valueStr != expectedStr {
		t.Fatalf("Expected '%s' but got '%s'\n", expectedStr, valueStr)
	}
}

func assertReaderBytes(t *testing.T, expected []byte, r io.ReadCloser) {
	t.Helper()
	defer func() {
		_ = r.Close()
	}()
	buf := bytes.Buffer{}
	_, err := buf.ReadFrom(r)
	assertNoError(t, err)

	if !bytes.Equal(buf.Bytes(), expected) {
		t.Fatalf("Expected '%s' but got '%s'\n", expected, buf.Bytes())
	}
}

func readFileForKey(t *testing.T, dir string, key uint64, mtime time.Time, expires time.Time) (data []byte, path string) {
	t.Helper()
	shard, name := toFilename(key, mtime, expires)
	path = filepath.Join(dir, shard, name)
	data, err := os.ReadFile(path)
	assertNoError(t, err)
	return data, path
}

func TestFileCache_toFilename(t *testing.T) {
	mtime := time.Unix(1644519455, 0)
	expires := time.Unix(1644519465, 0)

	shard1, name1 := toFilename(1, mtime, expires)
	assertString(t, "01", shard1)
	assertString(t, "1_kzhcf8k8_kzhcfga0", name1)

	shard2, name2 := toFilename(123, mtime, time.Time{})
	assertString(t, "3f", shard2)
	assertString(t, "3f_kzhcf8k8_+", name2)
}

func TestFileCache_fromFilename(t *testing.T) {
	key1, mtime1, expires1, err1 := fromFilename("1_kzhcf8k8_kzhcfga0")
	assertNoError(t, err1)
	if key1 != 1 {
		t.Fatal("Expected 1 but got:", key1)
	}
	assertTime(t, time.Unix(1644519455, 0), mtime1)
	assertTime(t, time.Unix(1644519465, 0), expires1)

	key2, mtime2, expires2, err2 := fromFilename("3f_kzhcf8k8_+")
	assertNoError(t, err2)
	if key2 != 123 {
		t.Fatal("Expected 123 but got:", key2)
	}
	assertTime(t, time.Unix(1644519455, 0), mtime2)
	assertTime(t, time.Time{}, expires2)
}

func TestFileCache_Put(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 50*MB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	start := time.Now().Add(-time.Millisecond)

	e1, err := c.Put(uint64(1), DATA, 0)
	assertNoError(t, err)
	if e1.Size != 59 {
		t.Fatal("Expected size 59 for e1 but got", e1.Size)
	}
	if !(e1.Mtime.After(start) && e1.Mtime.Before(time.Now().Add(time.Millisecond))) {
		t.Fatal("Unexpected mtime for e1", e1.Mtime)
	}
	assertTime(t, time.Time{}, e1.Expires)

	e2, err := c.Put(uint64(2), DATA, -1)
	assertNoError(t, err)
	if e2.Size != 59 {
		t.Fatal("Expected size 59 for e2 but got", e2.Size)
	}
	if !(e2.Mtime.After(start) && e2.Mtime.Before(time.Now().Add(time.Millisecond))) {
		t.Fatal("Unexpected mtime for e2", e2.Mtime)
	}
	assertTime(t, time.Time{}, e2.Expires)

	e3, err := c.PutReader(uint64(3), bytes.NewReader(DATA), 10*time.Minute)
	assertNoError(t, err)
	if e3.Size != 59 {
		t.Fatal("Expected size 59 for e3 but got", e3.Size)
	}
	if !(e3.Mtime.After(start) && e3.Mtime.Before(time.Now().Add(time.Millisecond))) {
		t.Fatal("Unexpected mtime for e3", e3.Mtime)
	}
	assertTime(t, e3.Mtime.Add(10*time.Minute), e3.Expires)

	assertStruct(t, Stats{
		Items:          3,
		Bytes:          177,
		Has:            0,
		Gets:           0,
		Hits:           0,
		Puts:           3,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())

	data1, path1 := readFileForKey(t, dir, 1, e1.Mtime, e1.Expires)
	if !bytes.Equal(data1, DATA) {
		t.Fatal("Expected data for e1 got", data1)
	}

	data2, _ := readFileForKey(t, dir, 2, e2.Mtime, e2.Expires)
	if !bytes.Equal(data2, DATA) {
		t.Fatal("Expected data for e2 got", data2)
	}

	data3, _ := readFileForKey(t, dir, 3, e3.Mtime, e3.Expires)
	if !bytes.Equal(data3, DATA) {
		t.Fatal("Expected data for e3 got", data3)
	}

	newData := []byte("DUMMY")
	updated1, err := c.Put(1, newData, 123*time.Minute)
	assertNoError(t, err)
	if updated1.Size != 5 {
		t.Fatal("Expected size 5 for updated1 but got", updated1.Size)
	}
	if !(updated1.Mtime.After(start) && updated1.Mtime.Before(time.Now().Add(time.Millisecond))) {
		t.Fatal("Unexpected mtime for updated1", updated1.Mtime)
	}
	assertTime(t, updated1.Mtime.Add(123*time.Minute), updated1.Expires)

	updatedData, updatedPath := readFileForKey(t, dir, 1, updated1.Mtime, updated1.Expires)
	if !bytes.Equal(updatedData, newData) {
		t.Fatal("Expected updatedData for updated1 got", updatedData)
	}

	if path1 == updatedPath {
		t.Fatal("Expected paths to change but both are", path1)
	}
	ue, ok := c.entriesMap[1]
	if !ok || ue.Value.(*cacheEntry).path != updatedPath {
		t.Fatalf("Internal path was not updated. Expected %s but got %s", ue.Value.(*cacheEntry).path, updatedPath)
	}

	_, err = os.Stat(path1)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Old path %s was not removed\n", path1)
	}

	assertStruct(t, Stats{
		Items:          3,
		Bytes:          123,
		Has:            0,
		Gets:           0,
		Hits:           0,
		Puts:           4,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_Put_ErrorOnWriteCleanUp(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 50*TB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	key := uint64(1)
	shard := fmt.Sprintf("%02s", strconv.FormatUint(key, 36))

	fakeError := errors.New("ohh no something failed")
	info, err := c.PutReader(key, ReaderFunc(func(p []byte) (n int, err error) {
		return 0, fakeError
	}), 0)
	if err != fakeError {
		t.Fatal("got unexpected error form PutReader:", err)
	}
	if info != nil {
		t.Fatal("info should be nil but was:", info)
	}

	d := filepath.Join(dir, shard)
	entries, err := os.ReadDir(d)
	assertNoError(t, err)
	if len(entries) != 0 {
		t.Fatalf("dir %s should not contain files but found %d", d, len(entries))
	}

	info, err = c.Has(key)
	if err != ErrNotFound {
		t.Fatal("err should be ErrNotFound but was:", err)
	}
	if info != nil {
		t.Fatal("info should be nil but was:", err)
	}

	data, info, err := c.Get(key)
	if err != ErrNotFound {
		t.Fatal("err should be ErrNotFound but was:", err)
	}
	if info != nil {
		t.Fatal("info should be nil but was:", err)
	}
	if data != nil {
		t.Fatal("data should be nil but was:", err)
	}

	assertStruct(t, Stats{
		Items:          0,
		Bytes:          0,
		Has:            1,
		Gets:           1,
		Hits:           0,
		Puts:           1,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_Has(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 50*MB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	key := uint64(1)
	putEntry, err := c.Put(key, DATA, 0)
	assertNoError(t, err)

	hasEntry, err := c.Has(key)
	assertNoError(t, err)

	assertStruct(t, putEntry, hasEntry)

	notExisting, err := c.Has(123)
	if err != ErrNotFound {
		assertNoError(t, err)
	}
	if notExisting != nil {
		t.Fatalf("Expected nil but got %+v\n", notExisting)
	}

	assertStruct(t, Stats{
		Items:          1,
		Bytes:          59,
		Has:            2,
		Gets:           0,
		Hits:           0,
		Puts:           1,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_Get(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 50*TiB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	start := time.Now().Add(-time.Millisecond)
	key := uint64(1)

	data, entry, err := c.Get(key)
	if err != ErrNotFound || data != nil || entry != nil {
		t.Fatalf("Unexpected return for not existing key: %+v, %+v, %+v", data, entry, err)
	}

	_, err = c.Put(key, DATA, 0)
	assertNoError(t, err)

	data, entry, err = c.Get(key)
	assertNoError(t, err)
	if !bytes.Equal(data, DATA) {
		t.Fatal("Unexpected data for get", data)
	}
	if entry.Size != 59 {
		t.Fatal("Expected size 59 for entry but got", entry.Size)
	}
	if !(entry.Mtime.After(start) && entry.Mtime.Before(time.Now().Add(time.Millisecond))) {
		t.Fatal("Unexpected mtime for entry", entry.Mtime)
	}
	assertTime(t, time.Time{}, entry.Expires)

	r, entry, err := c.GetReader(key)
	assertNoError(t, err)

	assertReaderBytes(t, DATA, r)

	if entry.Size != 59 {
		t.Fatal("Expected size 59 for entry but got", entry.Size)
	}
	if !(entry.Mtime.After(start) && entry.Mtime.Before(time.Now().Add(time.Millisecond))) {
		t.Fatal("Unexpected mtime for entry", entry.Mtime)
	}
	assertTime(t, time.Time{}, entry.Expires)

	_, err = c.Put(2, DATA, 1*time.Millisecond)
	assertNoError(t, err)

	time.Sleep(2 * time.Millisecond)

	expiredData, expiredEntry, err := c.Get(2)
	if err != ErrNotFound || expiredData != nil || expiredEntry != nil {
		t.Fatalf("Unexpected return for expired key: %+v, %+v, %+v", expiredData, expiredEntry, err)
	}

	assertStruct(t, Stats{
		Items:          2,
		Bytes:          118,
		Has:            0,
		Gets:           4,
		Hits:           2,
		Puts:           2,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_GetOrPut(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 50*TB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	start := time.Now().Add(-time.Millisecond)
	key := uint64(1)

	data, entry1, hit, err := c.GetOrPut(key, 0, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
		n, err := sink.Write(DATA)
		return int64(n), err
	}))
	assertNoError(t, err)
	if !bytes.Equal(data, DATA) {
		t.Fatal("Unexpected data for get", data)
	}
	if entry1.Size != 59 {
		t.Fatal("Expected size 59 for entry but got", entry1.Size)
	}
	if !(entry1.Mtime.After(start) && entry1.Mtime.Before(time.Now().Add(time.Millisecond))) {
		t.Fatal("Unexpected mtime for entry", entry1.Mtime)
	}
	assertTime(t, time.Time{}, entry1.Expires)
	if hit {
		t.Fatal("First GetOrPut should not be a cache hit")
	}

	r, entry2, hit, err := c.GetReaderOrPut(key, 1*time.Hour, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
		return 0, errors.New("filler should not have been called")
	}))
	assertNoError(t, err)

	assertReaderBytes(t, DATA, r)

	// using a different ttl will not change the entry (because it was a cache hit)
	assertStruct(t, entry1, entry2)
	if !hit {
		t.Fatal("Second GetOrPut should be a cache hit")
	}

	assertStruct(t, Stats{
		Items:          1,
		Bytes:          59,
		Has:            0,
		Gets:           2,
		Hits:           1,
		Puts:           1,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_GetOrPut_OnlyWritesOnce(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 1*TB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	wgStart := sync.WaitGroup{}
	wgDone := sync.WaitGroup{}
	lock := sync.Mutex{}
	lock.Lock()

	wgStart.Add(1)
	go func() {
		wgDone.Add(1)
		defer wgDone.Done()
		t.Log("First GetOrPut")
		data, _, _, err := c.GetOrPut(1, 0, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
			wgStart.Done()
			lock.Lock()
			defer lock.Unlock()
			n, err := sink.Write(DATA)
			t.Log("First GetOrPut done")
			return int64(n), err
		}))
		t.Log("First GetOrPut return")
		if err != nil {
			t.Errorf("Unexpected error form first GetOrPut: %s\n", err)
			return
		}
		if !bytes.Equal(data, DATA) {
			t.Error("Unexpected data for first GetOrPut", data)
			return
		}
		t.Log("First GetOrPut end")
	}()

	wgStart.Wait()

	wgStart.Add(1)
	go func() {
		wgStart.Done()
		wgDone.Add(1)
		defer wgDone.Done()
		t.Log("Second GetOrPut")
		data, _, _, err := c.GetOrPut(1, 0, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
			t.Log("Second GetOrPut work")
			return 0, errors.New("filler should not have been called")
		}))
		t.Log("Second GetOrPut return")
		if err != nil {
			t.Errorf("Unexpected error form second GetOrPut: %s\n", err)
			return
		}
		if !bytes.Equal(data, DATA) {
			t.Error("Unexpected data for second GetOrPut", data)
			return
		}
		t.Log("Second GetOrPut end")
	}()

	wgStart.Wait()
	lock.Unlock()
	t.Log("Unlock")
	wgDone.Wait()

	assertStruct(t, Stats{
		Items:          1,
		Bytes:          59,
		Has:            0,
		Gets:           2,
		Hits:           1,
		Puts:           1,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_GetOrPut_OnExpiredEntryOnlyWritesOnce(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 1*TB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	_, err = c.Put(1, DATA, 1*time.Millisecond)
	assertNoError(t, err)
	time.Sleep(2 * time.Millisecond)

	wgStart := sync.WaitGroup{}
	wgDone := sync.WaitGroup{}
	lock := sync.Mutex{}
	lock.Lock()

	wgStart.Add(1)
	go func() {
		wgDone.Add(1)
		defer wgDone.Done()
		t.Log("First GetOrPut")
		data, _, _, err := c.GetOrPut(1, 0, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
			wgStart.Done()
			lock.Lock()
			defer lock.Unlock()
			n, err := sink.Write(DATA)
			t.Log("First GetOrPut done")
			return int64(n), err
		}))
		t.Log("First GetOrPut return")
		if err != nil {
			t.Errorf("Unexpected error form first GetOrPut: %s\n", err)
			return
		}
		if !bytes.Equal(data, DATA) {
			t.Error("Unexpected data for first GetOrPut", data)
			return
		}
		t.Log("First GetOrPut end")
	}()

	wgStart.Wait()

	wgStart.Add(1)
	go func() {
		wgStart.Done()
		wgDone.Add(1)
		defer wgDone.Done()
		t.Log("Second GetOrPut")
		data, _, _, err := c.GetOrPut(1, 0, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
			t.Log("Second GetOrPut work")
			return 0, errors.New("filler should not have been called")
		}))
		t.Log("Second GetOrPut return")
		if err != nil {
			t.Errorf("Unexpected error form second GetOrPut: %s\n", err)
			return
		}
		if !bytes.Equal(data, DATA) {
			t.Error("Unexpected data for second GetOrPut", data)
			return
		}
		t.Log("Second GetOrPut end")
	}()

	wgStart.Wait()
	lock.Unlock()
	t.Log("Unlock")
	wgDone.Wait()

	assertStruct(t, Stats{
		Items:          1,
		Bytes:          59,
		Has:            0,
		Gets:           2,
		Hits:           1,
		Puts:           2,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_GetOrPut_OnOtherKeyDoesNotBlock(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 1*TB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	wgStart := sync.WaitGroup{}
	wgDone := sync.WaitGroup{}
	lock := sync.Mutex{}
	lock.Lock()

	wgStart.Add(1)
	go func() {
		wgDone.Add(1)
		defer wgDone.Done()
		t.Log("First GetOrPut")
		data, _, _, err := c.GetOrPut(1, 0, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
			wgStart.Done()
			lock.Lock()
			defer lock.Unlock()
			n, err := sink.Write(DATA)
			t.Log("First GetOrPut done")
			return int64(n), err
		}))
		t.Log("First GetOrPut return")
		if err != nil {
			t.Errorf("Unexpected error form first GetOrPut: %s\n", err)
			return
		}
		if !bytes.Equal(data, DATA) {
			t.Error("Unexpected data for first GetOrPut", data)
			return
		}
		t.Log("First GetOrPut end")
	}()

	wgStart.Wait()

	wgSecond := sync.WaitGroup{}

	wgSecond.Add(1)
	go func() {
		defer wgSecond.Done()
		t.Log("Second GetOrPut")
		data, _, _, err := c.GetOrPut(2, 0, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
			n, err := sink.Write(DATA)
			t.Log("Second GetOrPut done")
			return int64(n), err
		}))
		t.Log("Second GetOrPut return")
		if err != nil {
			t.Errorf("Unexpected error form second GetOrPut: %s\n", err)
			return
		}
		if !bytes.Equal(data, DATA) {
			t.Error("Unexpected data for second GetOrPut", data)
			return
		}
		t.Log("Second GetOrPut end")
	}()

	wgSecond.Wait() // second got through despite key 1 still being locked

	lock.Unlock()
	t.Log("Unlock")
	wgDone.Wait()

	assertStruct(t, Stats{
		Items:          2,
		Bytes:          118,
		Has:            0,
		Gets:           2,
		Hits:           0,
		Puts:           2,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_GetOrPut_ErrorOnWriteCleanUp(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 50*TB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	key := uint64(1)
	shard := fmt.Sprintf("%02s", strconv.FormatUint(key, 36))

	fakeError := errors.New("ohh no something failed")
	data, info, hit, err := c.GetOrPut(key, 0, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
		n, err := sink.Write(DATA)
		return int64(n), fakeError
	}))
	if err != fakeError {
		t.Fatal("got unexpected error form GetOrPut:", err)
	}
	if hit {
		t.Fatal("hit should be false but was:", hit)
	}
	if info != nil {
		t.Fatal("info should be nil but was:", info)
	}
	if data != nil {
		t.Fatal("data should be nil but was:", data)
	}

	d := filepath.Join(dir, shard)
	entries, err := os.ReadDir(d)
	assertNoError(t, err)
	if len(entries) != 0 {
		t.Fatalf("dir %s should not contain files but found %d", d, len(entries))
	}

	info, err = c.Has(key)
	if err != ErrNotFound {
		t.Fatal("err should be ErrNotFound but was:", err)
	}
	if info != nil {
		t.Fatal("info should be nil but was:", err)
	}

	data, info, err = c.Get(key)
	if err != ErrNotFound {
		t.Fatal("err should be ErrNotFound but was:", err)
	}
	if info != nil {
		t.Fatal("info should be nil but was:", err)
	}
	if data != nil {
		t.Fatal("data should be nil but was:", err)
	}

	assertStruct(t, Stats{
		Items:          0,
		Bytes:          0,
		Has:            1,
		Gets:           2,
		Hits:           0,
		Puts:           1,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_Delete(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 50*TB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	notExisting, err := c.Delete(1)
	assertNoError(t, err)
	if notExisting != nil {
		t.Fatal("notExisting entry should be nil but was:", notExisting)
	}

	entry, err := c.Put(1, DATA, 0)
	assertNoError(t, err)

	_, p := readFileForKey(t, dir, 1, entry.Mtime, entry.Expires)
	_, err = os.Stat(p)
	assertNoError(t, err)

	deletedEntry, err := c.Delete(1)
	assertNoError(t, err)

	assertStruct(t, entry, deletedEntry)

	_, err = os.Stat(p)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("path does still exist:", p)
	}

	_, err = c.Has(1)
	if err != ErrNotFound {
		t.Fatal("Has should respond with ErrNotFound error but was:", err)
	}

	assertStruct(t, Stats{
		Items:          0,
		Bytes:          0,
		Has:            1,
		Gets:           0,
		Hits:           0,
		Puts:           1,
		Deletes:        2,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_Clear(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 1*TB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	_, err = c.Put(1, DATA, 0)
	assertNoError(t, err)

	_, _, err = c.Get(1)
	assertNoError(t, err)

	_, err = c.Has(1)
	assertNoError(t, err)

	_, err = c.Put(2, DATA, 0)
	assertNoError(t, err)

	_, err = c.Delete(2)
	assertNoError(t, err)

	_, err = c.Put(3, DATA, 0)
	assertNoError(t, err)

	err = c.Clear(false)
	assertNoError(t, err)

	_, err = os.Stat(dir)
	if !os.IsNotExist(err) {
		t.Fatal("Cache dir was not removed")
	}

	assertStruct(t, Stats{
		Items:          0,
		Bytes:          0,
		Has:            1,
		Gets:           1,
		Hits:           1,
		Puts:           3,
		Deletes:        3, // 1 manual + 2 from clear
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())

	_, err = c.Put(1, DATA, 0)
	assertNoError(t, err)

	err = c.Clear(true)
	assertNoError(t, err)

	_, err = os.Stat(dir)
	if !os.IsNotExist(err) {
		t.Fatal("Cache dir was not removed")
	}

	assertStruct(t, Stats{
		Items:          0,
		Bytes:          0,
		Has:            0,
		Gets:           0,
		Hits:           0,
		Puts:           0,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_Eviction(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 130*Byte).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	fakeEvictionTime := time.Now().Add(-1 * time.Minute)
	c.evictionTime = fakeEvictionTime

	e1, err := c.Put(1, DATA, 0)
	assertNoError(t, err)

	_, err = c.Put(2, DATA, 0)
	assertNoError(t, err)

	e3, err := c.Put(3, DATA, 0)
	assertNoError(t, err)

	_, err = c.Put(4, DATA, 0)
	assertNoError(t, err)

	// protect 2 from eviction by using it
	_, _, err = c.Get(2)
	assertNoError(t, err)

	// give async evict some time to start
	time.Sleep(50 * time.Millisecond)

	// eviction was not called on put because evictionInterval did not pass
	assertTime(t, fakeEvictionTime, c.evictionTime)
	assertStruct(t, Stats{
		Items:          4,
		Bytes:          236,
		Has:            0,
		Gets:           1,
		Hits:           1,
		Puts:           4,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c.Stats())

	// fake past eviction, so evict actually runs
	c.evictionTime = time.Now().Add(-2 * time.Hour)

	beforeEviction := time.Now().Add(-1 * time.Millisecond)
	c.evict()

	if !beforeEviction.Before(c.evictionTime) {
		t.Fatalf("evictionTime was not updated: %v", c.evictionTime)
	}

	assertStruct(t, Stats{
		Items:          2,
		Bytes:          118,
		Has:            0,
		Gets:           1,
		Hits:           1,
		Puts:           4,
		Deletes:        0,
		Evictions:      2,
		EvictionErrors: nil,
	}, c.Stats())

	_, err = c.Has(1)
	if err != ErrNotFound {
		t.Fatal("entry 1 should have been evicted")
	}
	shard, name := toFilename(1, e1.Mtime, e1.Expires)
	path1 := filepath.Join(dir, shard, name)
	_, err = os.Stat(path1)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("entry 1 file was not removed", path1)
	}

	_, err = c.Has(2)
	if err != nil {
		t.Fatal("entry 2 should not have been evicted")
	}
	_, err = c.Has(3)
	if err != ErrNotFound {
		t.Fatal("entry 3 should have been evicted")
	}
	shard, name = toFilename(1, e3.Mtime, e3.Expires)
	path3 := filepath.Join(dir, shard, name)
	_, err = os.Stat(path3)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("entry 3 file was not removed", path3)
	}
	_, err = c.Has(4)
	if err != nil {
		t.Fatal("entry 4 should not have been evicted")
	}

	e5, err := c.Put(5, DATA, 1*time.Millisecond)
	assertNoError(t, err)

	time.Sleep(2 * time.Millisecond)

	assertStruct(t, Stats{
		Items:          3,
		Bytes:          177,
		Has:            4,
		Gets:           1,
		Hits:           1,
		Puts:           5,
		Deletes:        0,
		Evictions:      2,
		EvictionErrors: nil,
	}, c.Stats())

	// fake past eviction, so evict actually runs
	c.evictionTime = time.Now().Add(-2 * time.Hour)

	c.evict()

	_, err = c.Has(5)
	if err != ErrNotFound {
		t.Fatal("entry 5 should have been evicted")
	}
	shard, name = toFilename(1, e5.Mtime, e5.Expires)
	path5 := filepath.Join(dir, shard, name)
	_, err = os.Stat(path5)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("entry 5 file was not removed", path5)
	}

	assertStruct(t, Stats{
		Items:          2,
		Bytes:          118,
		Has:            5,
		Gets:           1,
		Hits:           1,
		Puts:           5,
		Deletes:        0,
		Evictions:      3,
		EvictionErrors: nil,
	}, c.Stats())
}

func TestFileCache_EvictionOnPut(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 130*Byte).WithEvictionInterval(0).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	beforeEviction := time.Now().Add(-1 * time.Millisecond)

	e1, err := c.Put(1, DATA, 0)
	assertNoError(t, err)

	e2, err := c.Put(2, DATA, 0)
	assertNoError(t, err)

	_, err = c.Put(3, DATA, 0)
	assertNoError(t, err)

	// give async evict some time to finish
	time.Sleep(25 * time.Millisecond)

	assertStruct(t, Stats{
		Items:          2,
		Bytes:          118,
		Has:            0,
		Gets:           0,
		Hits:           0,
		Puts:           3,
		Deletes:        0,
		Evictions:      1,
		EvictionErrors: nil,
	}, c.Stats())

	if !beforeEviction.Before(c.evictionTime) {
		t.Fatalf("evictionTime was not updated: %v", c.evictionTime)
	}

	beforeEviction = time.Now().Add(-1 * time.Millisecond)

	_, err = c.PutReader(4, bytes.NewReader(DATA), 0)
	assertNoError(t, err)

	// give async evict some time to finish
	time.Sleep(25 * time.Millisecond)

	assertStruct(t, Stats{
		Items:          2,
		Bytes:          118,
		Has:            0,
		Gets:           0,
		Hits:           0,
		Puts:           4,
		Deletes:        0,
		Evictions:      2,
		EvictionErrors: nil,
	}, c.Stats())

	if !beforeEviction.Before(c.evictionTime) {
		t.Fatalf("evictionTime was not updated: %v", c.evictionTime)
	}

	_, err = c.Has(1)
	if err != ErrNotFound {
		t.Fatal("entry 1 should have been evicted")
	}
	shard1, name1 := toFilename(1, e1.Mtime, e1.Expires)
	path1 := filepath.Join(dir, shard1, name1)
	_, err = os.Stat(path1)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("entry 1 file was not removed", path1)
	}

	_, err = c.Has(2)
	if err != ErrNotFound {
		t.Fatal("entry 2 should have been evicted")
	}
	shard2, name2 := toFilename(1, e2.Mtime, e2.Expires)
	path2 := filepath.Join(dir, shard2, name2)
	_, err = os.Stat(path2)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("entry 2 file was not removed", path2)
	}

	_, err = c.Has(3)
	if err != nil {
		t.Fatal("entry 3 should not have been evicted")
	}
	_, err = c.Has(4)
	if err != nil {
		t.Fatal("entry 4 should not have been evicted")
	}
}

func TestFileCache_Load(t *testing.T) {
	dir := t.TempDir()
	ci1, err := Builder(dir, 1*GB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c1 := ci1.(*cache)

	e1, err := c1.Put(1, DATA, 0)
	assertNoError(t, err)

	e2, err := c1.Put(2, DATA, 1*time.Hour)
	assertNoError(t, err)

	e3, err := c1.Put(3, DATA, 0)
	assertNoError(t, err)

	mtime4 := time.Now().Add(-24 * time.Hour)
	expires4 := time.Now().Add(-1 * time.Hour)
	shard, name := toFilename(4, mtime4, expires4)
	err = os.MkdirAll(filepath.Join(dir, shard), 0750)
	assertNoError(t, err)
	path4 := filepath.Join(dir, shard, name)
	err = os.WriteFile(path4, DATA, 0640)
	assertNoError(t, err)

	ci2, err := Builder(dir, 1*GB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c2 := ci2.(*cache)

	secondE1, err := c2.Has(1)
	assertNoError(t, err)
	e1.Mtime = time.UnixMilli(e1.Mtime.UnixMilli())
	assertStruct(t, e1, secondE1)

	data, secondE2, err := c2.Get(2)
	assertNoError(t, err)
	e2.Mtime = time.UnixMilli(e2.Mtime.UnixMilli())
	e2.Expires = time.UnixMilli(e2.Expires.UnixMilli())
	assertStruct(t, e2, secondE2)
	if !bytes.Equal(data, DATA) {
		t.Fatal("Unexpected data for entry 2", data)
	}

	secondE3, err := c2.Has(3)
	assertNoError(t, err)
	e3.Mtime = time.UnixMilli(e3.Mtime.UnixMilli())
	assertStruct(t, e3, secondE3)

	_, err = c2.Has(4)
	if err != ErrNotFound {
		t.Fatal("entry 4 should have been expired", err)
	}

	assertStruct(t, Stats{
		Items:          4,
		Bytes:          236,
		Has:            3,
		Gets:           1,
		Hits:           1,
		Puts:           0,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c2.Stats())
}

func TestFileCache_WithFileMode(t *testing.T) {
	dir := t.TempDir()
	fileMode := fs.FileMode(0744) // test with value that does not interfere with default umask of 022
	dirMode := fileMode | fs.ModeDir | 0700
	c, err := Builder(dir, 1*MB).WithFileMode(fileMode).Build()
	assertNoError(t, err)

	_, err = c.Put(1, DATA, 0)
	assertNoError(t, err)

	entries, err := os.ReadDir(dir)
	assertNoError(t, err)

	// sadly this test does not work on Windows,
	// since it seems to use fixed file modes
	if runtime.GOOS == "windows" {
		fileMode = 0666
		dirMode = 0777 | fs.ModeDir
	}

	for _, entry := range entries {
		if entry.IsDir() {
			info, err := entry.Info()
			assertNoError(t, err)
			if info.Mode() != dirMode {
				t.Fatalf("Expected mode %s for %s but got %s\n", dirMode, entry.Name(), info.Mode())
			}

			files, err := os.ReadDir(path.Join(dir, entry.Name()))
			assertNoError(t, err)
			for _, file := range files {
				if file.IsDir() {
					t.Fatal("Expected file but found dir:", entry.Name())
				} else {
					info, err := file.Info()
					assertNoError(t, err)
					if info.Mode() != fileMode {
						t.Fatalf("Expected mode %s for %s but got %s\n", fileMode, file.Name(), info.Mode())
					}
				}
			}
		} else {
			t.Fatal("Expected dir but found file:", entry.Name())
		}
	}
}

func TestFileCache_PutWhileReaderIsOpen(t *testing.T) {
	dir := t.TempDir()
	ci, err := Builder(dir, 1*GB).WithEvictionInterval(1 * time.Hour).Build()
	assertNoError(t, err)
	c := ci.(*cache)

	key := uint64(1)
	_, err = c.Put(key, DATA, 0)
	assertNoError(t, err)

	// get and hold open
	reader, _, err := c.GetReader(key)
	assertNoError(t, err)

	// update open key
	update := []byte("other data")
	_, err = c.Put(key, update, 0)
	if err != nil && runtime.GOOS == "windows" && err.(*fs.PathError).Op == "remove" && strings.HasPrefix(err.(*fs.PathError).Path, dir) && err.(*fs.PathError).Err.(syscall.Errno) == 32 {
		// ignore internal/syscall/windows.ERROR_SHARING_VIOLATION(32) and abort test
		_ = reader.Close()
		return
	}
	assertNoError(t, err)

	// fresh get reads updated entry
	updatedReader, _, err := c.GetReader(key)
	assertNoError(t, err)
	assertReaderBytes(t, update, updatedReader)

	// first get still reads initial data
	assertReaderBytes(t, DATA, reader)
}
