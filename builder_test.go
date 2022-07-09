package fcache

import (
	"io/fs"
	"sync"
	"testing"
)

func TestBuilder_WithFileMode(t *testing.T) {
	dir := t.TempDir()

	ci, err := Builder(dir, 50*MB).Build()
	assertNoError(t, err)
	c := ci.(*cache)
	if c.fileMode != 0600 {
		t.Fatalf("Expected '%o' but got '%o'\n", 0600, c.fileMode)
	}
	if c.dirMode != 0700 {
		t.Fatalf("Expected '%o' but got '%o'\n", 0700, c.fileMode)
	}

	_, err = Builder(dir, 50*MB).WithFileMode(0477).Build()
	if err == nil || err.Error() != "fileMode has to be at least 0600" {
		t.Fatal("Expected fileMode error but got:", err)
	}

	fileMode := fs.FileMode(0666)
	ci, err = Builder(dir, 50*MB).WithFileMode(fileMode).Build()
	assertNoError(t, err)
	c = ci.(*cache)
	if c.fileMode != fileMode {
		t.Fatalf("Expected '%o' but got '%o'\n", fileMode, c.fileMode)
	}
	if c.dirMode != 0766 {
		t.Fatalf("Expected '%o' but got '%o'\n", fileMode, c.fileMode)
	}
}

func TestBuilder_WithBackgroundInit(t *testing.T) {
	dir := t.TempDir()

	c1, err := Builder(dir, 50*MB).WithBackgroundInit(nil).Build()
	assertNoError(t, err)

	_, err = c1.Put(1, []byte("Hello World"), 0)
	assertNoError(t, err)

	wg := sync.WaitGroup{}

	wg.Add(1)
	var cacheFromInit Cache
	c2, err := Builder(dir, 50*MB).WithBackgroundInit(func(initCache Cache, initError error) {
		defer wg.Done()
		cacheFromInit = initCache
		assertNoError(t, initError)
	}).Build()
	assertNoError(t, err)

	wg.Wait()

	assertStruct(t, c2, cacheFromInit)

	assertStruct(t, Stats{
		Items:          1,
		Bytes:          11,
		Has:            0,
		Gets:           0,
		Hits:           0,
		Puts:           0,
		Deletes:        0,
		Evictions:      0,
		EvictionErrors: nil,
	}, c2.Stats())
}
