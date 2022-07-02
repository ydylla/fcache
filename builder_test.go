package fcache

import (
	"io/fs"
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
