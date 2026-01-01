package fs

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/winfsp/cgofuse/fuse"
)

func TestFileSystem(t *testing.T) {
	dir := t.TempDir()
	repository := filepath.Join(dir, "repository")
	if os.Mkdir(repository, 0755) != nil {
		t.Fatal("Failed to create repository directory")
	}
	f, err := newFileSystem(repository)
	if err != nil {
		t.Fatal("Failed to create new file system")
	}

	t.Run("Getattr", testGetattr(f))
	t.Run("Readdir", testReaddir(f))
}

func testGetattr(f *fileSystem) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("root", testGetattrRoot(f))
		t.Run("notFound", testGetattrNotFound(f))
	}
}

func testGetattrNotFound(f *fileSystem) func(t *testing.T) {
	return func(t *testing.T) {
		// prepare
		defer f.Rmdir(mkdirOK(t, f, "/dir"))
		defer f.Rmdir(mkdirOK(t, f, "/dir/inner"))
		defer f.Rmdir(mkdirOK(t, f, "/other"))

		// test 1
		stat := &fuse.Stat_t{}
		ret := f.Getattr("/nonexistent", stat, 0)
		if ret != -fuse.ENOENT {
			t.Fatalf("Getattr /nonexistent returned %d, expected -fuse.ENOENT", ret)
		}

		// test 2
		ret = f.Getattr("/dir/nonexistent", stat, 0)
		if ret != -fuse.ENOENT {
			t.Fatalf("Getattr /dir/nonexistent returned %d, expected -fuse.ENOENT", ret)
		}
	}
}

func mkdirOK(t *testing.T, f *fileSystem, path string) string {
	ret := f.Mkdir(path, 0)
	if ret != 0 {
		t.Fatalf("Mkdir %s returned %d, expected 0", path, ret)
	}
	return path
}

func testGetattrRoot(f *fileSystem) func(t *testing.T) {
	return func(t *testing.T) {
		stat := &fuse.Stat_t{}
		ret := f.Getattr("/", stat, 0)
		if ret != 0 {
			t.Fatalf("Getattr / returned %d, expected 0", ret)
		}
		if (stat.Mode & fuse.S_IFDIR) == 0 {
			t.Fatalf("Getattr / did not return a directory")
		}
	}
}

func testReaddir(f *fileSystem) func(t *testing.T) {
	return func(t *testing.T) {
		// prepare
		defer f.Rmdir(mkdirOK(t, f, "/dir"))
		defer f.Rmdir(mkdirOK(t, f, "/dir/inner"))
		defer f.Rmdir(mkdirOK(t, f, "/other"))
		entries := []string{}
		fill := func(name string, stat *fuse.Stat_t, ofst int64) bool {
			entries = append(entries, name)
			return true
		}

		// test 1
		ret := f.Readdir("/nonexistent", fill, 0, 0)
		if ret != -fuse.ENOENT {
			t.Fatalf("Readdir /nonexistent returned %d, expected -fuse.ENOENT", ret)
		}

		// test 2
		entries = nil
		ret = f.Readdir("/dir", fill, 0, 0)
		if ret != 0 {
			t.Fatalf("Readdir /dir returned %d, expected 0", ret)
		}
		expectedEntries := []string{".", "..", "inner"}
		if !reflect.DeepEqual(entries, expectedEntries) {
			t.Fatalf("Readdir /dir returned entries %v, expected %v", entries, expectedEntries)
		}
	}
}
