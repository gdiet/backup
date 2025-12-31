package fs

import (
	"os"
	"path/filepath"
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
}

func testGetattr(f *fileSystem) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("root", testGetattrRoot(f))
		t.Run("notFound", testGetattrNotFound(f))
	}
}

func testGetattrNotFound(f *fileSystem) func(t *testing.T) {
	return func(t *testing.T) {
		stat := &fuse.Stat_t{}
		ret := f.Getattr("/nonexistent", stat, 0)
		if ret != -fuse.ENOENT {
			t.Fatalf("Getattr /nonexistent returned %d, expected -fuse.ENOENT", ret)
		}
		ret = f.Mkdir("/dir", 0)
		if ret != 0 {
			t.Fatalf("Mkdir /dir returned %d, expected 0", ret)
		}
		defer f.Rmdir("/dir")
		ret = f.Mkdir("/dir/inner", 0)
		if ret != 0 {
			t.Fatalf("Mkdir /dir/inner returned %d, expected 0", ret)
		}
		defer f.Rmdir("/dir/inner")
		ret = f.Mkdir("/other", 0)
		if ret != 0 {
			t.Fatalf("Mkdir /other returned %d, expected 0", ret)
		}
		defer f.Rmdir("/other")
		ret = f.Getattr("/dir/nonexistent", stat, 0)
		if ret != -fuse.ENOENT {
			t.Fatalf("Getattr /dir/nonexistent returned %d, expected -fuse.ENOENT", ret)
		}
	}
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
