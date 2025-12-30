package fs

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestDedupFileSystem(t *testing.T) {
	dir := t.TempDir()
	repository := filepath.Join(dir, "repository")
	if os.Mkdir(repository, 0755) != nil {
		t.Fatal("Failed to create repository directory")
	}
	mountpoint := filepath.Join(dir, "mountpoint")
	if runtime.GOOS != "windows" && os.Mkdir(mountpoint, 0755) != nil {
		t.Fatal("Failed to create mountpoint directory")
	}
	host, _ := setup(repository)
	go func() { // Run in goroutine to avoid blocking test
		mount(host, mountpoint)
	}()
	defer host.Unmount()
	// wait for mount to be ready. 80 ms was enough on all environments I checked so far.
	time.Sleep(100 * time.Millisecond)

	result := true
	if result {
		result = t.Run("mkdir", mkdir(mountpoint))
	}
	if result {
		result = t.Run("rmdir", rmdir(mountpoint))
	}
	if result {
		result = t.Run("readdir", readdir(mountpoint))
	}
}

func testWithDir(mountpoint, dirname string, testFn func(*testing.T, string)) func(*testing.T) {
	dir := filepath.Join(mountpoint, dirname)
	defer os.RemoveAll(dir)
	return func(t *testing.T) {
		if err := os.Mkdir(dir, 0755); err != nil {
			t.Fatalf("Failed to create directory in mounted file system: %v", err)
		}
		testFn(t, dir)
	}
}

func mkdir(mountpoint string) func(t *testing.T) {
	return testWithDir(mountpoint, "mkdir", func(t *testing.T, dir string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read directory in mounted file system: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("Unexpected directory entries: %v", entries)
		}
	})
}

func rmdir(mountpoint string) func(t *testing.T) {
	return testWithDir(mountpoint, "rmdir", func(t *testing.T, dir string) {
		inner := filepath.Join(dir, "inner")
		if err := os.Mkdir(inner, 0755); err != nil {
			t.Fatalf("Failed to create inner directory in mounted file system: %v", err)
		}
		if err := os.Remove(dir); err == nil {
			t.Fatalf("Could remove non-empty directory")
		}
		if err := os.RemoveAll(inner); err != nil {
			t.Fatalf("Failed to remove inner directory in mounted file system: %v", err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read directory in mounted file system: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("Unexpected directory entries: %v", entries)
		}
	})
}

func readdir(mountpoint string) func(t *testing.T) {
	return testWithDir(mountpoint, "readdir", func(t *testing.T, dir string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read directory in mounted file system: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("Unexpected directory entries: %v", entries)
		}
		inner := filepath.Join(dir, "inner")
		if err := os.Mkdir(inner, 0755); err != nil {
			t.Fatalf("Failed to create inner directory in mounted file system: %v", err)
		}
		entries, err = os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read directory in mounted file system: %v", err)
		}
		if len(entries) != 1 || entries[0].Name() != "inner" {
			t.Fatalf("Unexpected directory entries after creating inner dir: %v", entries)
		}
	})
}
