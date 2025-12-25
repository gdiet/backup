package fstest

import (
	"backup/src/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestDedupFileSystem(t *testing.T) {
	dir := t.TempDir()
	mountpoint := filepath.Join(dir, "mountpoint")
	if runtime.GOOS != "windows" {
		if os.Mkdir(mountpoint, 0755) != nil {
			t.Fatal("Failed to create mountpoint directory")
		}
	}
	host := fs.Setup()
	go func() { // Run in goroutine to avoid blocking test
		fs.DoMount(host, mountpoint)
	}()
	defer host.Unmount()
	// wait for mount to be ready. 80 ms was enough on all environments I checked so far.
	time.Sleep(100 * time.Millisecond)

	t.Run("mkdir", mkdir(mountpoint))
}

func mkdir(mountpoint string) func(t *testing.T) {
	dir := filepath.Join(mountpoint, "mkdir")
	defer os.RemoveAll(dir)
	return func(t *testing.T) {
		if err := os.Mkdir(filepath.Join(mountpoint, "mkdir"), 0755); err != nil {
			t.Fatalf("Failed to create directory in mounted file system: %v", err)
		}
	}
}
