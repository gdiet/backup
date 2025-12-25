package fstest

import (
	"backup/src/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestExample(t *testing.T) {
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
	// list files in mounted file system
	entries, err := os.ReadDir(mountpoint)
	t.Logf("Files in mountpoint: %v", entries)
	// read file from mounted file system
	data, err := os.ReadFile(filepath.Join(mountpoint, "hello.txt"))
	if err != nil {
		t.Fatalf("Failed to read file from mounted file system: %v", err)
	}
	expected := "Hello, World!\n"
	if string(data) != expected {
		t.Fatalf("Unexpected file contents: got %q, want %q", string(data), expected)
	}
}
