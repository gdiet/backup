package cache_old2

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDiskAutoOpen tests that the Disk struct automatically opens files when writing.
func TestDiskAutoOpen(t *testing.T) {
	// Create a temporary file path
	tempDir := t.TempDir()
	testFilePath := filepath.Join(tempDir, "test_auto_open.dat")

	// Create Disk with filePath but no open file
	disk := &Disk{
		file:     nil, // File not open yet
		filePath: testFilePath,
	}

	// Write should automatically open the file
	testData := Bytes([]byte("Hello, World!"))
	err := disk.Write(0, testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify file was created and opened
	if disk.file == nil {
		t.Fatal("File should be open after write")
	}

	// Verify file exists on disk
	if _, err := os.Stat(testFilePath); os.IsNotExist(err) {
		t.Fatal("File should exist on disk after write")
	}

	// Clean up
	err = disk.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// TestDiskTruncateWithoutFile tests that Truncate handles unopened files gracefully.
func TestDiskTruncateWithoutFile(t *testing.T) {
	// Create Disk with filePath but no open file
	disk := &Disk{
		file:     nil,
		filePath: "some/path.dat",
	}

	// Truncate should succeed without opening the file (nothing to truncate)
	err := disk.Truncate(100)
	if err != nil {
		t.Fatalf("Truncate should succeed when file is not open: %v", err)
	}

	// File should still be nil (not opened)
	if disk.file != nil {
		t.Fatal("File should remain closed after truncate on unopened file")
	}
}

// TestDiskWriteThenTruncate tests the normal workflow: Write (opens file) then Truncate.
func TestDiskWriteThenTruncate(t *testing.T) {
	// Create a temporary file path
	tempDir := t.TempDir()
	testFilePath := filepath.Join(tempDir, "test_write_truncate.dat")

	disk := &Disk{
		file:     nil,
		filePath: testFilePath,
	}

	// First write (should open the file)
	testData := Bytes([]byte("Hello, World! This is a longer text."))
	err := disk.Write(0, testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Now truncate (file is already open)
	err = disk.Truncate(13) // Keep only "Hello, World!"
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify file size
	stat, err := os.Stat(testFilePath)
	if err != nil {
		t.Fatal("File should exist after write and truncate")
	}
	if stat.Size() != 13 {
		t.Errorf("Expected file size 13 after truncate, got %d", stat.Size())
	}

	// Clean up
	err = disk.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// TestDiskNoFilePathError tests error handling when no filePath is set.
func TestDiskNoFilePathError(t *testing.T) {
	// Create Disk without filePath
	disk := &Disk{
		file:     nil,
		filePath: "", // No file path
	}

	// Write should return error
	testData := Bytes([]byte("test"))
	err := disk.Write(0, testData)
	if err == nil {
		t.Fatal("Write should fail when no filePath is set")
	}

	// Truncate should succeed (nothing to truncate when file not open)
	err = disk.Truncate(50)
	if err != nil {
		t.Errorf("Truncate should succeed on unopened file: %v", err)
	}
}
