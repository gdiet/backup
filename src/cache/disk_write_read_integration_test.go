package cache

import (
	"path/filepath"
	"testing"
)

// TestDiskWriteReadIntegration tests that Write opens file for both reading and writing.
func TestDiskWriteReadIntegration(t *testing.T) {
	// Create a temporary file path
	tempDir := t.TempDir()
	testFilePath := filepath.Join(tempDir, "test_write_read.dat")

	disk := &Disk{
		file:     nil,
		filePath: testFilePath,
	}

	// Write some data (should open file with O_RDWR)
	originalData := Bytes([]byte("Hello, World!"))
	err := disk.Write(0, originalData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Now read the data back without closing/reopening
	readBuffer := make(Bytes, len(originalData))
	err = disk.Read(0, readBuffer)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// Verify the data matches
	if string(readBuffer) != string(originalData) {
		t.Errorf("Read data mismatch. Expected: %q, Got: %q",
			string(originalData), string(readBuffer))
	}

	// Write more data at different position
	moreData := Bytes([]byte(" How are you?"))
	err = disk.Write(int64(len(originalData)), moreData)
	if err != nil {
		t.Fatalf("Second write failed: %v", err)
	}

	// Read the full content
	fullBuffer := make(Bytes, len(originalData)+len(moreData))
	err = disk.Read(0, fullBuffer)
	if err != nil {
		t.Fatalf("Full read failed: %v", err)
	}

	expected := string(originalData) + string(moreData)
	if string(fullBuffer) != expected {
		t.Errorf("Full read mismatch. Expected: %q, Got: %q",
			expected, string(fullBuffer))
	}

	// Clean up
	err = disk.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}
