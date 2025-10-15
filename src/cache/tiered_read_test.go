package cache

import (
	"io"
	"os"
	"testing"
)

const (
	noErrorMsg        = "Expected no error, got: %v"
	createFileFailMsg = "Failed to create test file: %v"
	openFileFailMsg   = "Failed to open test file: %v"
	tenBytesReadMsg   = "Expected 10 bytes read, got %d"
)

// TestTieredReadBasic tests basic Tiered.Read functionality
func TestTieredReadBasic(t *testing.T) {
	// Test empty cache
	tiered := Tiered{
		sparse: Sparse{size: 100},
		memory: Memory{},
		disk:   Disk{},
	}

	data := make([]byte, 10)
	bytesRead, err := tiered.Read(0, data)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if bytesRead != 10 {
		t.Errorf("Expected 10 bytes read, got %d", bytesRead)
	}
	// Should be all zeros
	for i, b := range data {
		if b != 0 {
			t.Errorf("Position %d: expected 0, got %d", i, b)
		}
	}
}

// TestTieredReadZeroLength tests zero-length reads
func TestTieredReadZeroLength(t *testing.T) {
	tiered := Tiered{
		sparse: Sparse{size: 100},
		memory: Memory{},
		disk:   Disk{},
	}

	data := make([]byte, 0)
	bytesRead, err := tiered.Read(0, data)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if bytesRead != 0 {
		t.Errorf("Expected 0 bytes read, got %d", bytesRead)
	}
}

// TestTieredReadEOF tests reading beyond EOF
func TestTieredReadEOF(t *testing.T) {
	tiered := Tiered{
		sparse: Sparse{size: 50},
		memory: Memory{},
		disk:   Disk{},
	}

	data := make([]byte, 10)
	bytesRead, err := tiered.Read(45, data)

	if err != io.EOF {
		t.Errorf("Expected EOF error, got: %v", err)
	}
	if bytesRead != 5 {
		t.Errorf("Expected 5 bytes read, got %d", bytesRead)
	}
}

// TestTieredReadMemoryLayer tests reading from memory layer
func TestTieredReadMemoryLayer(t *testing.T) {
	memory := Memory{}
	memory.Write(10, []byte("Hello"), 1000)

	tiered := Tiered{
		sparse: Sparse{size: 100},
		memory: memory,
		disk:   Disk{},
	}

	data := make([]byte, 10)
	bytesRead, err := tiered.Read(8, data)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if bytesRead != 10 {
		t.Errorf("Expected 10 bytes read, got %d", bytesRead)
	}

	expected := append([]byte{0, 0}, []byte("Hello")...)
	expected = append(expected, []byte{0, 0, 0}...)

	for i := 0; i < len(data); i++ {
		if data[i] != expected[i] {
			t.Errorf("Position %d: expected %d, got %d", i, expected[i], data[i])
		}
	}
}

// TestTieredReadWithDiskOnly tests reading from disk layer only
func TestTieredReadWithDiskOnly(t *testing.T) {
	tempDir := t.TempDir()
	testFile := tempDir + "/tiered_test.dat"

	diskData := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	err := os.WriteFile(testFile, diskData, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	file, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	tiered := Tiered{
		sparse: Sparse{size: int64(len(diskData))},
		memory: Memory{},
		disk:   Disk{file: file},
	}

	data := make([]byte, 5)
	bytesRead, err := tiered.Read(0, data)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if bytesRead != 5 {
		t.Errorf("Expected 5 bytes read, got %d", bytesRead)
	}
	if string(data) != "ABCDE" {
		t.Errorf("Expected 'ABCDE', got %q", string(data))
	}
}

// TestTieredReadMemoryOverridesDisk tests memory layer overriding disk
func TestTieredReadMemoryOverridesDisk(t *testing.T) {
	tempDir := t.TempDir()
	testFile := tempDir + "/override_test.dat"

	diskData := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	err := os.WriteFile(testFile, diskData, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	file, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	memory := Memory{}
	memory.Write(2, []byte("123"), 1000)

	tiered := Tiered{
		sparse: Sparse{size: int64(len(diskData))},
		memory: memory,
		disk:   Disk{file: file},
	}

	data := make([]byte, 7)
	bytesRead, err := tiered.Read(0, data)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if bytesRead != 7 {
		t.Errorf("Expected 7 bytes read, got %d", bytesRead)
	}
	if string(data) != "AB123FG" {
		t.Errorf("Expected 'AB123FG', got %q", string(data))
	}
}

// TestTieredReadSparseWithDisk tests sparse areas with disk fallback
func TestTieredReadSparseWithDisk(t *testing.T) {
	tempDir := t.TempDir()
	testFile := tempDir + "/sparse_test.dat"

	diskData := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	err := os.WriteFile(testFile, diskData, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	file, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	sparse := Sparse{
		size:        int64(len(diskData)),
		sparseAreas: Areas{{Off: 5, Len: 5}}, // Bytes 5-9 are sparse
	}

	tiered := Tiered{
		sparse: sparse,
		memory: Memory{},
		disk:   Disk{file: file},
	}

	data := make([]byte, 10)
	bytesRead, err := tiered.Read(3, data)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if bytesRead != 10 {
		t.Errorf("Expected 10 bytes read, got %d", bytesRead)
	}

	// Expected: "DE" + 5 zeros + "KLM"
	expected := "DE\x00\x00\x00\x00\x00KLM"
	if string(data) != expected {
		t.Errorf("Expected %q, got %q", expected, string(data))
	}
}
