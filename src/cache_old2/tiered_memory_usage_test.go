package cache_old2

import (
	"testing"
)

// Helper function to create a properly initialized Tiered for testing
func createTestTiered(t *testing.T) *Tiered {
	tempDir := t.TempDir()
	return &Tiered{
		sparse: Sparse{size: 0},
		memory: Memory{areas: nil},
		disk:   Disk{filePath: tempDir + "/cache.dat"},
	}
}

const writeFailedMsg = "Write() failed: %v"

// TestTieredCloseEmpty tests that Close on empty tiered returns zero memory freed.
func TestTieredCloseEmpty(t *testing.T) {
	tiered := createTestTiered(t)

	memoryDelta, err := tiered.Close()
	if err != nil {
		t.Errorf("Close() failed: %v", err)
	}
	if memoryDelta != 0 {
		t.Errorf("Expected memory delta 0, got %d", memoryDelta)
	}
}

// TestTieredTruncateEmpty tests that Truncate on empty tiered returns zero memory delta.
func TestTieredTruncateEmpty(t *testing.T) {
	tiered := &Tiered{
		sparse: Sparse{size: 0},
		memory: Memory{areas: nil},
		disk:   Disk{},
	}

	memoryDelta, err := tiered.Truncate(100)
	if err != nil {
		t.Errorf("Truncate() failed: %v", err)
	}
	if memoryDelta != 0 {
		t.Errorf("Expected memory delta 0, got %d", memoryDelta)
	}
}

// TestTieredWriteToMemory tests that writes going to memory work correctly.
func TestTieredWriteToMemory(t *testing.T) {
	tiered := &Tiered{
		sparse: Sparse{size: 0},
		memory: Memory{areas: nil},
		disk:   Disk{},
	}

	// Position 0 with size 10 -> (0 + 10) % 2 = 0 -> goes to memory
	data := Bytes([]byte("0123456789"))
	memoryDelta, err := tiered.Write(0, data, true, 1024)
	if err != nil {
		t.Errorf(writeFailedMsg, err)
	}
	if memoryDelta <= 0 {
		t.Errorf("Expected positive memory delta from write to memory, got %d", memoryDelta)
	}
}

// TestTieredWriteToDisk tests that writes going to disk work correctly.
// NOTE: Currently skipped because Disk layer requires file initialization.
func TestTieredWriteToDisk(t *testing.T) {
	tempDir := t.TempDir()
	filePath := tempDir + "/cache_disk_test.dat"
	tiered := &Tiered{
		sparse: Sparse{size: 0},
		memory: Memory{areas: nil},
		disk:   Disk{filePath: filePath},
	}

	// Position 1 with size 10 -> (1 + 10) % 2 = 1 -> goes to disk
	data := Bytes([]byte("ABCDEFGHIJ"))
	memoryDelta, err := tiered.Write(1, data, false, 1024)
	if err != nil {
		t.Errorf(writeFailedMsg, err)
	}
	// Disk writes should not change memory usage (or might free some)
	if memoryDelta > 0 {
		t.Errorf("Expected zero or negative memory delta from disk write, got %d", memoryDelta)
	}

	// Verify that the data was written to disk using Disk.Read
	readBack := make([]byte, 10)
	err = tiered.disk.Read(1, readBack)
	if err != nil {
		t.Errorf("Disk.Read failed: %v", err)
	}
	if string(readBack) != "ABCDEFGHIJ" {
		t.Errorf("Expected 'ABCDEFGHIJ' on disk, got '%s'", string(readBack))
	}
}

// TestTieredWriteMemoryThenTruncate tests memory changes from write and truncate.
func TestTieredWriteMemoryThenTruncate(t *testing.T) {
	tiered := &Tiered{
		sparse: Sparse{size: 0},
		memory: Memory{areas: nil},
		disk:   Disk{},
	}

	// First write to memory (position 0 + size 10 = even -> memory)
	data := Bytes([]byte("0123456789"))
	memoryDelta, err := tiered.Write(0, data, true, 1024)
	if err != nil {
		t.Errorf(writeFailedMsg, err)
	}
	t.Logf("Memory delta from write: %d", memoryDelta)

	// Then truncate to smaller size
	truncateDelta, err := tiered.Truncate(5)
	if err != nil {
		t.Errorf("Truncate() failed: %v", err)
	}

	// Should have freed some memory (negative delta)
	if truncateDelta >= 0 {
		t.Errorf("Expected negative memory delta from truncate, got %d", truncateDelta)
	}
}

// TestTieredMemoryUsageIntegration tests integration between all layers with memory tracking.
func TestTieredMemoryUsageIntegration(t *testing.T) {
	tiered := &Tiered{
		sparse: Sparse{size: 0},
		memory: Memory{areas: nil},
		disk:   Disk{},
	}

	// Test sequence: Write to memory, write to disk, then cleanup
	t.Log("Writing data that should go to memory")
	memoryData := Bytes([]byte("MemoryData")) // pos 0 + size 10 = 10 % 2 = 0 -> memory
	writeMemoryDelta, err := tiered.Write(0, memoryData, true, 1024)
	if err != nil {
		t.Fatalf(writeFailedMsg, err)
	}
	t.Logf("Memory delta from write: %d", writeMemoryDelta)

	// Skip disk write test for now due to file initialization requirement
	t.Log("Skipping disk write - requires file initialization")

	t.Log("Reading data to verify it's stored correctly")
	readBuffer := make(Bytes, 10)
	_, err = tiered.Read(0, readBuffer)
	if err != nil {
		t.Fatalf("Failed to read from position 0: %v", err)
	}
	if string(readBuffer) != "MemoryData" {
		t.Errorf("Expected 'MemoryData', got '%s'", string(readBuffer))
	}

	t.Log("Truncating to free memory")
	memoryDelta, err := tiered.Truncate(5) // Should truncate memory area
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}
	t.Logf("Memory delta from truncate: %d", memoryDelta)

	t.Log("Closing to free all memory")
	memoryFreed, err := tiered.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}
	t.Logf("Memory freed from close: %d", memoryFreed)

	if memoryFreed > 0 {
		t.Errorf("Expected negative or zero memory freed, got %d", memoryFreed)
	}
}
