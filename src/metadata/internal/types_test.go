package internal

import (
	"math"
	"testing"
)

func TestTreeEntrySetName(t *testing.T) {
	var entry TreeEntry

	entry = NewDirEntry("old long name")
	entry.SetName("new name")
	if entry.Name() != "new name" {
		t.Errorf("Expected directory name 'new name', got '%s'", entry.Name())
	}

	entry = NewFileEntry("old file.txt", 0, [40]byte{})
	entry.SetName("new longer file.txt")
	if entry.Name() != "new longer file.txt" {
		t.Errorf("Expected file name 'new longer file.txt', got '%s'", entry.Name())
	}
}

func TestDirEntryToBytes(t *testing.T) {
	entry := NewDirEntry("testdir")
	data := entry.ToBytes()

	// Verify structure: 1 byte type + name
	expectedLen := 1 + len("testdir")
	if len(data) != expectedLen {
		t.Fatalf("Expected %d bytes, got %d", expectedLen, len(data))
	}

	// Verify type byte
	if data[0] != 0 {
		t.Errorf("Expected type byte 0, got %d", data[0])
	}

	// Verify name
	if string(data[1:]) != "testdir" {
		t.Errorf("Expected name 'testdir', got '%s'", string(data[1:]))
	}
}

func TestFileEntryToBytes(t *testing.T) {
	entry := NewFileEntry("test.txt", 1640995200000, [40]byte{1, 2, 3})

	data := entry.ToBytes()

	// Verify structure: 1 byte type + 8 bytes time + 40 bytes dref + name
	expectedLen := 1 + 8 + 40 + len("test.txt")
	if len(data) != expectedLen {
		t.Fatalf("Expected %d bytes, got %d", expectedLen, len(data))
	}

	// Verify type byte
	if data[0] != 1 {
		t.Errorf("Expected type byte 1, got %d", data[0])
	}

	// Verify time (bytes 1-8)
	restoredTime := B64i(data[1:9])
	if restoredTime != 1640995200000 {
		t.Errorf("Expected time 1640995200000, got %d", restoredTime)
	}

	// Verify name at the end
	if string(data[49:]) != "test.txt" {
		t.Errorf("Expected name 'test.txt', got '%s'", string(data[49:]))
	}
}

func TestTreeEntryFromBytesDirectory(t *testing.T) {
	data := append([]byte{0}, []byte("documents")...) // type 0 + name

	result, err := treeEntryFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to parse directory entry: %v", err)
	}

	if result.Name() != "documents" {
		t.Errorf("Expected name 'documents', got '%s'", result.Name())
	}

	_, ok := result.(*DirEntry)
	if !ok {
		t.Fatal("Expected *DirEntry, got different type")
	}

}

func TestTreeEntryFromBytesFile(t *testing.T) {
	// Create test data for file: type(1) + time(8) + dref(40) + name
	fileName := "readme.md"
	data := make([]byte, 1+8+40+len(fileName))
	data[0] = 1                    // type 1
	I64w(data[1:9], 1640995200000) // time
	U64w(data[9:17], 7631)         // size in dref
	// hash bytes can be zero for this test
	copy(data[49:], []byte(fileName)) // file name

	result, err := treeEntryFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to parse file entry: %v", err)
	}

	if result.Name() != fileName {
		t.Errorf("Expected name '%s', got '%s'", fileName, result.Name())
	}

	fileEntry, ok := result.(*FileEntry)
	if !ok {
		t.Fatal("Expected *FileEntry, got different type")
	}

	if fileEntry.Time() != 1640995200000 {
		t.Errorf("Expected time 1640995200000, got %d", fileEntry.Time())
	}

	if fileEntry.Size() != 7631 {
		t.Errorf("Expected size 7631, got %d", fileEntry.Size())
	}

}

func TestDataEntryRoundtrip(t *testing.T) {
	original := DataEntry{
		Refs: 5,
		Areas: []Area{
			{Off: 0, Len: 1024},
			{Off: 2048, Len: 512},
		},
	}

	data := original.ToBytes()

	restored, err := dataEntryFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to parse data entry: %v", err)
	}

	if restored.Refs != original.Refs {
		t.Errorf("Refs mismatch: expected %d, got %d", original.Refs, restored.Refs)
	}

	if len(restored.Areas) != len(original.Areas) {
		t.Errorf("Areas length mismatch: expected %d, got %d", len(original.Areas), len(restored.Areas))
	}

	for i, area := range original.Areas {
		if restored.Areas[i].Off != area.Off || restored.Areas[i].Len != area.Len {
			t.Errorf("Area %d mismatch: expected {%d, %d}, got {%d, %d}",
				i, area.Off, area.Len, restored.Areas[i].Off, restored.Areas[i].Len)
		}
	}
}

func TestDataEntryMaxValues(t *testing.T) {
	entry := DataEntry{
		Refs: math.MaxUint64,
		Areas: []Area{
			{Off: math.MaxUint64, Len: math.MaxUint64},
		},
	}

	data := entry.ToBytes()
	restored, err := dataEntryFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to handle max values: %v", err)
	}

	if restored.Refs != math.MaxUint64 {
		t.Error("Max refs value not preserved")
	}

	if restored.Areas[0].Off != math.MaxUint64 || restored.Areas[0].Len != math.MaxUint64 {
		t.Error("Max area values not preserved")
	}
}

// Test error cases for DataEntryFromBytes
func TestDataEntryFromBytesErrors(t *testing.T) {
	t.Run("TooShort", func(t *testing.T) {
		// Less than 8 bytes
		data := []byte{1, 2, 3}
		_, err := dataEntryFromBytes(data)
		if err == nil {
			t.Error("Expected error for data too short")
		}
		expectedMsg := "DeserializationError: dataEntry length invalid"
		if err.Error() != expectedMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
		}
	})

	t.Run("InvalidLength", func(t *testing.T) {
		// Length not matching 8 + 16*n format (e.g., 15 bytes = 8 + 7, not divisible by 16)
		data := make([]byte, 15)
		_, err := dataEntryFromBytes(data)
		if err == nil {
			t.Error("Expected error for invalid length")
		}
		expectedMsg := "DeserializationError: dataEntry length invalid"
		if err.Error() != expectedMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
		}
	})
}

// Test error cases for treeEntryFromBytes
func TestTreeEntryFromBytesErrors(t *testing.T) {
	t.Run("EmptyData", func(t *testing.T) {
		data := []byte{}
		_, err := treeEntryFromBytes(data)
		if err == nil {
			t.Error("Expected error for empty data")
		}
		expectedMsg := "DeserializationError: treeEntry too short"
		if err.Error() != expectedMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
		}
	})

	t.Run("TooShort", func(t *testing.T) {
		data := []byte{1} // Only 1 byte
		_, err := treeEntryFromBytes(data)
		if err == nil {
			t.Error("Expected error for data too short")
		}
		expectedMsg := "DeserializationError: treeEntry too short"
		if err.Error() != expectedMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
		}
	})

	t.Run("FileEntryTooShort", func(t *testing.T) {
		// Type 1 (file) but less than 50 bytes required
		data := make([]byte, 49) // One byte short
		data[0] = 1
		_, err := treeEntryFromBytes(data)
		if err == nil {
			t.Error("Expected error for file entry too short")
		}
		expectedMsg := "DeserializationError: fileEntry too short"
		if err.Error() != expectedMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
		}
	})

	t.Run("InvalidEntryType", func(t *testing.T) {
		data := []byte{2, 't', 'e', 's', 't'} // Type 2 is invalid
		_, err := treeEntryFromBytes(data)
		if err == nil {
			t.Error("Expected error for invalid entry type")
		}
		expectedMsg := "DeserializationError: invalid treeEntry type"
		if err.Error() != expectedMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
		}
	})
}

// Test empty areas in DataEntry
func TestDataEntryEmpty(t *testing.T) {
	entry := DataEntry{
		Refs:  0,
		Areas: []Area{}, // Empty areas
	}

	data := entry.ToBytes()
	if len(data) != 8 { // Should be just the 8 bytes for Refs
		t.Errorf("Expected 8 bytes for empty areas, got %d", len(data))
	}

	restored, err := dataEntryFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to parse empty data entry: %v", err)
	}

	if restored.Refs != 0 {
		t.Errorf("Expected Refs 0, got %d", restored.Refs)
	}

	if len(restored.Areas) != 0 {
		t.Errorf("Expected 0 areas, got %d", len(restored.Areas))
	}
}
