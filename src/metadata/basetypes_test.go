package metadata

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestDataEntryFromBytesValid(t *testing.T) {
	// Test with minimal valid data (8 bytes reference count, no areas)
	data := make([]byte, 8)

	// Set refs = 42
	data[7] = 42

	entry, err := dataEntryFromBytes(data)
	if err != nil {
		t.Fatalf("Expected no error for valid data, got: %v", err)
	}

	if entry.refs != 42 {
		t.Errorf("Expected refs=42, got %d", entry.refs)
	}

	if len(entry.areas) != 0 {
		t.Errorf("Expected 0 areas, got %d", len(entry.areas))
	}
}

func TestDataEntryFromBytesWithAreas(t *testing.T) {
	// Test with 2 areas (8 + 2*16 = 40 bytes)
	data := make([]byte, 40)

	// Set refs = 123
	data[7] = 123

	// First area: off=1000, len=500
	pos := 8
	data[pos+7] = 232  // 1000 in big endian (low byte)
	data[pos+6] = 3    // 1000 >> 8
	data[pos+15] = 244 // 500 in big endian (low byte)
	data[pos+14] = 1   // 500 >> 8

	// Second area: off=2000, len=750
	pos = 24
	data[pos+7] = 208  // 2000 in big endian (low byte)
	data[pos+6] = 7    // 2000 >> 8
	data[pos+15] = 238 // 750 in big endian (low byte)
	data[pos+14] = 2   // 750 >> 8

	entry, err := dataEntryFromBytes(data)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if entry.refs != 123 {
		t.Errorf("Expected refs=123, got %d", entry.refs)
	}

	if len(entry.areas) != 2 {
		t.Fatalf("Expected 2 areas, got %d", len(entry.areas))
	}

	if entry.areas[0].off != 1000 || entry.areas[0].len != 500 {
		t.Errorf("Area 0: expected off=1000, len=500, got off=%d, len=%d",
			entry.areas[0].off, entry.areas[0].len)
	}

	if entry.areas[1].off != 2000 || entry.areas[1].len != 750 {
		t.Errorf("Area 1: expected off=2000, len=750, got off=%d, len=%d",
			entry.areas[1].off, entry.areas[1].len)
	}
}

func TestDataEntryFromBytesTooShort(t *testing.T) {
	// Test with data too short (< 8 bytes)
	data := make([]byte, 0)

	_, err := dataEntryFromBytes(data)
	if err == nil {
		t.Fatal("Expected error for data too short, got nil")
	}

	if err.Error() != "dataEntry length invalid" {
		t.Errorf("Expected 'dataEntry length invalid', got: %v", err)
	}
}

func TestDataEntryFromBytesInvalidLength(t *testing.T) {
	// Test with invalid length (not 8 + n*16)
	data := make([]byte, 41) // Should be 40, 56, 72, etc.

	_, err := dataEntryFromBytes(data)
	if err == nil {
		t.Fatal("Expected error for invalid length, got nil")
	}

	if err.Error() != "dataEntry length invalid" {
		t.Errorf("Expected 'dataEntry length invalid', got: %v", err)
	}
}

func TestDataEntryToBytes(t *testing.T) {
	// Create a test entry
	entry := dataEntry{
		refs: 456,
		areas: []area{
			{off: 1000, len: 500},
			{off: 2000, len: 750},
		},
	}

	data := entry.toBytes()

	// Verify length
	expectedLen := 8 + 2*16 // 40 bytes
	if len(data) != expectedLen {
		t.Fatalf("Expected %d bytes, got %d", expectedLen, len(data))
	}

	// Verify refs
	refs := uint64(data[7]) | (uint64(data[6]) << 8) // big endian
	if refs != 456 {
		t.Errorf("Expected refs=456, got %d", refs)
	}
}

func TestDataEntryRoundTrip(t *testing.T) {
	// Test that toBytes() followed by dataEntryFromBytes() preserves data
	original := dataEntry{
		refs: 12345,
		areas: []area{
			{off: 100, len: 200},
			{off: 500, len: 300},
			{off: 1000, len: 150},
		},
	}

	// Convert to bytes and back
	data := original.toBytes()
	restored, err := dataEntryFromBytes(data)
	if err != nil {
		t.Fatal("Round-trip failed:", err)
	}

	// Compare
	if restored.refs != original.refs {
		t.Errorf("Refs mismatch: expected %d, got %d", original.refs, restored.refs)
	}

	if len(restored.areas) != len(original.areas) {
		t.Fatalf("Areas length mismatch: expected %d, got %d", len(original.areas), len(restored.areas))
	}

	for i, area := range restored.areas {
		if area.off != original.areas[i].off || area.len != original.areas[i].len {
			t.Errorf("Area %d mismatch: expected off=%d, len=%d, got off=%d, len=%d",
				i, original.areas[i].off, original.areas[i].len, area.off, area.len)
		}
	}
}

func TestDataEntryEdgeCases(t *testing.T) {
	t.Run("zero refs and empty areas", func(t *testing.T) {
		entry := dataEntry{refs: 0, areas: []area{}}

		data := entry.toBytes()
		restored, err := dataEntryFromBytes(data)
		if err != nil {
			t.Fatal("Failed to process zero entry:", err)
		}

		if restored.refs != 0 {
			t.Errorf("Expected refs=0, got %d", restored.refs)
		}

		if len(restored.areas) != 0 {
			t.Errorf("Expected no areas, got %d", len(restored.areas))
		}
	})

	t.Run("maximum values", func(t *testing.T) {
		entry := dataEntry{
			refs: ^uint64(0), // Max uint64
			areas: []area{
				{off: 9223372036854775807, len: 9223372036854775807}, // Max int64
			},
		}

		data := entry.toBytes()
		restored, err := dataEntryFromBytes(data)
		if err != nil {
			t.Fatal("Failed to process max values:", err)
		}

		if restored.refs != ^uint64(0) {
			t.Error("Max refs value not preserved")
		}

		if restored.areas[0].off != 9223372036854775807 || restored.areas[0].len != 9223372036854775807 {
			t.Error("Max area values not preserved")
		}
	})
}

func TestDirEntryToBytes(t *testing.T) {
	entry := dirEntry{name: "testdir"}

	data := entry.toBytes()

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
	name := string(data[1:])
	if name != "testdir" {
		t.Errorf("Expected name 'testdir', got '%s'", name)
	}
}

func TestFileEntryToBytes(t *testing.T) {
	entry := fileEntry{
		time: 1699123456789, // Example Unix milliseconds
		dref: [40]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		name: "test.txt",
	}

	data := entry.toBytes()

	// Verify structure: 1 byte type + 8 time + 8 dataID + name
	expectedLen := 1 + 8 + 40 + len("test.txt")
	if len(data) != expectedLen {
		t.Fatalf("Expected %d bytes, got %d", expectedLen, len(data))
	}

	// Verify type byte
	if data[0] != 1 {
		t.Errorf("Expected type byte 1, got %d", data[0])
	}

	// Verify time
	time := int64(binary.BigEndian.Uint64(data[1:]))
	if time != 1699123456789 {
		t.Errorf("Expected time 1699123456789, got %d", time)
	}

	// Verify dref
	dref := data[9:49]
	if !reflect.DeepEqual(dref, entry.dref[:]) {
		t.Errorf("Unexpected data reference, got %d", dref)
	}

	// Verify name
	name := string(data[49:])
	if name != "test.txt" {
		t.Errorf("Expected name 'test.txt', got '%s'", name)
	}
}

func TestTreeEntryFromBytesDirectory(t *testing.T) {
	// Create test data for directory
	data := []byte{0} // type byte 0
	data = append(data, []byte("documents")...)

	result, err := treeEntryFromBytes(data)
	if err != nil {
		t.Fatal("Expected no error for valid directory data:", err)
	}

	dirEntry, ok := result.(dirEntry)
	if !ok {
		t.Fatal("Expected dirEntry, got different type")
	}

	if dirEntry.name != "documents" {
		t.Errorf("Expected name 'documents', got '%s'", dirEntry.name)
	}
}

func TestTreeEntryFromBytesFile(t *testing.T) {
	// Create test data for file: type(1) + time(8) + dref(40) + name
	data := make([]byte, 1+8+40+len("readme.md"))
	data[0] = 1 // type byte 1

	// Set time = 1699123456789
	binary.BigEndian.PutUint64(data[1:], 1699123456789)

	// Set dref = 42, 0, 0, ...
	data[9] = 42

	// Set name
	copy(data[49:], []byte("readme.md"))

	result, err := treeEntryFromBytes(data)
	if err != nil {
		t.Fatal("Expected no error for valid file data:", err)
	}

	fileEntry, ok := result.(fileEntry)
	if !ok {
		t.Fatal("Expected fileEntry, got different type")
	}

	if fileEntry.time != 1699123456789 {
		t.Errorf("Expected time 1699123456789, got %d", fileEntry.time)
	}

	// Verify dref
	dref := data[9:49]
	if !reflect.DeepEqual(dref, fileEntry.dref[:]) {
		t.Errorf("Unexpected data reference, got %d", dref)
	}

	if fileEntry.name != "readme.md" {
		t.Errorf("Expected name 'readme.md', got '%s'", fileEntry.name)
	}
}

func TestTreeEntryFromBytesErrors(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expectedErr string
	}{
		{
			name:        "too short",
			data:        []byte{0},
			expectedErr: "treeEntry too short",
		},
		{
			name:        "empty data",
			data:        []byte{},
			expectedErr: "treeEntry too short",
		},
		{
			name:        "invalid type",
			data:        []byte{2, 't', 'e', 's', 't'},
			expectedErr: "invalid treeEntry type",
		},
		{
			name:        "file too short",
			data:        []byte{1, 0, 0, 0, 0, 0, 0, 0, 0}, // Only 9 bytes, need 18
			expectedErr: "fileEntry too short",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := treeEntryFromBytes(tt.data)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}

			if err.Error() != tt.expectedErr {
				t.Errorf("Expected error '%s', got '%s'", tt.expectedErr, err.Error())
			}
		})
	}
}

func TestTreeEntryRoundTrip(t *testing.T) {
	t.Run("directory round trip", func(t *testing.T) {
		original := dirEntry{name: "my-directory"}

		// Serialize and deserialize
		data := original.toBytes()
		result, err := treeEntryFromBytes(data)
		if err != nil {
			t.Fatal("Directory round-trip failed:", err)
		}

		restored, ok := result.(dirEntry)
		if !ok {
			t.Fatal("Expected dirEntry after round-trip")
		}

		if restored.name != original.name {
			t.Errorf("Name mismatch: expected '%s', got '%s'", original.name, restored.name)
		}
	})

	t.Run("file round trip", func(t *testing.T) {
		original := fileEntry{
			time: 1699987654321,
			dref: [40]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			name: "important-file.log",
		}

		// Serialize and deserialize
		data := original.toBytes()
		result, err := treeEntryFromBytes(data)
		if err != nil {
			t.Fatal("File round-trip failed:", err)
		}

		restored, ok := result.(fileEntry)
		if !ok {
			t.Fatal("Expected fileEntry after round-trip")
		}

		if restored.time != original.time {
			t.Errorf("Time mismatch: expected %d, got %d", original.time, restored.time)
		}

		if !reflect.DeepEqual(restored.dref, original.dref) {
			t.Errorf("DataID mismatch: expected %d, got %d", original.dref, restored.dref)
		}

		if restored.name != original.name {
			t.Errorf("Name mismatch: expected '%s', got '%s'", original.name, restored.name)
		}
	})
}
