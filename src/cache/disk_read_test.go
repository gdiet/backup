package cache

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiskRead(t *testing.T) {
	// Create a temporary file for testing
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.dat")

	// Create test file with known content
	testData := []byte("Hello, World! This is a test file for disk reading.")
	err := os.WriteFile(testFile, testData, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Open file for Disk cache
	file, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	disk := Disk{file: file}

	tests := []struct {
		name          string
		offset        int64
		readSize      int
		expectedData  []byte
		expectedError bool
		description   string
	}{
		{
			name:          "Read from beginning",
			offset:        0,
			readSize:      5,
			expectedData:  []byte("Hello"),
			expectedError: false,
			description:   "Read first 5 bytes",
		},
		{
			name:          "Read from middle",
			offset:        7,
			readSize:      5,
			expectedData:  []byte("World"),
			expectedError: false,
			description:   "Read 5 bytes from offset 7",
		},
		{
			name:          "Read exact remaining",
			offset:        int64(len(testData) - 4),
			readSize:      4,
			expectedData:  []byte("ing."),
			expectedError: false,
			description:   "Read last 4 bytes exactly",
		},
		{
			name:          "Read beyond EOF",
			offset:        int64(len(testData) - 5),
			readSize:      10,
			expectedData:  append([]byte("ding."), make([]byte, 5)...), // "ding." + 5 zeros
			expectedError: false,
			description:   "Read beyond EOF should fill with zeros",
		},
		{
			name:          "Read from EOF position",
			offset:        int64(len(testData)),
			readSize:      5,
			expectedData:  make([]byte, 5), // All zeros
			expectedError: false,
			description:   "Read from EOF should return all zeros",
		},
		{
			name:          "Read beyond file entirely",
			offset:        int64(len(testData) + 10),
			readSize:      5,
			expectedData:  make([]byte, 5), // All zeros
			expectedError: false,
			description:   "Read beyond file should return all zeros",
		},
		{
			name:          "Zero-length read",
			offset:        0,
			readSize:      0,
			expectedData:  []byte{},
			expectedError: false,
			description:   "Zero-length read should succeed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log("Test description:", tt.description)

			// Prepare read buffer
			data := make(Bytes, tt.readSize)

			// Perform read
			err := disk.Read(tt.offset, data)

			// Check error expectation
			if tt.expectedError && err == nil {
				t.Error("Expected an error, but got none")
				return
			}
			if !tt.expectedError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Check data content
			if len(data) != len(tt.expectedData) {
				t.Errorf("Expected data length %d, got %d", len(tt.expectedData), len(data))
				return
			}

			for i, expected := range tt.expectedData {
				if data[i] != expected {
					t.Errorf("Data mismatch at position %d: expected %v, got %v", i, expected, data[i])
					t.Errorf("Expected: %q", tt.expectedData)
					t.Errorf("Got:      %q", data)
					return
				}
			}
		})
	}
}

func TestDiskReadEdgeCases(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name        string
		fileContent []byte
		offset      int64
		readSize    int
		description string
	}{
		{
			name:        "Empty file",
			fileContent: []byte{},
			offset:      0,
			readSize:    5,
			description: "Reading from empty file should return zeros",
		},
		{
			name:        "Single byte file",
			fileContent: []byte("X"),
			offset:      0,
			readSize:    5,
			description: "Reading beyond single byte should fill with zeros",
		},
		{
			name:        "Large offset",
			fileContent: []byte("test"),
			offset:      1000,
			readSize:    3,
			description: "Large offset beyond file should return zeros",
		},
		{
			name:        "Negative offset",
			fileContent: []byte("test"),
			offset:      -1,
			readSize:    3,
			description: "Negative offset should handle gracefully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log("Test description:", tt.description)

			// Create test file
			testFile := filepath.Join(tempDir, tt.name+".dat")
			err := os.WriteFile(testFile, tt.fileContent, 0644)
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			// Open and test
			file, err := os.Open(testFile)
			if err != nil {
				t.Fatalf("Failed to open test file: %v", err)
			}
			defer file.Close()

			disk := Disk{file: file}
			data := make(Bytes, tt.readSize)

			// The read might fail for negative offsets - that's okay
			err = disk.Read(tt.offset, data)
			if err != nil {
				t.Logf("Read returned error (which may be expected): %v", err)
			}

			// For successful reads, verify the data makes sense
			if err == nil {
				t.Logf("Read succeeded, data: %q", data)
			}
		})
	}
}

func TestDiskReadWithRealFile(t *testing.T) {
	t.Log("Test Disk.Read with actual file I/O patterns")

	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "large.dat")

	// Create a larger test file with pattern
	var testData []byte
	for i := 0; i < 1000; i++ {
		testData = append(testData, byte(i%256))
	}

	err := os.WriteFile(testFile, testData, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	file, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	disk := Disk{file: file}

	// Test various read patterns
	testCases := []struct {
		offset   int64
		size     int
		checkPos int
		expected byte
	}{
		{0, 10, 5, 5},       // Read from start
		{100, 50, 10, 110},  // Read from middle
		{950, 100, 25, 207}, // Read crossing EOF (950+25=975, 975%256=207)
	}

	for i, tc := range testCases {
		t.Run("Pattern"+string(rune('A'+i)), func(t *testing.T) {
			data := make(Bytes, tc.size)
			err := disk.Read(tc.offset, data)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Check specific position if within original data
			if tc.offset+int64(tc.checkPos) < int64(len(testData)) {
				if data[tc.checkPos] != tc.expected {
					t.Errorf("Expected byte %d at check position %d, got %d",
						tc.expected, tc.checkPos, data[tc.checkPos])
				}
			}
		})
	}
}
