package cache_old2

import (
	"reflect"
	"testing"
)

func TestMemoryTruncate(t *testing.T) {
	tests := []struct {
		name                 string
		initialAreas         DataAreas
		newSize              int64
		expectedAreas        DataAreas
		expectedMemoryChange int // negative = freed, positive = allocated, 0 = no change
	}{
		{
			name:                 "EmptyMemory",
			initialAreas:         DataAreas{},
			newSize:              10,
			expectedAreas:        nil, // Empty slice becomes nil after filtering
			expectedMemoryChange: 0,   // No memory to free
		},
		{
			name:                 "TruncateToZero",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("hello")}, {Off: 10, Data: Bytes("world")}},
			newSize:              0,
			expectedAreas:        nil, // All areas removed
			expectedMemoryChange: -10, // "hello" (5) + "world" (5) = 10 bytes freed
		},
		{
			name:                 "NoChange",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("hello")}},
			newSize:              10,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("hello")}},
			expectedMemoryChange: 0, // No change in memory usage
		},
		{
			name:                 "TruncateCompleteArea",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("hello")}, {Off: 10, Data: Bytes("world")}},
			newSize:              8,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("hello")}},
			expectedMemoryChange: -5, // "world" (5 bytes) removed
		},
		{
			name:                 "TruncatePartialArea",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("hello")}},
			newSize:              3,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("hel")}},
			expectedMemoryChange: -2, // "lo" (2 bytes) truncated from "hello"
		},
		{
			name:                 "TruncateMultipleAreas",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("abc")}, {Off: 5, Data: Bytes("def")}, {Off: 10, Data: Bytes("ghi")}},
			newSize:              7,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("abc")}, {Off: 5, Data: Bytes("de")}},
			expectedMemoryChange: -4, // "f" (1 byte) from "def" + "ghi" (3 bytes) = 4 bytes freed
		},
		{
			name:                 "RemoveAreasBeyondNewSize",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("keep")}, {Off: 10, Data: Bytes("remove1")}, {Off: 20, Data: Bytes("remove2")}},
			newSize:              8,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("keep")}},
			expectedMemoryChange: -14, // "remove1" (7) + "remove2" (7) = 14 bytes freed
		},
		{
			name:                 "TruncateAtAreaBoundary",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("keep")}, {Off: 10, Data: Bytes("remove")}},
			newSize:              10,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("keep")}},
			expectedMemoryChange: -6, // "remove" (6 bytes) freed
		},
		{
			name:                 "TruncateInMiddleOfArea",
			initialAreas:         DataAreas{{Off: 5, Data: Bytes("hello world")}},
			newSize:              10,
			expectedAreas:        DataAreas{{Off: 5, Data: Bytes("hello")}},
			expectedMemoryChange: -6, // " world" (6 bytes) truncated from "hello world"
		},
		{
			name:                 "TruncateWithGaps",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("abc")}, {Off: 10, Data: Bytes("def")}, {Off: 20, Data: Bytes("ghi")}},
			newSize:              15,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("abc")}, {Off: 10, Data: Bytes("def")}},
			expectedMemoryChange: -3, // "ghi" (3 bytes) removed
		},
		{
			name:                 "TruncateLargeFile",
			initialAreas:         DataAreas{{Off: 1000, Data: Bytes("data")}},
			newSize:              500,
			expectedAreas:        nil, // Area completely removed
			expectedMemoryChange: -4,  // "data" (4 bytes) freed
		},
		{
			name:                 "TruncateOverlappingAreas",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("abc")}, {Off: 2, Data: Bytes("defg")}},
			newSize:              4,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("abc")}, {Off: 2, Data: Bytes("de")}},
			expectedMemoryChange: -2, // "fg" (2 bytes) truncated from "defg"
		},
		{
			name:                 "TruncateExactlyAtAreaEnd",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("hello")}},
			newSize:              5,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("hello")}},
			expectedMemoryChange: 0, // No change - truncate at exact end
		},
		{
			name:                 "TruncateAtAreaStart",
			initialAreas:         DataAreas{{Off: 5, Data: Bytes("hello")}},
			newSize:              5,
			expectedAreas:        nil, // Area starts at truncate point, gets removed
			expectedMemoryChange: -5,  // "hello" (5 bytes) removed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memory := &Memory{areas: tt.initialAreas}

			// Calculate initial memory usage for verification
			initialMemoryUsage := memory.calculateMemoryUsage()

			// Perform truncate and capture memory change
			memoryChange := memory.Truncate(tt.newSize)

			// Verify areas are correct
			if !reflect.DeepEqual(memory.areas, tt.expectedAreas) {
				t.Errorf("Truncate() areas = %v, want %v", memory.areas, tt.expectedAreas)
			}

			// Verify memory change is correct
			if memoryChange != tt.expectedMemoryChange {
				t.Errorf("Truncate() memory change = %d, want %d", memoryChange, tt.expectedMemoryChange)
			}

			// Verify consistency: initial + change = final
			finalMemoryUsage := memory.calculateMemoryUsage()
			expectedFinalUsage := initialMemoryUsage + memoryChange
			if finalMemoryUsage != expectedFinalUsage {
				t.Errorf("Memory usage consistency check failed: initial=%d + change=%d = %d, but final=%d",
					initialMemoryUsage, memoryChange, expectedFinalUsage, finalMemoryUsage)
			}
		})
	}
}

func TestMemoryTruncateEdgeCases(t *testing.T) {
	t.Run("NegativeSize", func(t *testing.T) {
		memory := &Memory{areas: DataAreas{{Off: 0, Data: Bytes("test")}}}

		// Truncate to negative size should behave like truncate to 0
		memory.Truncate(-5)

		var expectedAreas DataAreas // nil slice
		if !reflect.DeepEqual(memory.areas, expectedAreas) {
			t.Errorf("Truncate() with negative size: areas = %v, want %v", memory.areas, expectedAreas)
		}
	})

	t.Run("VeryLargeSize", func(t *testing.T) {
		initialAreas := DataAreas{{Off: 0, Data: Bytes("small")}}
		memory := &Memory{areas: initialAreas}

		// Truncate to very large size should not change anything
		memory.Truncate(1000000)

		if !reflect.DeepEqual(memory.areas, initialAreas) {
			t.Errorf("Truncate() with very large size should not change areas: got %v, want %v", memory.areas, initialAreas)
		}
	})

	t.Run("EmptyAreas", func(t *testing.T) {
		memory := &Memory{areas: DataAreas{}}

		memory.Truncate(100)

		var expectedAreas DataAreas // nil slice
		if !reflect.DeepEqual(memory.areas, expectedAreas) {
			t.Errorf("Truncate() on empty areas: areas = %v, want %v", memory.areas, expectedAreas)
		}
	})

	t.Run("ZeroLengthAreas", func(t *testing.T) {
		memory := &Memory{areas: DataAreas{{Off: 5, Data: Bytes("")}}}

		memory.Truncate(3)

		var expectedAreas DataAreas // nil slice
		if !reflect.DeepEqual(memory.areas, expectedAreas) {
			t.Errorf("Truncate() with zero-length areas: areas = %v, want %v", memory.areas, expectedAreas)
		}
	})

	t.Run("PreserveAreasExactly", func(t *testing.T) {
		initialAreas := DataAreas{{Off: 0, Data: Bytes("abc")}, {Off: 5, Data: Bytes("def")}}
		memory := &Memory{areas: initialAreas}

		// Truncate at exactly the end of the last area
		memory.Truncate(8) // Area ends at 5+3=8

		if !reflect.DeepEqual(memory.areas, initialAreas) {
			t.Errorf("Truncate() at exact end should preserve areas: got %v, want %v", memory.areas, initialAreas)
		}
	})
}
