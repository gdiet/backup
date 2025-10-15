package cache

import (
	"reflect"
	"testing"
)

func TestMemoryWrite(t *testing.T) {
	tests := []struct {
		name                 string
		initialAreas         DataAreas
		position             int64
		data                 Bytes
		maxMergeSize         int64
		expectedAreas        DataAreas
		expectedMemoryChange int // negative = freed, positive = allocated, 0 = no change
	}{
		{
			name:                 "WriteToEmpty",
			initialAreas:         DataAreas{},
			position:             5,
			data:                 Bytes("hello"),
			maxMergeSize:         100,
			expectedAreas:        DataAreas{{Off: 5, Data: Bytes("hello")}},
			expectedMemoryChange: 5, // "hello" (5 bytes) added
		},
		{
			name:                 "WriteZeroLength",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("existing")}},
			position:             10,
			data:                 Bytes(""),
			maxMergeSize:         100,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("existing")}},
			expectedMemoryChange: 0, // No data written
		},
		{
			name:                 "WriteBeforeExisting",
			initialAreas:         DataAreas{{Off: 10, Data: Bytes("world")}},
			position:             0,
			data:                 Bytes("hello"),
			maxMergeSize:         100,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("hello")}, {Off: 10, Data: Bytes("world")}},
			expectedMemoryChange: 5, // "hello" (5 bytes) added
		},
		{
			name:                 "WriteAfterExisting",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:             10,
			data:                 Bytes("world"),
			maxMergeSize:         100,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("hello")}, {Off: 10, Data: Bytes("world")}},
			expectedMemoryChange: 5, // "world" (5 bytes) added
		},
		{
			name:                 "WriteBetweenAreas",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("a")}, {Off: 10, Data: Bytes("c")}},
			position:             5,
			data:                 Bytes("b"),
			maxMergeSize:         100,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("a")}, {Off: 5, Data: Bytes("b")}, {Off: 10, Data: Bytes("c")}},
			expectedMemoryChange: 1, // "b" (1 byte) added
		},
		{
			name:                 "MergeAdjacent",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:             5,
			data:                 Bytes("world"),
			maxMergeSize:         100,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("helloworld")}},
			expectedMemoryChange: 5, // "world" (5 bytes) added to existing area
		},
		{
			name:                 "MergeOverlapping",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:             3,
			data:                 Bytes("world"),
			maxMergeSize:         100,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("helworld")}},
			expectedMemoryChange: 3, // Net: "helworld"(8) - "hello"(5) = 3 bytes added
		},
		{
			name:                 "NoMergeDueToSize",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:             5,
			data:                 Bytes("world"),
			maxMergeSize:         5, // Too small to merge
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("hello")}, {Off: 5, Data: Bytes("world")}},
			expectedMemoryChange: 5, // "world" (5 bytes) added as separate area
		},
		{
			name:                 "OverwriteExisting",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:             0,
			data:                 Bytes("hi"),
			maxMergeSize:         100,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("hillo")}},
			expectedMemoryChange: 0, // Net: "hillo"(5) - "hello"(5) = 0 bytes change
		},
		{
			name:                 "OverwriteMiddle",
			initialAreas:         DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:             1,
			data:                 Bytes("XX"),
			maxMergeSize:         100,
			expectedAreas:        DataAreas{{Off: 0, Data: Bytes("hXXlo")}},
			expectedMemoryChange: 0, // Net: "hXXlo"(5) - "hello"(5) = 0 bytes change
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memory := &Memory{areas: tt.initialAreas}

			// Calculate initial memory usage for verification
			initialMemoryUsage := memory.calculateMemoryUsage()

			// Perform write and capture memory change
			memoryChange := memory.Write(tt.position, tt.data, tt.maxMergeSize)

			// Verify areas are correct
			if !reflect.DeepEqual(memory.areas, tt.expectedAreas) {
				t.Errorf("Write() areas = %v, want %v", memory.areas, tt.expectedAreas)
			}

			// Verify memory change is correct
			if memoryChange != tt.expectedMemoryChange {
				t.Errorf("Write() memory change = %d, want %d", memoryChange, tt.expectedMemoryChange)
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

func TestMemoryWriteEdgeCases(t *testing.T) {
	t.Run("NegativePosition", func(t *testing.T) {
		memory := &Memory{areas: DataAreas{}}
		memory.Write(-5, Bytes("test"), 100)

		expected := DataAreas{{Off: -5, Data: Bytes("test")}}
		if !reflect.DeepEqual(memory.areas, expected) {
			t.Errorf("Write() with negative position: areas = %v, want %v", memory.areas, expected)
		}
	})

	t.Run("ZeroMaxMergeSize", func(t *testing.T) {
		memory := &Memory{areas: DataAreas{{Off: 0, Data: Bytes("hello")}}}
		memory.Write(5, Bytes("world"), 0) // No merging allowed

		expected := DataAreas{{Off: 0, Data: Bytes("hello")}, {Off: 5, Data: Bytes("world")}}
		if !reflect.DeepEqual(memory.areas, expected) {
			t.Errorf("Write() with zero max merge size: areas = %v, want %v", memory.areas, expected)
		}
	})

	t.Run("LargeData", func(t *testing.T) {
		largeData := make(Bytes, 1000)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		memory := &Memory{areas: DataAreas{}}
		memory.Write(0, largeData, 2000)

		expected := DataAreas{{Off: 0, Data: largeData}}
		if !reflect.DeepEqual(memory.areas, expected) {
			t.Errorf("Write() with large data failed")
		}
	})
}
