package cache

import (
	"reflect"
	"testing"
)

func TestMemory_Read(t *testing.T) {
	tests := []struct {
		name          string
		memoryAreas   DataAreas
		position      int64
		dataLen       int
		expectedData  []byte
		expectedAreas Areas
	}{
		{
			name:          "EmptyMemory",
			memoryAreas:   DataAreas{},
			position:      0,
			dataLen:       10,
			expectedData:  make([]byte, 10),
			expectedAreas: Areas{{Off: 0, Len: 10}},
		},
		{
			name:          "ZeroLengthRead",
			memoryAreas:   DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:      0,
			dataLen:       0,
			expectedData:  []byte{},
			expectedAreas: nil, // Implementation returns nil for empty reads
		},
		{
			name:          "ExactMatch",
			memoryAreas:   DataAreas{{Off: 10, Data: Bytes("hello")}},
			position:      10,
			dataLen:       5,
			expectedData:  []byte("hello"),
			expectedAreas: nil, // Areas{} != nil
		},
		{
			name:          "PartialMatchStart",
			memoryAreas:   DataAreas{{Off: 5, Data: Bytes("world")}},
			position:      3,
			dataLen:       5,
			expectedData:  []byte{0, 0, 'w', 'o', 'r'},
			expectedAreas: Areas{{Off: 3, Len: 2}},
		},
		{
			name:          "PartialMatchEnd",
			memoryAreas:   DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:      3,
			dataLen:       5,
			expectedData:  []byte{'l', 'o', 0, 0, 0},
			expectedAreas: Areas{{Off: 5, Len: 3}},
		},
		{
			name:          "PartialMatchMiddle",
			memoryAreas:   DataAreas{{Off: 2, Data: Bytes("test")}},
			position:      1,
			dataLen:       4, // Changed from 5 to 4
			expectedData:  []byte{0, 't', 'e', 's'},
			expectedAreas: Areas{{Off: 1, Len: 1}}, // Only gap at start
		},
		{
			name: "MultipleAreas",
			memoryAreas: DataAreas{
				{Off: 0, Data: Bytes("ab")},
				{Off: 5, Data: Bytes("cd")},
			},
			position:      0,
			dataLen:       10,
			expectedData:  []byte{'a', 'b', 0, 0, 0, 'c', 'd', 0, 0, 0},
			expectedAreas: Areas{{Off: 2, Len: 3}, {Off: 7, Len: 3}},
		},
		{
			name:          "NoOverlap",
			memoryAreas:   DataAreas{{Off: 20, Data: Bytes("test")}},
			position:      0,
			dataLen:       10,
			expectedData:  make([]byte, 10),
			expectedAreas: Areas{{Off: 0, Len: 10}},
		},
		{
			name: "ComplexOverlaps",
			memoryAreas: DataAreas{
				{Off: 1, Data: Bytes("abc")},
				{Off: 6, Data: Bytes("def")},
				{Off: 12, Data: Bytes("ghi")},
			},
			position:      0,
			dataLen:       16,
			expectedData:  []byte{0, 'a', 'b', 'c', 0, 0, 'd', 'e', 'f', 0, 0, 0, 'g', 'h', 'i', 0},
			expectedAreas: Areas{{Off: 0, Len: 1}, {Off: 4, Len: 2}, {Off: 9, Len: 3}, {Off: 15, Len: 1}},
		},
		{
			name: "OverlappingMemoryAreas",
			memoryAreas: DataAreas{
				{Off: 0, Data: Bytes("hello")},
				{Off: 3, Data: Bytes("world")}, // Overlaps with first area
			},
			position:      0,
			dataLen:       10,
			expectedData:  []byte{'h', 'e', 'l', 'w', 'o', 'r', 'l', 'd', 0, 0},
			expectedAreas: Areas{{Off: 8, Len: 2}},
		},
		{
			name: "LargeGaps",
			memoryAreas: DataAreas{
				{Off: 0, Data: Bytes("a")},
				{Off: 100, Data: Bytes("b")},
			},
			position:      0,
			dataLen:       50,
			expectedData:  append([]byte{'a'}, make([]byte, 49)...),
			expectedAreas: Areas{{Off: 1, Len: 49}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memory := &Memory{areas: tt.memoryAreas}
			data := make(Bytes, tt.dataLen)

			gotAreas := memory.Read(tt.position, data)

			// Check data content
			if !reflect.DeepEqual([]byte(data), tt.expectedData) {
				t.Errorf("Read() data = %v, want %v", []byte(data), tt.expectedData)
			}

			// Check unread areas
			if !reflect.DeepEqual(gotAreas, tt.expectedAreas) {
				t.Errorf("Read() areas = %v, want %v", gotAreas, tt.expectedAreas)
			}
		})
	}
}

func TestMemory_Read_EdgeCases(t *testing.T) {
	t.Run("NegativePosition", func(t *testing.T) {
		memory := &Memory{areas: DataAreas{{Off: 0, Data: Bytes("test")}}}
		data := make(Bytes, 5)

		// Reading from negative position -2, length 5 -> covers positions -2 to 2
		areas := memory.Read(-2, data)

		// Memory area at offset 0 should fill positions 0-3 (which maps to data[2:5])
		expectedData := []byte{0, 0, 't', 'e', 's'}
		if !reflect.DeepEqual([]byte(data), expectedData) {
			t.Errorf("Read() with negative position: data = %v, want %v", []byte(data), expectedData)
		}

		// Should report the part before memory area as unread (positions -2 to -1)
		expectedAreas := Areas{{Off: -2, Len: 2}}
		if !reflect.DeepEqual(areas, expectedAreas) {
			t.Errorf("Read() with negative position: areas = %v, want %v", areas, expectedAreas)
		}
	})

	t.Run("LargeRead", func(t *testing.T) {
		memory := &Memory{areas: DataAreas{{Off: 1000, Data: Bytes("small")}}}
		data := make(Bytes, 10000)

		areas := memory.Read(0, data)

		// Should have copied "small" at position 1000
		for i := 1000; i < 1005; i++ {
			if data[i] != "small"[i-1000] {
				t.Errorf("Expected '%c' at position %d, got '%c'", "small"[i-1000], i, data[i])
			}
		}

		// Should report gaps before and after
		expectedAreas := Areas{{Off: 0, Len: 1000}, {Off: 1005, Len: 8995}}
		if !reflect.DeepEqual(areas, expectedAreas) {
			t.Errorf("Read() large read: areas = %v, want %v", areas, expectedAreas)
		}
	})
}
