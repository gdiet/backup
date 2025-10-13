package cache

import (
	"reflect"
	"testing"
)

func TestMemoryWrite(t *testing.T) {
	tests := []struct {
		name          string
		initialAreas  DataAreas
		position      int64
		data          Bytes
		maxMergeSize  int64
		expectedAreas DataAreas
	}{
		{
			name:          "WriteToEmpty",
			initialAreas:  DataAreas{},
			position:      5,
			data:          Bytes("hello"),
			maxMergeSize:  100,
			expectedAreas: DataAreas{{Off: 5, Data: Bytes("hello")}},
		},
		{
			name:          "WriteZeroLength",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("existing")}},
			position:      10,
			data:          Bytes(""),
			maxMergeSize:  100,
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("existing")}},
		},
		{
			name:          "WriteBeforeExisting",
			initialAreas:  DataAreas{{Off: 10, Data: Bytes("world")}},
			position:      0,
			data:          Bytes("hello"),
			maxMergeSize:  100,
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("hello")}, {Off: 10, Data: Bytes("world")}},
		},
		{
			name:          "WriteAfterExisting",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:      10,
			data:          Bytes("world"),
			maxMergeSize:  100,
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("hello")}, {Off: 10, Data: Bytes("world")}},
		},
		{
			name:          "WriteBetweenAreas",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("a")}, {Off: 10, Data: Bytes("c")}},
			position:      5,
			data:          Bytes("b"),
			maxMergeSize:  100,
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("a")}, {Off: 5, Data: Bytes("b")}, {Off: 10, Data: Bytes("c")}},
		},
		{
			name:          "MergeAdjacent",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:      5,
			data:          Bytes("world"),
			maxMergeSize:  100,
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("helloworld")}},
		},
		{
			name:          "MergeOverlapping",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:      3,
			data:          Bytes("world"),
			maxMergeSize:  100,
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("helworld")}},
		},
		{
			name:          "NoMergeDueToSize",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:      5,
			data:          Bytes("world"),
			maxMergeSize:  5, // Too small to merge
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("hello")}, {Off: 5, Data: Bytes("world")}},
		},
		{
			name:          "OverwriteExisting",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:      0,
			data:          Bytes("hi"),
			maxMergeSize:  100,
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("hillo")}},
		},
		{
			name:          "OverwriteMiddle",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("hello")}},
			position:      1,
			data:          Bytes("XX"),
			maxMergeSize:  100,
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("hXXlo")}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memory := &Memory{areas: tt.initialAreas}
			memory.Write(tt.position, tt.data, tt.maxMergeSize)

			if !reflect.DeepEqual(memory.areas, tt.expectedAreas) {
				t.Errorf("Write() areas = %v, want %v", memory.areas, tt.expectedAreas)
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
