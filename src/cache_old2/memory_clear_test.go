package cache_old2

import (
	"reflect"
	"testing"
)

func TestMemoryClear(t *testing.T) {
	tests := []struct {
		name          string
		initialAreas  DataAreas
		clearArea     Area
		expectedAreas DataAreas
		expectedDelta int
	}{
		{
			name:          "Clear nothing (empty memory)",
			initialAreas:  DataAreas{},
			clearArea:     Area{Off: 0, Len: 10},
			expectedAreas: DataAreas{},
			expectedDelta: 0,
		},
		{
			name:          "Clear full overlap",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("hello")}},
			clearArea:     Area{Off: 0, Len: 5},
			expectedAreas: DataAreas{},
			expectedDelta: -5,
		},
		{
			name:          "Clear partial overlap left",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("hello")}},
			clearArea:     Area{Off: 0, Len: 2},
			expectedAreas: DataAreas{{Off: 2, Data: Bytes("llo")}},
			expectedDelta: -2,
		},
		{
			name:          "Clear partial overlap right",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("hello")}},
			clearArea:     Area{Off: 3, Len: 2},
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("hel")}},
			expectedDelta: -2,
		},
		{
			name:          "Clear middle of area",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("abcdef")}},
			clearArea:     Area{Off: 2, Len: 2},
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("ab")}, {Off: 4, Data: Bytes("ef")}},
			expectedDelta: -2,
		},
		{
			name:          "Clear multiple areas",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("abc")}, {Off: 10, Data: Bytes("def")}},
			clearArea:     Area{Off: 0, Len: 20},
			expectedAreas: DataAreas{},
			expectedDelta: -6,
		},
		{
			name:          "Clear non-overlapping area",
			initialAreas:  DataAreas{{Off: 0, Data: Bytes("abc")}, {Off: 10, Data: Bytes("def")}},
			clearArea:     Area{Off: 5, Len: 2},
			expectedAreas: DataAreas{{Off: 0, Data: Bytes("abc")}, {Off: 10, Data: Bytes("def")}},
			expectedDelta: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memory := &Memory{areas: tt.initialAreas}
			delta := memory.Clear(tt.clearArea)
			if !reflect.DeepEqual(memory.areas, tt.expectedAreas) {
				t.Errorf("areas mismatch: got %v, want %v", memory.areas, tt.expectedAreas)
			}
			if delta != tt.expectedDelta {
				t.Errorf("memory delta mismatch: got %d, want %d", delta, tt.expectedDelta)
			}
		})
	}
}
