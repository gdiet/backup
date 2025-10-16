package cache_old2

import (
	"reflect"
	"testing"
)

// TestMemoryWriteWithInvariantViolations tests the behavior of Write when
// the input DataAreas violate the recommended invariants:
// - Sorted by Offset
// - Non-overlapping
// - Mostly merged (adjacent areas combined)
//
// These tests document the actual behavior when invariants are violated,
// which may be different from expected behavior and should be considered
// undefined/unsupported usage.
func TestMemoryWriteWithInvariantViolations(t *testing.T) {
	tests := []struct {
		name          string
		initialAreas  DataAreas // Areas that violate invariants
		position      int64
		data          Bytes
		maxMergeSize  int64
		expectedAreas DataAreas
		description   string // Documents the behavior
	}{
		{
			name: "UnsortedAreas",
			initialAreas: DataAreas{
				{Off: 10, Data: Bytes("world")}, // Should come after "hello"
				{Off: 0, Data: Bytes("hello")},  // Violates "sorted by offset"
			},
			position:     5,
			data:         Bytes("X"),
			maxMergeSize: 100,
			expectedAreas: DataAreas{
				{Off: 5, Data: Bytes("X")}, // New area placed first
				{Off: 10, Data: Bytes("world")},
				{Off: 0, Data: Bytes("hello")},
			},
			description: "With unsorted input areas, the new area is inserted where the algorithm first finds a suitable position, not necessarily maintaining order",
		},
		{
			name: "OverlappingAreas",
			initialAreas: DataAreas{
				{Off: 0, Data: Bytes("hello")},
				{Off: 3, Data: Bytes("world")}, // Overlaps with "hello" at positions 3-4
			},
			position:     8,
			data:         Bytes("!"),
			maxMergeSize: 100,
			expectedAreas: DataAreas{
				{Off: 0, Data: Bytes("hello")},
				{Off: 3, Data: Bytes("world!")}, // Merged with new data
			},
			description: "With overlapping input areas, the algorithm merges with the first overlapping area it encounters",
		},
		{
			name: "AdjacentNotMerged",
			initialAreas: DataAreas{
				{Off: 0, Data: Bytes("hello")},
				{Off: 5, Data: Bytes("world")}, // Adjacent but not merged (violates "mostly merged")
			},
			position:     10,
			data:         Bytes("!"),
			maxMergeSize: 100,
			expectedAreas: DataAreas{
				{Off: 0, Data: Bytes("hello")},
				{Off: 5, Data: Bytes("world!")}, // Merges with second area
			},
			description: "With unmerged adjacent areas, new data merges with the area it overlaps/touches first in iteration order",
		},
		{
			name: "MultipleViolations",
			initialAreas: DataAreas{
				{Off: 10, Data: Bytes("c")},   // Unsorted
				{Off: 2, Data: Bytes("bbbb")}, // Unsorted, overlaps with next
				{Off: 0, Data: Bytes("aaaa")}, // Overlaps with previous
			},
			position:     1,
			data:         Bytes("X"),
			maxMergeSize: 100,
			expectedAreas: DataAreas{
				{Off: 1, Data: Bytes("X")}, // New area inserted separately
				{Off: 10, Data: Bytes("c")},
				{Off: 2, Data: Bytes("bbbb")},
				{Off: 0, Data: Bytes("aaaa")},
			},
			description: "With multiple invariant violations, the algorithm may insert the new area at an unexpected position without merging",
		},
		{
			name: "WriteIntoOverlappingRegion",
			initialAreas: DataAreas{
				{Off: 0, Data: Bytes("hello")},
				{Off: 2, Data: Bytes("world")}, // Overlaps at positions 2-4
			},
			position:     3,
			data:         Bytes("XX"),
			maxMergeSize: 100,
			expectedAreas: DataAreas{
				{Off: 0, Data: Bytes("helXXld")}, // Write merges both overlapping areas
			},
			description: "When writing into an overlapping region with invariant violations, areas may be merged in unexpected ways",
		},
		{
			name: "ZeroLengthAreas",
			initialAreas: DataAreas{
				{Off: 5, Data: Bytes("")},      // Zero-length area (edge case)
				{Off: 0, Data: Bytes("hello")}, // Normal area
			},
			position:     3,
			data:         Bytes("XX"),
			maxMergeSize: 100,
			expectedAreas: DataAreas{
				{Off: 0, Data: Bytes("helXX")}, // Zero-length area removed, overlapping write truncated
			},
			description: "Zero-length areas may be removed during processing; write merges with overlapping non-empty areas",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Test description: %s", tt.description)

			memory := &Memory{areas: tt.initialAreas}
			memory.Write(tt.position, tt.data, tt.maxMergeSize)

			if !reflect.DeepEqual(memory.areas, tt.expectedAreas) {
				t.Errorf("Write() with invariant violations:\nGot:  %v\nWant: %v", memory.areas, tt.expectedAreas)
			}
		})
	}
}

// TestMemoryWriteInvariantViolationEdgeCases tests additional edge cases
// when invariants are violated that might cause unexpected behavior.
func TestMemoryWriteInvariantViolationEdgeCases(t *testing.T) {
	t.Run("DuplicateOffsets", func(t *testing.T) {
		// Multiple areas at the same offset (severe invariant violation)
		memory := &Memory{areas: DataAreas{
			{Off: 0, Data: Bytes("first")},
			{Off: 0, Data: Bytes("second")}, // Same offset
		}}

		memory.Write(0, Bytes("X"), 100)

		// The behavior with duplicate offsets is undefined but should not crash
		t.Logf("Result with duplicate offsets: %v", memory.areas)
		// We don't assert specific results here since the behavior is undefined
	})

	t.Run("ExtremelyOutOfOrder", func(t *testing.T) {
		// Completely reverse-sorted areas
		memory := &Memory{areas: DataAreas{
			{Off: 100, Data: Bytes("z")},
			{Off: 50, Data: Bytes("y")},
			{Off: 0, Data: Bytes("x")},
		}}

		memory.Write(25, Bytes("NEW"), 100)

		// New area is inserted at beginning due to algorithm behavior
		expected := DataAreas{
			{Off: 25, Data: Bytes("NEW")},
			{Off: 100, Data: Bytes("z")},
			{Off: 50, Data: Bytes("y")},
			{Off: 0, Data: Bytes("x")},
		}

		if !reflect.DeepEqual(memory.areas, expected) {
			t.Errorf("Write() with reverse-sorted areas: got %v, want %v", memory.areas, expected)
		}

		t.Log("With completely unsorted input, new areas are inserted where the algorithm finds the first suitable position")
	})
}
