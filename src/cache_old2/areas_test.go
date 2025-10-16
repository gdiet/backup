package cache_old2

import (
	"reflect"
	"testing"
)

func TestRemoveOverlappingAreas(t *testing.T) {
	// Test case 1: Empty areas - should return empty slice
	t.Run("EmptyAreas", func(t *testing.T) {
		areas := Areas{}
		area := Area{Off: 10, Len: 5}
		result := areas.RemoveOverlappingAreas(area)
		if len(result) != 0 {
			t.Errorf("Expected empty slice, got %v", result)
		}
	})

	// Test case 2: No overlaps - all areas should remain
	t.Run("NoOverlaps", func(t *testing.T) {
		areas := Areas{
			{Off: 0, Len: 5},  // [0-5)
			{Off: 10, Len: 5}, // [10-15)
			{Off: 20, Len: 5}, // [20-25)
		}
		area := Area{Off: 6, Len: 3} // [6-9) - no overlap with any area
		result := areas.RemoveOverlappingAreas(area)
		expected := areas
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 3: Complete overlap - area completely contains one existing area
	t.Run("CompleteOverlap", func(t *testing.T) {
		areas := Areas{
			{Off: 0, Len: 5},  // [0-5)
			{Off: 10, Len: 5}, // [10-15) - will be completely removed
			{Off: 20, Len: 5}, // [20-25)
		}
		area := Area{Off: 8, Len: 10} // [8-18) - completely contains [10-15)
		result := areas.RemoveOverlappingAreas(area)
		expected := Areas{
			{Off: 0, Len: 5},
			{Off: 20, Len: 5},
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 4: Partial overlap at start - should split area
	t.Run("PartialOverlapStart", func(t *testing.T) {
		areas := Areas{
			{Off: 0, Len: 5},   // [0-5)
			{Off: 10, Len: 10}, // [10-20) - overlaps with [10-12)
			{Off: 25, Len: 5},  // [25-30)
		}
		area := Area{Off: 10, Len: 2} // [10-12) - partial overlap at start
		result := areas.RemoveOverlappingAreas(area)
		expected := Areas{
			{Off: 0, Len: 5},  // [0-5) - unchanged
			{Off: 12, Len: 8}, // [12-20) - remaining part after removing [10-12)
			{Off: 25, Len: 5}, // [25-30) - unchanged
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 5: Partial overlap at end - should split area
	t.Run("PartialOverlapEnd", func(t *testing.T) {
		areas := Areas{
			{Off: 0, Len: 5},   // [0-5)
			{Off: 10, Len: 10}, // [10-20) - overlaps with [18-22)
			{Off: 25, Len: 5},  // [25-30)
		}
		area := Area{Off: 18, Len: 4} // [18-22) - partial overlap at end
		result := areas.RemoveOverlappingAreas(area)
		expected := Areas{
			{Off: 0, Len: 5},  // [0-5) - unchanged
			{Off: 10, Len: 8}, // [10-18) - remaining part after removing [18-20)
			{Off: 25, Len: 5}, // [25-30) - unchanged
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 6: Overlap in middle - should split area into two parts
	t.Run("PartialOverlapMiddle", func(t *testing.T) {
		areas := Areas{
			{Off: 0, Len: 20}, // [0-20) - should be split into [0-5) and [15-20)
		}
		area := Area{Off: 5, Len: 10} // [5-15) - overlaps middle of [0-20)
		result := areas.RemoveOverlappingAreas(area)
		expected := Areas{
			{Off: 0, Len: 5},  // [0-5) - part before overlap
			{Off: 15, Len: 5}, // [15-20) - part after overlap
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 7: Multiple overlaps - remove from multiple areas
	t.Run("MultipleOverlaps", func(t *testing.T) {
		areas := Areas{
			{Off: 0, Len: 5},  // [0-5)
			{Off: 10, Len: 5}, // [10-15) - overlaps completely
			{Off: 16, Len: 5}, // [16-21) - partial overlap [16-18)
			{Off: 25, Len: 5}, // [25-30)
		}
		area := Area{Off: 8, Len: 10} // [8-18) - overlaps with [10-15) and [16-18)
		result := areas.RemoveOverlappingAreas(area)
		expected := Areas{
			{Off: 0, Len: 5},
			{Off: 18, Len: 3}, // [18-21) - remaining part of [16-21) after removing [16-18)
			{Off: 25, Len: 5},
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 8: Exact match - area exactly matches existing area
	t.Run("ExactMatch", func(t *testing.T) {
		areas := Areas{
			{Off: 0, Len: 5},
			{Off: 10, Len: 5}, // [10-15) - exact match with removal area
			{Off: 20, Len: 5},
		}
		area := Area{Off: 10, Len: 5} // [10-15) - exact match
		result := areas.RemoveOverlappingAreas(area)
		expected := Areas{
			{Off: 0, Len: 5},
			{Off: 20, Len: 5},
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 9: Large area overlaps all
	t.Run("OverlapAll", func(t *testing.T) {
		areas := Areas{
			{Off: 5, Len: 5},  // [5-10)
			{Off: 15, Len: 5}, // [15-20)
			{Off: 25, Len: 5}, // [25-30)
		}
		area := Area{Off: 0, Len: 35} // [0-35) - overlaps with all areas
		result := areas.RemoveOverlappingAreas(area)
		if len(result) != 0 { // All should be removed
			t.Errorf("Expected empty slice, got %v", result)
		}
	})

	// Test case 10: Unsorted input areas - function should still work correctly
	t.Run("UnsortedInput", func(t *testing.T) {
		areas := Areas{
			{Off: 20, Len: 5}, // [20-25) - not in sorted order
			{Off: 0, Len: 5},  // [0-5)
			{Off: 10, Len: 5}, // [10-15)
		}
		area := Area{Off: 8, Len: 7} // [8-15) - overlaps with [10-15)
		result := areas.RemoveOverlappingAreas(area)
		expected := Areas{
			{Off: 20, Len: 5}, // [20-25) - unchanged, same order as input
			{Off: 0, Len: 5},  // [0-5) - unchanged, same order as input
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 11: Overlapping input areas - function should handle correctly
	t.Run("OverlappingInput", func(t *testing.T) {
		areas := Areas{
			{Off: 0, Len: 5},  // [0-5)
			{Off: 3, Len: 4},  // [3-7) - overlaps with previous area
			{Off: 10, Len: 5}, // [10-15)
		}
		area := Area{Off: 2, Len: 3} // [2-5) - overlaps with both [0-5) and [3-7)
		result := areas.RemoveOverlappingAreas(area)
		expected := Areas{
			{Off: 0, Len: 2},  // [0-2) - remaining part of [0-5)
			{Off: 5, Len: 2},  // [5-7) - remaining part of [3-7)
			{Off: 10, Len: 5}, // [10-15) - unchanged
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 12: Adjacent but not merged areas - function should work correctly
	t.Run("AdjacentAreas", func(t *testing.T) {
		areas := Areas{
			{Off: 0, Len: 5},  // [0-5)
			{Off: 5, Len: 5},  // [5-10) - adjacent to previous, but not merged
			{Off: 15, Len: 5}, // [15-20)
		}
		area := Area{Off: 3, Len: 4} // [3-7) - overlaps with both [0-5) and [5-10)
		result := areas.RemoveOverlappingAreas(area)
		expected := Areas{
			{Off: 0, Len: 3},  // [0-3) - remaining part of [0-5)
			{Off: 7, Len: 3},  // [7-10) - remaining part of [5-10)
			{Off: 15, Len: 5}, // [15-20) - unchanged
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 13: Zero-length areas - function should handle correctly
	t.Run("ZeroLengthAreas", func(t *testing.T) {
		areas := Areas{
			{Off: 0, Len: 5},  // [0-5)
			{Off: 10, Len: 0}, // [10-10) - zero-length area (point)
			{Off: 15, Len: 5}, // [15-20)
		}
		area := Area{Off: 8, Len: 4} // [8-12) - contains point 10
		result := areas.RemoveOverlappingAreas(area)
		expected := Areas{
			{Off: 0, Len: 5},  // [0-5) - unchanged
			{Off: 15, Len: 5}, // [15-20) - unchanged
			// Zero-length area at 10 is removed because point 10 is inside [8-12)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test case 14: Zero-length removal area - should split areas at the point
	t.Run("ZeroLengthRemovalArea", func(t *testing.T) {
		areas := Areas{
			{Off: 0, Len: 5},   // [0-5)
			{Off: 10, Len: 10}, // [10-20) - point 15 is inside, so gets split
		}
		area := Area{Off: 15, Len: 0} // [15-15) - zero-length area (point 15)
		result := areas.RemoveOverlappingAreas(area)
		expected := Areas{
			{Off: 0, Len: 5},  // [0-5) - unchanged
			{Off: 10, Len: 5}, // [10-15) - part before point 15
			{Off: 15, Len: 5}, // [15-20) - part after point 15
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
}
