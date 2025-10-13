package cache

import (
	"reflect"
	"testing"
)

func TestSparse_Truncate(t *testing.T) {
	// Test case 1: No size change
	t.Run("NoSizeChange", func(t *testing.T) {
		sparse := &Sparse{
			size: 10,
			sparseAreas: DataAreas{
				{Off: 2, Len: 3},
			},
		}

		needsTruncation := sparse.Truncate(10)

		if needsTruncation {
			t.Error("Expected no truncation needed")
		}
		if sparse.Size() != 10 {
			t.Errorf("Expected size 10, got %d", sparse.Size())
		}
	})

	// Test case 2: File grows
	t.Run("FileGrows", func(t *testing.T) {
		sparse := &Sparse{
			size: 10,
			sparseAreas: DataAreas{
				{Off: 2, Len: 3},
			},
		}

		needsTruncation := sparse.Truncate(15)

		if needsTruncation {
			t.Error("Expected no truncation needed for growth")
		}
		if sparse.Size() != 15 {
			t.Errorf("Expected size 15, got %d", sparse.Size())
		}
		// Should add new sparse area [10-15)
		expectedAreas := DataAreas{
			{Off: 2, Len: 3},  // Original area
			{Off: 10, Len: 5}, // New sparse area
		}
		if !reflect.DeepEqual(sparse.sparseAreas, expectedAreas) {
			t.Errorf("Expected %v, got %v", expectedAreas, sparse.sparseAreas)
		}
	})

	// Test case 3: File shrinks - remove areas beyond new size
	t.Run("FileShrinks_RemoveAreas", func(t *testing.T) {
		sparse := &Sparse{
			size: 20,
			sparseAreas: DataAreas{
				{Off: 2, Len: 3},  // [2-5) - should remain
				{Off: 15, Len: 3}, // [15-18) - should be removed
				{Off: 25, Len: 5}, // [25-30) - should be removed
			},
		}

		needsTruncation := sparse.Truncate(10)

		if !needsTruncation {
			t.Error("Expected truncation needed")
		}
		if sparse.Size() != 10 {
			t.Errorf("Expected size 10, got %d", sparse.Size())
		}
		// Should only keep the area that's completely within new size
		expectedAreas := DataAreas{
			{Off: 2, Len: 3}, // Only this area remains
		}
		if !reflect.DeepEqual(sparse.sparseAreas, expectedAreas) {
			t.Errorf("Expected %v, got %v", expectedAreas, sparse.sparseAreas)
		}
	})

	// Test case 4: File shrinks - truncate areas that extend beyond new size
	t.Run("FileShrinks_TruncateAreas", func(t *testing.T) {
		sparse := &Sparse{
			size: 20,
			sparseAreas: DataAreas{
				{Off: 2, Len: 3}, // [2-5) - should remain unchanged
				{Off: 7, Len: 8}, // [7-15) - should be truncated to [7-12)
			},
		}

		needsTruncation := sparse.Truncate(12)

		if !needsTruncation {
			t.Error("Expected truncation needed")
		}
		if sparse.Size() != 12 {
			t.Errorf("Expected size 12, got %d", sparse.Size())
		}
		// Should truncate the second area
		expectedAreas := DataAreas{
			{Off: 2, Len: 3}, // [2-5) unchanged
			{Off: 7, Len: 5}, // [7-12) truncated from [7-15)
		}
		if !reflect.DeepEqual(sparse.sparseAreas, expectedAreas) {
			t.Errorf("Expected %v, got %v", expectedAreas, sparse.sparseAreas)
		}
	})

	// Test case 5: File shrinks to zero
	t.Run("FileShrinks_ToZero", func(t *testing.T) {
		sparse := &Sparse{
			size: 10,
			sparseAreas: DataAreas{
				{Off: 2, Len: 3},
				{Off: 7, Len: 2},
			},
		}

		needsTruncation := sparse.Truncate(0)

		if !needsTruncation {
			t.Error("Expected truncation needed")
		}
		if sparse.Size() != 0 {
			t.Errorf("Expected size 0, got %d", sparse.Size())
		}
		// All areas should be removed
		if len(sparse.sparseAreas) != 0 {
			t.Errorf("Expected no sparse areas, got %v", sparse.sparseAreas)
		}
	})
}
