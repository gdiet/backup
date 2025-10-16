package cache_old2

import (
	"reflect"
	"testing"
)

func TestSparse_Write(t *testing.T) {
	// Test case 1: Write to empty sparse (no sparse areas)
	t.Run("WriteToEmpty", func(t *testing.T) {
		sparse := &Sparse{
			size:        0,
			sparseAreas: Areas{},
		}
		data := Bytes("hello")

		sparse.Write(0, data)

		// Should update size
		if sparse.Size() != 5 {
			t.Errorf("Expected size 5, got %d", sparse.Size())
		}
		// Should have no sparse areas (none were there)
		if len(sparse.sparseAreas) != 0 {
			t.Errorf("Expected no sparse areas, got %v", sparse.sparseAreas)
		}
	})

	// Test case 2: Write zero-length data - should be no-op
	t.Run("WriteZeroLength", func(t *testing.T) {
		sparse := &Sparse{
			size: 10,
			sparseAreas: Areas{
				{Off: 5, Len: 3},
			},
		}
		originalAreas := append(Areas{}, sparse.sparseAreas...) // Copy

		sparse.Write(2, Bytes{}) // Empty write

		// Nothing should change
		if sparse.Size() != 10 {
			t.Errorf("Expected size unchanged at 10, got %d", sparse.Size())
		}
		if !reflect.DeepEqual(sparse.sparseAreas, originalAreas) {
			t.Errorf("Expected sparse areas unchanged, got %v", sparse.sparseAreas)
		}
	})

	// Test case 3: Write extends file size
	t.Run("WriteExtendsSize", func(t *testing.T) {
		sparse := &Sparse{
			size:        10,
			sparseAreas: Areas{{Off: 2, Len: 3}}, // [2-5) sparse
		}
		data := Bytes("extended")

		sparse.Write(15, data) // Write beyond current size

		// Should update size to end of write
		expectedSize := int64(15 + len(data))
		if sparse.Size() != expectedSize {
			t.Errorf("Expected size %d, got %d", expectedSize, sparse.Size())
		}
		// Sparse areas should remain unchanged (no overlap)
		expected := Areas{{Off: 2, Len: 3}}
		if !reflect.DeepEqual(sparse.sparseAreas, expected) {
			t.Errorf("Expected sparse areas %v, got %v", expected, sparse.sparseAreas)
		}
	})

	// Test case 4: Write completely overlaps sparse area - should remove it
	t.Run("WriteCompleteOverlap", func(t *testing.T) {
		sparse := &Sparse{
			size: 20,
			sparseAreas: Areas{
				{Off: 0, Len: 5},  // [0-5) sparse
				{Off: 10, Len: 5}, // [10-15) sparse - will be completely removed
				{Off: 18, Len: 2}, // [18-20) sparse
			},
		}
		data := Bytes("replace")

		sparse.Write(8, data) // [8-15) - completely covers [10-15)

		// Should remove the overlapping sparse area [10-15)
		expected := Areas{
			{Off: 0, Len: 5},  // [0-5) unchanged
			{Off: 18, Len: 2}, // [18-20) unchanged
		}
		if !reflect.DeepEqual(sparse.sparseAreas, expected) {
			t.Errorf("Expected sparse areas %v, got %v", expected, sparse.sparseAreas)
		}
	})

	// Test case 5: Write partially overlaps sparse area - should split it
	t.Run("WritePartialOverlap", func(t *testing.T) {
		sparse := &Sparse{
			size: 20,
			sparseAreas: Areas{
				{Off: 5, Len: 10}, // [5-15) sparse
			},
		}
		data := Bytes("data")

		sparse.Write(8, data) // [8-12) - overlaps middle of [5-15)

		// Should split sparse area into [5-8) and [12-15)
		expected := Areas{
			{Off: 5, Len: 3},  // [5-8) - part before write
			{Off: 12, Len: 3}, // [12-15) - part after write
		}
		if !reflect.DeepEqual(sparse.sparseAreas, expected) {
			t.Errorf("Expected sparse areas %v, got %v", expected, sparse.sparseAreas)
		}
	})

	// Test case 6: Write overlaps multiple sparse areas
	t.Run("WriteMultipleOverlaps", func(t *testing.T) {
		sparse := &Sparse{
			size: 30,
			sparseAreas: Areas{
				{Off: 0, Len: 5},  // [0-5) sparse - no overlap
				{Off: 8, Len: 4},  // [8-12) sparse - completely overlapped
				{Off: 15, Len: 8}, // [15-23) sparse - partially overlapped
				{Off: 25, Len: 3}, // [25-28) sparse - no overlap
			},
		}
		data := Bytes("longwritedata")

		sparse.Write(10, data) // [10-23) - overlaps [8-12) completely and [15-23) completely

		// Should remove/adjust overlapping areas
		expected := Areas{
			{Off: 0, Len: 5},  // [0-5) unchanged
			{Off: 8, Len: 2},  // [8-10) remaining part of [8-12) after [10-23) write
			{Off: 25, Len: 3}, // [25-28) unchanged
		}
		if !reflect.DeepEqual(sparse.sparseAreas, expected) {
			t.Errorf("Expected sparse areas %v, got %v", expected, sparse.sparseAreas)
		}
	})

	// Test case 7: Write at start of sparse area
	t.Run("WriteAtSparseStart", func(t *testing.T) {
		sparse := &Sparse{
			size: 20,
			sparseAreas: Areas{
				{Off: 10, Len: 8}, // [10-18) sparse
			},
		}
		data := Bytes("hi")

		sparse.Write(10, data) // [10-12) - starts at sparse area beginning

		// Should remove overlapping part, leave [12-18)
		expected := Areas{
			{Off: 12, Len: 6}, // [12-18) remaining part
		}
		if !reflect.DeepEqual(sparse.sparseAreas, expected) {
			t.Errorf("Expected sparse areas %v, got %v", expected, sparse.sparseAreas)
		}
	})

	// Test case 8: Write at end of sparse area
	t.Run("WriteAtSparseEnd", func(t *testing.T) {
		sparse := &Sparse{
			size: 20,
			sparseAreas: Areas{
				{Off: 5, Len: 10}, // [5-15) sparse
			},
		}
		data := Bytes("end")

		sparse.Write(12, data) // [12-15) - ends at sparse area end

		// Should remove overlapping part, leave [5-12)
		expected := Areas{
			{Off: 5, Len: 7}, // [5-12) remaining part
		}
		if !reflect.DeepEqual(sparse.sparseAreas, expected) {
			t.Errorf("Expected sparse areas %v, got %v", expected, sparse.sparseAreas)
		}
	})

	// Test case 9: Write exactly matches sparse area
	t.Run("WriteExactMatch", func(t *testing.T) {
		sparse := &Sparse{
			size: 20,
			sparseAreas: Areas{
				{Off: 3, Len: 2},  // [3-5) sparse
				{Off: 10, Len: 5}, // [10-15) sparse - exact match
				{Off: 18, Len: 1}, // [18-19) sparse
			},
		}
		data := Bytes("exact")

		sparse.Write(10, data) // [10-15) - exactly matches sparse area

		// Should remove the exactly matching sparse area
		expected := Areas{
			{Off: 3, Len: 2},  // [3-5) unchanged
			{Off: 18, Len: 1}, // [18-19) unchanged
		}
		if !reflect.DeepEqual(sparse.sparseAreas, expected) {
			t.Errorf("Expected sparse areas %v, got %v", expected, sparse.sparseAreas)
		}
	})

	// Test case 10: Write with no overlap to sparse areas
	t.Run("WriteNoOverlap", func(t *testing.T) {
		sparse := &Sparse{
			size: 20,
			sparseAreas: Areas{
				{Off: 5, Len: 3},  // [5-8) sparse
				{Off: 15, Len: 3}, // [15-18) sparse
			},
		}
		data := Bytes("gap")

		sparse.Write(10, data) // [10-13) - no overlap with sparse areas

		// Should leave sparse areas unchanged
		expected := Areas{
			{Off: 5, Len: 3},  // [5-8) unchanged
			{Off: 15, Len: 3}, // [15-18) unchanged
		}
		if !reflect.DeepEqual(sparse.sparseAreas, expected) {
			t.Errorf("Expected sparse areas %v, got %v", expected, sparse.sparseAreas)
		}
	})

	// Test case 11: Write beyond current size with existing sparse areas
	t.Run("WriteExtendWithSparse", func(t *testing.T) {
		sparse := &Sparse{
			size: 10,
			sparseAreas: Areas{
				{Off: 2, Len: 4}, // [2-6) sparse
			},
		}
		data := Bytes("extend")

		sparse.Write(15, data) // [15-21) - beyond current size, no overlap

		// Should extend size and keep sparse areas
		if sparse.Size() != 21 {
			t.Errorf("Expected size 21, got %d", sparse.Size())
		}
		expected := Areas{
			{Off: 2, Len: 4}, // [2-6) unchanged
		}
		if !reflect.DeepEqual(sparse.sparseAreas, expected) {
			t.Errorf("Expected sparse areas %v, got %v", expected, sparse.sparseAreas)
		}
	})
}
