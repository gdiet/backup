package cache

import (
	"reflect"
	"testing"
)

func TestSparse_Read(t *testing.T) {
	// Test case 1: Read from empty sparse (no sparse areas)
	t.Run("NoSparseAreas", func(t *testing.T) {
		sparse := &Sparse{
			size:        20,
			sparseAreas: DataAreas{},
		}
		data := make(Bytes, 10)
		for i := range data {
			data[i] = 0xFF // Fill with non-zero values
		}

		nonSparseAreas, totalRead := sparse.Read(5, data)

		// Should return the full requested area as non-sparse
		expected := DataAreas{{Off: 5, Len: 10}}
		if !reflect.DeepEqual(nonSparseAreas, expected) {
			t.Errorf("Expected %v, got %v", expected, nonSparseAreas)
		}
		if totalRead != 10 {
			t.Errorf("Expected totalRead 10, got %d", totalRead)
		}
		// Data should remain unchanged (no sparse areas to zero out)
		for i, b := range data {
			if b != 0xFF {
				t.Errorf("Data[%d] should be 0xFF, got %02x", i, b)
			}
		}
	})

	// Test case 2: Read with EOF (beyond file size)
	t.Run("ReadBeyondEOF", func(t *testing.T) {
		sparse := &Sparse{
			size:        10,
			sparseAreas: DataAreas{},
		}
		data := make(Bytes, 10)

		nonSparseAreas, totalRead := sparse.Read(8, data)

		// Should only read 2 bytes (from position 8 to size 10)
		expected := DataAreas{{Off: 8, Len: 2}}
		if !reflect.DeepEqual(nonSparseAreas, expected) {
			t.Errorf("Expected %v, got %v", expected, nonSparseAreas)
		}
		if totalRead != 2 {
			t.Errorf("Expected totalRead 2, got %d", totalRead)
		}
	})

	// Test case 3: Read completely beyond EOF
	t.Run("ReadCompletelyBeyondEOF", func(t *testing.T) {
		sparse := &Sparse{
			size:        10,
			sparseAreas: DataAreas{},
		}
		data := make(Bytes, 5)

		nonSparseAreas, totalRead := sparse.Read(15, data)

		// Should read nothing
		if len(nonSparseAreas) != 0 {
			t.Errorf("Expected no non-sparse areas, got %v", nonSparseAreas)
		}
		if totalRead != 0 {
			t.Errorf("Expected totalRead 0, got %d", totalRead)
		}
	})

	// Test case 4: Read with sparse area overlap
	t.Run("SparseAreaOverlap", func(t *testing.T) {
		sparse := &Sparse{
			size: 20,
			sparseAreas: DataAreas{
				{Off: 5, Len: 5}, // [5-10) is sparse
			},
		}
		data := make(Bytes, 10)
		for i := range data {
			data[i] = 0xFF // Fill with non-zero values
		}

		nonSparseAreas, totalRead := sparse.Read(3, data)

		// Should return two non-sparse areas: [3-5) and [10-13)
		expected := DataAreas{
			{Off: 3, Len: 2},  // [3-5)
			{Off: 10, Len: 3}, // [10-13)
		}
		if !reflect.DeepEqual(nonSparseAreas, expected) {
			t.Errorf("Expected %v, got %v", expected, nonSparseAreas)
		}
		if totalRead != 10 {
			t.Errorf("Expected totalRead 10, got %d", totalRead)
		}
		// Check that sparse area (positions 2-7 in data, which is file positions 5-10) is zeroed
		for i := 2; i < 7; i++ {
			if data[i] != 0 {
				t.Errorf("Data[%d] should be 0 (sparse), got %02x", i, data[i])
			}
		}
		// Check that non-sparse areas remain unchanged
		for i := 0; i < 2; i++ {
			if data[i] != 0xFF {
				t.Errorf("Data[%d] should be 0xFF (non-sparse), got %02x", i, data[i])
			}
		}
		for i := 7; i < 10; i++ {
			if data[i] != 0xFF {
				t.Errorf("Data[%d] should be 0xFF (non-sparse), got %02x", i, data[i])
			}
		}
	})

	// Test case 5: Read with multiple sparse areas
	t.Run("MultipleSparseAreas", func(t *testing.T) {
		sparse := &Sparse{
			size: 20,
			sparseAreas: DataAreas{
				{Off: 2, Len: 3}, // [2-5) is sparse
				{Off: 8, Len: 2}, // [8-10) is sparse
			},
		}
		data := make(Bytes, 10)
		for i := range data {
			data[i] = 0xFF // Fill with non-zero values
		}

		nonSparseAreas, totalRead := sparse.Read(0, data)

		// Should return three non-sparse areas: [0-2), [5-8), [10-20)
		expected := DataAreas{
			{Off: 0, Len: 2}, // [0-2)
			{Off: 5, Len: 3}, // [5-8)
		}
		if !reflect.DeepEqual(nonSparseAreas, expected) {
			t.Errorf("Expected %v, got %v", expected, nonSparseAreas)
		}
		if totalRead != 10 {
			t.Errorf("Expected totalRead 10, got %d", totalRead)
		}
		// Check sparse areas are zeroed
		for i := 2; i < 5; i++ { // positions 2-4 (file 2-5)
			if data[i] != 0 {
				t.Errorf("Data[%d] should be 0 (sparse), got %02x", i, data[i])
			}
		}
		for i := 8; i < 10; i++ { // positions 8-9 (file 8-10)
			if data[i] != 0 {
				t.Errorf("Data[%d] should be 0 (sparse), got %02x", i, data[i])
			}
		}
	})

	// Test case 6: Read with no overlap with sparse areas
	t.Run("NoSparseOverlap", func(t *testing.T) {
		sparse := &Sparse{
			size: 20,
			sparseAreas: DataAreas{
				{Off: 10, Len: 5}, // [10-15) is sparse
			},
		}
		data := make(Bytes, 5)
		for i := range data {
			data[i] = 0xFF
		}

		nonSparseAreas, totalRead := sparse.Read(0, data)

		// Should return the full requested area as non-sparse
		expected := DataAreas{{Off: 0, Len: 5}}
		if !reflect.DeepEqual(nonSparseAreas, expected) {
			t.Errorf("Expected %v, got %v", expected, nonSparseAreas)
		}
		if totalRead != 5 {
			t.Errorf("Expected totalRead 5, got %d", totalRead)
		}
		// Data should remain unchanged
		for i, b := range data {
			if b != 0xFF {
				t.Errorf("Data[%d] should be 0xFF, got %02x", i, b)
			}
		}
	})
}
