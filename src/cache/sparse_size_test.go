package cache

import (
	"testing"
)

func TestSparse_Size(t *testing.T) {
	// Test case 1: Initial size
	t.Run("InitialSize", func(t *testing.T) {
		sparse := &Sparse{size: 100}
		if sparse.Size() != 100 {
			t.Errorf("Expected size 100, got %d", sparse.Size())
		}
	})

	// Test case 2: Zero size
	t.Run("ZeroSize", func(t *testing.T) {
		sparse := &Sparse{size: 0}
		if sparse.Size() != 0 {
			t.Errorf("Expected size 0, got %d", sparse.Size())
		}
	})
}
