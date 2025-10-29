package aaa

import (
	"testing"
)

func TestSparseTruncate(t *testing.T) {
	t.Run("Truncate within existing sparse areas", func(t *testing.T) {
		// Setup
		s := &sparse{
			sparseAreas: areas{
				{off: 0, len: 10},
				{off: 20, len: 10},
			},
		}

		// Act
		s.truncate(15)

		// Assert
		if len(s.sparseAreas) != 1 {
			t.Errorf("Expected 1 sparse area, got %d", len(s.sparseAreas))
		}
		if s.sparseAreas[0].len != 10 {
			t.Errorf("Expected first sparse area length to be 10, got %d", s.sparseAreas[0].len)
		}
	})

	t.Run("Truncate beyond all sparse areas", func(t *testing.T) {
		// Setup
		s := &sparse{
			sparseAreas: areas{
				{off: 0, len: 10},
				{off: 20, len: 10},
			},
		}

		// Act
		s.truncate(5)

		// Assert
		if len(s.sparseAreas) != 1 {
			t.Errorf("Expected 1 sparse area, got %d", len(s.sparseAreas))
		}
		if s.sparseAreas[0].len != 5 {
			t.Errorf("Expected first sparse area length to be 5, got %d", s.sparseAreas[0].len)
		}
	})

	t.Run("Truncate removes all sparse areas", func(t *testing.T) {
		// Setup
		s := &sparse{
			sparseAreas: areas{
				{off: 0, len: 10},
				{off: 20, len: 10},
			},
		}

		// Act
		s.truncate(0)

		// Assert
		if len(s.sparseAreas) != 0 {
			t.Errorf("Expected 0 sparse areas, got %d", len(s.sparseAreas))
		}
	})
}
