package cache

import (
	"testing"
)

func TestSparseRemoveWithinSparseArea(t *testing.T) {
	// Setup
	s := &sparse{
		areas: areas{
			{off: 0, len: 10},
			{off: 20, len: 10},
		},
	}

	// Act
	s.remove(2, 5)

	// Assert
	if len(s.areas) != 3 {
		t.Errorf("Expected 3 sparse areas, got %d", len(s.areas))
	}
	if s.areas[0] != (area{off: 0, len: 2}) {
		t.Errorf("Expected first area to be {off: 0, len: 2}, got %+v", s.areas[0])
	}
	if s.areas[1] != (area{off: 7, len: 3}) {
		t.Errorf("Expected second area to be {off: 7, len: 3}, got %+v", s.areas[1])
	}
}

func TestSparseRemoveOverlappingSparseAreas(t *testing.T) {
	// Setup
	s := &sparse{
		areas: areas{
			{off: 0, len: 10},
			{off: 20, len: 10},
		},
	}

	// Act
	s.remove(5, 20)

	// Assert
	if len(s.areas) != 2 {
		t.Errorf("Expected 2 sparse areas, got %d", len(s.areas))
	}
	if s.areas[0] != (area{off: 0, len: 5}) {
		t.Errorf("Expected first area to be {off: 0, len: 5}, got %+v", s.areas[0])
	}
	if s.areas[1] != (area{off: 25, len: 5}) {
		t.Errorf("Expected second area to be {off: 25, len: 5}, got %+v", s.areas[1])
	}
}

func TestSparseRemoveOutsideSparseAreas(t *testing.T) {
	// Setup
	s := &sparse{
		areas: areas{
			{off: 0, len: 10},
			{off: 20, len: 10},
		},
	}

	// Act
	s.remove(30, 10)

	// Assert
	if len(s.areas) != 2 {
		t.Errorf("Expected 2 sparse areas, got %d", len(s.areas))
	}
	if s.areas[0] != (area{off: 0, len: 10}) {
		t.Errorf("Expected first area to be {off: 0, len: 10}, got %+v", s.areas[0])
	}
	if s.areas[1] != (area{off: 20, len: 10}) {
		t.Errorf("Expected second area to be {off: 20, len: 10}, got %+v", s.areas[1])
	}
}
