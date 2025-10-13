package cache

import (
	"testing"
)

const (
	expectedMergeSuccessMsg = "Expected merge to succeed"
	expectedCanMergeFmt     = "Expected canMerge=%v, got %v"
	negativeOffsetMsg       = "Should handle negative offsets correctly"
)

func TestTryMergingDataAreasAdjacentAreas(t *testing.T) {
	tests := []struct {
		name         string
		current      DataArea
		existing     DataArea
		expectedArea DataArea
	}{
		{
			name:         "Adjacent areas - current first",
			current:      DataArea{Off: 0, Data: Bytes("abc")},
			existing:     DataArea{Off: 3, Data: Bytes("def")},
			expectedArea: DataArea{Off: 0, Data: Bytes("abcdef")},
		},
		{
			name:         "Adjacent areas - existing first",
			current:      DataArea{Off: 5, Data: Bytes("xyz")},
			existing:     DataArea{Off: 0, Data: Bytes("hello")},
			expectedArea: DataArea{Off: 0, Data: Bytes("helloxyz")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, canMerge := tryMergingDataAreas(tt.current, tt.existing, 10)

			if !canMerge {
				t.Error(expectedMergeSuccessMsg)
				return
			}

			assertDataAreaEqual(t, tt.expectedArea, merged)
		})
	}
}

func TestTryMergingDataAreasOverlappingAreas(t *testing.T) {
	tests := []struct {
		name         string
		current      DataArea
		existing     DataArea
		expectedArea DataArea
	}{
		{
			name:         "Overlapping areas - current first",
			current:      DataArea{Off: 0, Data: Bytes("abcde")},
			existing:     DataArea{Off: 3, Data: Bytes("XYZ")},
			expectedArea: DataArea{Off: 0, Data: Bytes("abcdeZ")}, // XY wird überschrieben, Z bleibt
		},
		{
			name:         "Overlapping areas - existing first",
			current:      DataArea{Off: 3, Data: Bytes("XYZ")},
			existing:     DataArea{Off: 0, Data: Bytes("abcde")},
			expectedArea: DataArea{Off: 0, Data: Bytes("abcXYZ")},
		},
		{
			name:         "Complete overlap - current overwrites existing",
			current:      DataArea{Off: 2, Data: Bytes("NEW")},
			existing:     DataArea{Off: 0, Data: Bytes("old data")},
			expectedArea: DataArea{Off: 0, Data: Bytes("olNEWata")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, canMerge := tryMergingDataAreas(tt.current, tt.existing, 15)

			if !canMerge {
				t.Error(expectedMergeSuccessMsg)
				return
			}

			assertDataAreaEqual(t, tt.expectedArea, merged)
		})
	}
}

func TestTryMergingDataAreasSizeLimits(t *testing.T) {
	tests := []struct {
		name         string
		current      DataArea
		existing     DataArea
		maxMergeSize int64
		expectMerge  bool
		expectedArea DataArea
	}{
		{
			name:         "Gap between areas",
			current:      DataArea{Off: 0, Data: Bytes("abc")},
			existing:     DataArea{Off: 5, Data: Bytes("def")},
			maxMergeSize: 10,
			expectMerge:  true,
			expectedArea: DataArea{Off: 0, Data: Bytes("abc\x00\x00def")},
		},
		{
			name:         "Too large to merge",
			current:      DataArea{Off: 0, Data: Bytes("abc")},
			existing:     DataArea{Off: 10, Data: Bytes("def")},
			maxMergeSize: 5,
			expectMerge:  false,
		},
		{
			name:         "Exact size limit - should merge",
			current:      DataArea{Off: 0, Data: Bytes("abc")},
			existing:     DataArea{Off: 3, Data: Bytes("def")},
			maxMergeSize: 6,
			expectMerge:  true,
			expectedArea: DataArea{Off: 0, Data: Bytes("abcdef")},
		},
		{
			name:         "One byte over limit - should not merge",
			current:      DataArea{Off: 0, Data: Bytes("abc")},
			existing:     DataArea{Off: 4, Data: Bytes("def")},
			maxMergeSize: 6,
			expectMerge:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, canMerge := tryMergingDataAreas(tt.current, tt.existing, tt.maxMergeSize)

			if canMerge != tt.expectMerge {
				t.Errorf(expectedCanMergeFmt, tt.expectMerge, canMerge)
				return
			}

			if tt.expectMerge {
				assertDataAreaEqual(t, tt.expectedArea, merged)
			}
		})
	}
}

func TestTryMergingDataAreasZeroLengthAreas(t *testing.T) {
	tests := []struct {
		name         string
		current      DataArea
		existing     DataArea
		expectedArea DataArea
	}{
		{
			name:         "Zero-length current area",
			current:      DataArea{Off: 3, Data: Bytes("")},
			existing:     DataArea{Off: 0, Data: Bytes("hello")},
			expectedArea: DataArea{Off: 0, Data: Bytes("hello")},
		},
		{
			name:         "Zero-length existing area",
			current:      DataArea{Off: 0, Data: Bytes("hello")},
			existing:     DataArea{Off: 3, Data: Bytes("")},
			expectedArea: DataArea{Off: 0, Data: Bytes("hello")},
		},
		{
			name:         "Both zero-length areas",
			current:      DataArea{Off: 5, Data: Bytes("")},
			existing:     DataArea{Off: 3, Data: Bytes("")},
			expectedArea: DataArea{Off: 3, Data: Bytes("\x00\x00")}, // Gap zwischen 3 und 5 wird mit Nullen gefüllt
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, canMerge := tryMergingDataAreas(tt.current, tt.existing, 10)

			if !canMerge {
				t.Error(expectedMergeSuccessMsg)
				return
			}

			assertDataAreaEqual(t, tt.expectedArea, merged)
		})
	}
}

func TestTryMergingDataAreasEdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		current      DataArea
		existing     DataArea
		maxMergeSize int64
		expectMerge  bool
		description  string
	}{
		{
			name:         "Negative offset in current",
			current:      DataArea{Off: -5, Data: Bytes("abc")},
			existing:     DataArea{Off: 0, Data: Bytes("def")},
			maxMergeSize: 10,
			expectMerge:  true,
			description:  negativeOffsetMsg,
		},
		{
			name:         "Negative offset in existing",
			current:      DataArea{Off: 0, Data: Bytes("abc")},
			existing:     DataArea{Off: -3, Data: Bytes("def")},
			maxMergeSize: 10,
			expectMerge:  true,
			description:  negativeOffsetMsg,
		},
		{
			name:         "Large offsets",
			current:      DataArea{Off: 1000000, Data: Bytes("abc")},
			existing:     DataArea{Off: 1000003, Data: Bytes("def")},
			maxMergeSize: 10,
			expectMerge:  true,
			description:  "Should handle large offsets without overflow",
		},
		{
			name:         "Zero max merge size",
			current:      DataArea{Off: 0, Data: Bytes("a")},
			existing:     DataArea{Off: 1, Data: Bytes("b")},
			maxMergeSize: 0,
			expectMerge:  false,
			description:  "Zero max merge size should prevent all merges",
		},
		{
			name:         "Identical areas",
			current:      DataArea{Off: 5, Data: Bytes("same")},
			existing:     DataArea{Off: 5, Data: Bytes("data")},
			maxMergeSize: 10,
			expectMerge:  true,
			description:  "Identical offsets should merge with current overwriting existing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log("Test description:", tt.description)

			merged, canMerge := tryMergingDataAreas(tt.current, tt.existing, tt.maxMergeSize)

			if canMerge != tt.expectMerge {
				t.Errorf(expectedCanMergeFmt, tt.expectMerge, canMerge)
				return
			}

			if canMerge {
				// Basic sanity checks for successful merges
				expectedStart := min(tt.current.Off, tt.existing.Off)
				if merged.Off != expectedStart {
					t.Errorf("Expected merged offset to be %d, got %d", expectedStart, merged.Off)
				}

				expectedEnd := max(tt.current.Off+tt.current.Data.Size(), tt.existing.Off+tt.existing.Data.Size())
				actualEnd := merged.Off + merged.Data.Size()
				if actualEnd != expectedEnd {
					t.Errorf("Expected merged end to be %d, got %d", expectedEnd, actualEnd)
				}
			}
		})
	}
}

// Helper function to reduce repetition and complexity
func assertDataAreaEqual(t *testing.T, expected, actual DataArea) {
	if actual.Off != expected.Off {
		t.Errorf("Expected offset %d, got %d", expected.Off, actual.Off)
	}
	if string(actual.Data) != string(expected.Data) {
		t.Errorf("Expected data %q, got %q", string(expected.Data), string(actual.Data))
	}
	if actual.Data.Size() != expected.Data.Size() {
		t.Errorf("Expected size %d, got %d", expected.Data.Size(), actual.Data.Size())
	}
}
