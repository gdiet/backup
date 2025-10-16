package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrimOverlappingAreas(t *testing.T) {
	tests := []struct {
		name        string
		top, bottom dataArea
		expected    dataAreas
		expectPanic bool
	}{
		{
			name:     "Top overlaps start of bottom #1",
			top:      dataArea{position: 0, data: bytes("hello")},
			bottom:   dataArea{position: 3, data: bytes("world")},
			expected: dataAreas{dataArea{position: 0, data: bytes("hello")}, dataArea{position: 5, data: bytes("rld")}},
		},
		{
			name:     "Top overlaps start of bottom #2",
			top:      dataArea{position: 0, data: bytes("1234")},
			bottom:   dataArea{position: 2, data: bytes("abcdefg")},
			expected: dataAreas{dataArea{position: 0, data: bytes("1234")}, dataArea{position: 4, data: bytes("cdefg")}},
		},
		{
			name:     "Top fully covers bottom #1",
			top:      dataArea{position: 0, data: bytes("1234")},
			bottom:   dataArea{position: 0, data: bytes("abcd")},
			expected: dataAreas{dataArea{position: 0, data: bytes("1234")}},
		},
		{
			name:     "Top fully covers bottom #2",
			top:      dataArea{position: 0, data: bytes("1234")},
			bottom:   dataArea{position: 1, data: bytes("ab")},
			expected: dataAreas{dataArea{position: 0, data: bytes("1234")}},
		},
		{
			name:     "Top overlaps end of bottom #1",
			top:      dataArea{position: 3, data: bytes("hello")},
			bottom:   dataArea{position: 0, data: bytes("world")},
			expected: dataAreas{dataArea{position: 0, data: bytes("wor")}, dataArea{position: 3, data: bytes("hello")}},
		},
		{
			name:     "Top overlaps end of bottom #2",
			top:      dataArea{position: 2, data: bytes("1234")},
			bottom:   dataArea{position: 0, data: bytes("abcdef")},
			expected: dataAreas{dataArea{position: 0, data: bytes("ab")}, dataArea{position: 2, data: bytes("1234")}},
		},
		{
			name:   "Top is fully contained in bottom",
			top:    dataArea{position: 1, data: bytes("12")},
			bottom: dataArea{position: 0, data: bytes("abcd")},
			expected: dataAreas{
				dataArea{position: 0, data: bytes("a")},
				dataArea{position: 1, data: bytes("12")},
				dataArea{position: 3, data: bytes("d")}},
		},
		{
			name:        "NoOverlapPanicTopBeforeBottom",
			top:         dataArea{position: 1, data: bytes("12")},
			bottom:      dataArea{position: 10, data: bytes("abcd")},
			expectPanic: true,
		},
		{
			name:        "NoOverlapPanicTopAfterBottom",
			top:         dataArea{position: 10, data: bytes("12")},
			bottom:      dataArea{position: 1, data: bytes("abcd")},
			expectPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				require.Panics(t, func() { removeOverlaps(tt.top, tt.bottom) })
			} else {
				result := removeOverlaps(tt.top, tt.bottom)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}
