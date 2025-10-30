package cache

import (
	"reflect"
	"testing"
)

func TestSplitAreasForProcessing(t *testing.T) {
	testCases := []struct {
		name      string
		areas     dataAreas
		position  int
		length    int
		expBefore dataAreas
		expMergeL dataArea
		expMergeR dataArea
		expAfter  dataAreas
		expDelta  int
	}{
		{
			name:      "no overlap, insert at start",
			areas:     dataAreas{{position: 10, data: []byte{1, 2, 3}}},
			position:  0,
			length:    5,
			expBefore: nil,
			expMergeL: dataArea{},
			expMergeR: dataArea{},
			expAfter:  dataAreas{{position: 10, data: []byte{1, 2, 3}}},
			expDelta:  5,
		},
		{
			name:      "no overlap, insert at end",
			areas:     dataAreas{{position: 0, data: []byte{1, 2, 3}}},
			position:  10,
			length:    2,
			expBefore: dataAreas{{position: 0, data: []byte{1, 2, 3}}},
			expMergeL: dataArea{},
			expMergeR: dataArea{},
			expAfter:  nil,
			expDelta:  2,
		},
		{
			name:      "exact full overwrite",
			areas:     dataAreas{{position: 5, data: []byte{1, 2, 3}}},
			position:  5,
			length:    3,
			expBefore: nil,
			expMergeL: dataArea{},
			expMergeR: dataArea{},
			expAfter:  nil,
			expDelta:  0,
		},
		{
			name:      "extended full overwrite",
			areas:     dataAreas{{position: 6, data: []byte{2}}},
			position:  5,
			length:    3,
			expBefore: nil,
			expMergeL: dataArea{},
			expMergeR: dataArea{},
			expAfter:  nil,
			expDelta:  2,
		},
		{
			name:      "partial left visible",
			areas:     dataAreas{{position: 0, data: []byte{1, 2, 3, 4, 5}}},
			position:  2,
			length:    3,
			expBefore: nil,
			expMergeL: dataArea{position: 0, data: []byte{1, 2}},
			expMergeR: dataArea{},
			expAfter:  nil,
			expDelta:  0,
		},
		{
			name:      "extended partial left visible",
			areas:     dataAreas{{position: 0, data: []byte{1, 2, 3, 4, 5}}},
			position:  2,
			length:    6,
			expBefore: nil,
			expMergeL: dataArea{position: 0, data: []byte{1, 2}},
			expMergeR: dataArea{},
			expAfter:  nil,
			expDelta:  3,
		},
		{
			name:      "partial right visible",
			areas:     dataAreas{{position: 0, data: []byte{1, 2, 3, 4, 5}}},
			position:  0,
			length:    3,
			expBefore: nil,
			expMergeL: dataArea{},
			expMergeR: dataArea{position: 3, data: []byte{4, 5}},
			expAfter:  nil,
			expDelta:  0,
		},
		{
			name:      "extended partial right visible",
			areas:     dataAreas{{position: 3, data: []byte{1, 2, 3, 4, 5}}},
			position:  0,
			length:    6,
			expBefore: nil,
			expMergeL: dataArea{},
			expMergeR: dataArea{position: 6, data: []byte{4, 5}},
			expAfter:  nil,
			expDelta:  3,
		},
		{
			name:      "both sides visible",
			areas:     dataAreas{{position: 0, data: []byte{1, 2, 3, 4, 5}}},
			position:  1,
			length:    3,
			expBefore: nil,
			expMergeL: dataArea{position: 0, data: []byte{1}},
			expMergeR: dataArea{position: 4, data: []byte{5}},
			expAfter:  nil,
			expDelta:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			before, mergeL, mergeR, after, delta := splitAreasForProcessing(tc.areas, tc.position, tc.length)
			if !reflect.DeepEqual(before, tc.expBefore) {
				t.Errorf("before: got %v, want %v", before, tc.expBefore)
			}
			if !reflect.DeepEqual(mergeL, tc.expMergeL) {
				t.Errorf("mergeLeft: got %v, want %v", mergeL, tc.expMergeL)
			}
			if !reflect.DeepEqual(mergeR, tc.expMergeR) {
				t.Errorf("mergeRight: got %v, want %v", mergeR, tc.expMergeR)
			}
			if !reflect.DeepEqual(after, tc.expAfter) {
				t.Errorf("after: got %v, want %v", after, tc.expAfter)
			}
			if delta != tc.expDelta {
				t.Errorf("memoryDelta: got %v, want %v", delta, tc.expDelta)
			}
		})
	}
}
