package cache

type Bytes []byte

func (data Bytes) Size() int64 {
	return int64(len(data))
}

// DataArea represents a contiguous area of data.
// Invariant: Length must be >= 0 (non-negative).
type DataArea struct {
	Off int64
	Len int64
}

// DataAreas represents a collection of data areas.
// Recommended general invariants (not enforced by the type system):
// - Sorted by Offset
// - Non-overlapping
// - Fully merged (adjacent areas combined)
type DataAreas []DataArea

// RemoveOverlappingAreas removes the parts of the areas overlapping the given area.
// The general invariants for data areas are not needed or checked here.
func (areas DataAreas) RemoveOverlappingAreas(removalArea DataArea) DataAreas {
	var result DataAreas

	for _, current := range areas {

		removalEnd := removalArea.Off + removalArea.Len
		currentEnd := current.Off + current.Len

		// No overlap: removalArea ends before current starts OR current ends before removalArea starts
		if removalEnd <= current.Off || currentEnd <= removalArea.Off {
			result = append(result, current)
			continue
		}

		// There is overlap - calculate non-overlapping parts

		// Part before overlap
		if current.Off < removalArea.Off {
			beforeLength := removalArea.Off - current.Off
			result = append(result, DataArea{Off: current.Off, Len: beforeLength})
		}

		// Part after overlap
		if currentEnd > removalEnd {
			afterOffset := removalEnd
			afterLength := currentEnd - removalEnd
			result = append(result, DataArea{Off: afterOffset, Len: afterLength})
		}

		// The overlapping part is not added to result (= discarded)
	}

	return result
}
