package cache

// Area represents a contiguous area of data in a file.
// Invariant: Length must be >= 0 (non-negative).
type Area struct {
	Off int64
	Len int64
}

// Areas represents a collection of areas.
// Recommended general invariants (not enforced by the type system):
// - Sorted by Offset
// - Non-overlapping
// - Fully merged (adjacent areas combined)
type Areas []Area

// RemoveOverlappingAreas removes the parts of the areas overlapping the given area.
// The general invariants for data areas are not needed or checked here.
func (areas Areas) RemoveOverlappingAreas(removalArea Area) Areas {
	var result Areas

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
			result = append(result, Area{Off: current.Off, Len: beforeLength})
		}

		// Part after overlap
		if currentEnd > removalEnd {
			afterOffset := removalEnd
			afterLength := currentEnd - removalEnd
			result = append(result, Area{Off: afterOffset, Len: afterLength})
		}

		// The overlapping part is not added to result (= discarded)
	}

	return result
}
