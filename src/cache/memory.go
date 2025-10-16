package cache

// memory is a file cache layer that stores parts of a cached file in memory.
type memory struct {
	areas dataAreas
}

// // Write caches a copy of the data in the memory at the specified position, overwriting and merging where needed.
// // Required invariants: Sorted by Offset, Non-overlapping.
// // Returns the change in memory usage (bytes allocated) caused by this operation.
// func (memory *memory) Write(position int, data bytes, maxMergeSize int) int {
// 	if len(data) == 0 {
// 		return 0 // Nothing to write, no memory change
// 	}

// 	memoryDelta := len(data) // Start with the new data being added
// 	var newAreas dataAreas
// 	current := dataArea{position: position, data: data}

// 	for index, area := range memory.areas {
// 		if current.position > area.position+len(area.data) {
// 			// Current starts after end of area, keep area
// 			newAreas = append(newAreas, area)
// 			continue
// 		}
// 		if current.position+len(current.data) < area.position {
// 			// Current ends before area starts, keep current and remaining areas
// 			newAreas = append(newAreas, current)
// 			newAreas = append(newAreas, memory.areas[index:]...)
// 			current = dataArea{} // Mark as processed
// 			break
// 		}
// 		// There is overlap or adjacency, try to merge areas
// 		if merged, canMerge := tryMergingDataAreas(current, area, maxMergeSize); canMerge {
// 			// Account for the area being replaced by the merge
// 			memoryDelta -= len(area.data)    // Subtract existing area that gets replaced
// 			memoryDelta -= len(current.data) // Subtract current area (already counted)
// 			memoryDelta += len(merged.data)  // Add merged result
// 			current = merged
// 			continue
// 		} else {
// 			// Trim areas to avoid overlap - area will be replaced by trimmed version
// 			first, second := trimOverlappingAreas(current, area)
// 			memoryDelta -= len(area.data)   // Subtract original area
// 			memoryDelta += len(second.data) // Add trimmed area back
// 			newAreas = append(newAreas, first, second)
// 			newAreas = append(newAreas, memory.areas[index+1:]...)
// 			current = dataArea{} // Mark as processed
// 			break
// 		}
// 	}

// 	// If current still has data, it needs to be appended
// 	if len(current.data) > 0 {
// 		newAreas = append(newAreas, current)
// 	}

// 	memory.areas = newAreas
// 	return memoryDelta
// }

// // tryMergingDataAreas merges two overlapping or adjacent data areas into one.
// // Returns the merged area and true if merging was successful.
// func tryMergingDataAreas(current, existing dataArea, maxMergeSize int) (dataArea, bool) {
// 	mergeStart := min(current.position, existing.position)
// 	mergeEnd := max(current.position+len(current.data), existing.position+len(existing.data))

// 	if mergeEnd-mergeStart > maxMergeSize {
// 		return dataArea{}, false // Too large to merge
// 	}

// 	// Merge areas
// 	mergedData := make(bytes, mergeEnd-mergeStart)

// 	// Copy each area's data to its correct position
// 	existingStart := existing.position - mergeStart
// 	existingEnd := existingStart + len(existing.data)
// 	copy(mergedData[existingStart:existingEnd], existing.data)

// 	currentStart := current.position - mergeStart
// 	currentEnd := currentStart + len(current.data)
// 	copy(mergedData[currentStart:currentEnd], current.data)

// 	return dataArea{position: mergeStart, data: mergedData}, true
// }

// removeOverlaps removes overlaps of two areas. Areas must overlap.
// Returns the 1..3 resulting areas ordered by position.
func removeOverlaps(topLayer, bottomLayer dataArea) dataAreas {
	deltaPosition := bottomLayer.position - topLayer.position
	deltaEnd := deltaPosition + len(bottomLayer.data) - len(topLayer.data)
	if deltaPosition >= 0 {
		if deltaEnd > 0 {
			// Bottom starts inside top and ends outside
			bottomLayer.position = topLayer.position + len(topLayer.data)
			bottomLayer.data = bottomLayer.data[len(topLayer.data)-deltaPosition:]
			return dataAreas{topLayer, bottomLayer}
		}
		// Bottom is contained within top
		return dataAreas{topLayer}
	}
	if deltaEnd <= 0 {
		// Bottom starts before top and ends inside
		bottomLayer.data = bottomLayer.data[:-deltaPosition]
		return dataAreas{bottomLayer, topLayer}
	}
	// Bottom starts before top and ends after
	bottomStart := dataArea{position: bottomLayer.position, data: bottomLayer.data[:-deltaPosition]}
	bottomEnd := dataArea{position: topLayer.position + len(topLayer.data), data: bottomLayer.data[len(topLayer.data)-deltaPosition:]}
	return dataAreas{bottomStart, topLayer, bottomEnd}
}
