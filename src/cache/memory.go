package cache

type Bytes []byte

func (data Bytes) Size() int64 {
	return int64(len(data))
}

// DataArea represents a contiguous area of bytes in a file.
type DataArea struct {
	Off  int64
	Data Bytes
}

// DataAreas represents a collection of data areas.
// Recommended general invariants (not enforced by the type system):
// - Sorted by Offset
// - Non-overlapping
// - Mostly merged (adjacent areas combined)
type DataAreas []DataArea

// Memory is a file cache layer that stores parts of a cached file in memory.
type Memory struct {
	areas DataAreas
}

// Read reads data from the memory entry.
// Returns the Areas that were not read.
func (memory *Memory) Read(position int64, data Bytes) Areas {
	if len(data) == 0 { // TODO check whether this condition is needed or helpful
		return nil // Nothing to read
	}

	// Initialize unread areas with the full requested area
	unreadAreas := Areas{Area{Off: position, Len: data.Size()}}

	// For each memory area, try to satisfy parts of the read request
	for _, memArea := range memory.areas {
		readStart := max(position, memArea.Off)
		readEnd := min(position+data.Size(), memArea.Off+memArea.Data.Size())
		if readStart >= readEnd {
			continue // No overlap
		}

		// Copy data from memory area to output buffer
		copy(data[readStart-position:readEnd-position], memArea.Data[readStart-memArea.Off:readEnd-memArea.Off])

		// Adjust unread areas
		unreadAreas = unreadAreas.RemoveOverlappingAreas(Area{Off: readStart, Len: readEnd - readStart})
	}

	return unreadAreas
}

// Truncate changes the size of the memory entry, adjusting cached areas as needed.
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (memory *Memory) Truncate(newSize int64) int {
	memoryFreed := 0 // Track only the memory that gets freed
	var filteredAreas DataAreas

	for _, area := range memory.areas {
		maxIncluded := newSize - area.Off

		if maxIncluded <= 0 {
			// Area starts beyond new size, remove it entirely
			memoryFreed += len(area.Data)
			continue
		}
		if area.Data.Size() > maxIncluded {
			// Area extends beyond new size, truncate it
			memoryFreed += len(area.Data) - int(maxIncluded)
			area.Data = area.Data[:maxIncluded]
		}
		filteredAreas = append(filteredAreas, area)
	}

	memory.areas = filteredAreas
	return -memoryFreed // Return negative value since memory was freed
}

// Write caches the data in the memory at the specified position, overwriting and merging where needed.
// Required invariants: Sorted by Offset, Non-overlapping.
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (memory *Memory) Write(position int64, data Bytes, maxMergeSize int64) int {
	if len(data) == 0 {
		return 0 // Nothing to write, no memory change
	}

	memoryDelta := len(data) // Start with the new data being added
	var newAreas DataAreas
	current := DataArea{Off: position, Data: data}

	for index, area := range memory.areas {
		if current.Off > area.Off+area.Data.Size() {
			// Current starts after end of area, keep area
			newAreas = append(newAreas, area)
			continue
		}
		if current.Off+current.Data.Size() < area.Off {
			// Current ends before area starts, keep current and remaining areas
			newAreas = append(newAreas, current)
			newAreas = append(newAreas, memory.areas[index:]...)
			current = DataArea{} // Mark as processed
			break
		}
		// There is overlap or adjacency, try to merge areas
		if merged, canMerge := tryMergingDataAreas(current, area, maxMergeSize); canMerge {
			// Account for the area being replaced by the merge
			memoryDelta -= len(area.Data)    // Subtract existing area that gets replaced
			memoryDelta -= len(current.Data) // Subtract current area (already counted)
			memoryDelta += len(merged.Data)  // Add merged result
			current = merged
			continue
		} else {
			// Trim areas to avoid overlap - area will be replaced by trimmed version
			first, second := trimOverlappingAreas(current, area)
			memoryDelta -= len(area.Data)   // Subtract original area
			memoryDelta += len(second.Data) // Add trimmed area back
			newAreas = append(newAreas, first, second)
			newAreas = append(newAreas, memory.areas[index+1:]...)
			current = DataArea{} // Mark as processed
			break
		}
	}

	// If current still has data, it needs to be appended
	if len(current.Data) > 0 {
		newAreas = append(newAreas, current)
	}

	memory.areas = newAreas
	return memoryDelta
}

// Clear removes data in the specified area from the memory entry.
// Required invariants: Sorted by Offset, Non-overlapping.
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (memory *Memory) Clear(area Area) int {
	areaEnd := area.Off + area.Len
	memoryDelta := 0
	newAreas := make([]DataArea, 0, len(memory.areas))
	for index, current := range memory.areas {
		currentEnd := current.Off + current.Data.Size()
		// current behind area?
		if current.Off >= areaEnd {
			newAreas = append(newAreas, memory.areas[index:]...)
			break
		}
		// current before area?
		if currentEnd <= area.Off {
			newAreas = append(newAreas, current)
			continue
		}
		// current fully contained in area?
		if current.Off >= area.Off && currentEnd <= areaEnd {
			memoryDelta -= int(current.Data.Size())
			continue // Remove area
		}
		// clear region strictly inside current area: split into two
		if current.Off < area.Off && currentEnd > areaEnd {
			leftLen := area.Off - current.Off
			rightLen := currentEnd - areaEnd
			leftData := make(Bytes, leftLen)
			copy(leftData, current.Data[:leftLen])
			rightData := make(Bytes, rightLen)
			copy(rightData, current.Data[current.Data.Size()-rightLen:])
			memoryDelta -= int(current.Data.Size() - (leftLen + rightLen))
			newAreas = append(newAreas, DataArea{Off: current.Off, Data: leftData})
			newAreas = append(newAreas, DataArea{Off: areaEnd, Data: rightData})
			newAreas = append(newAreas, memory.areas[index+1:]...)
			break
		}
		// overlap at start of area?
		if current.Off < area.Off {
			// Trim right
			trimLen := area.Off - current.Off
			newData := make(Bytes, trimLen)
			copy(newData, current.Data[:trimLen])
			memoryDelta -= len(current.Data) - len(newData)
			current.Data = newData
			newAreas = append(newAreas, current)
			continue
		}
		// overlap at end of area ... trim left
		trimLen := currentEnd - areaEnd
		newData := make(Bytes, trimLen)
		copy(newData, current.Data[current.Data.Size()-trimLen:])
		memoryDelta -= len(current.Data) - len(newData)
		current.Off = areaEnd
		current.Data = newData
		newAreas = append(newAreas, current)
		newAreas = append(newAreas, memory.areas[index+1:]...)
		break
	}
	memory.areas = newAreas
	return memoryDelta
}

// Close clears the memory entry.
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (memory *Memory) Close() int {
	// Calculate current memory usage before clearing
	totalBytes := 0
	for _, area := range memory.areas {
		totalBytes += len(area.Data)
	}
	memory.areas = nil
	return -totalBytes // All memory is freed, so negative change
}

// tryMergingDataAreas merges two overlapping or adjacent data areas into one.
// Returns the merged area and true if merging was successful.
func tryMergingDataAreas(current, existing DataArea, maxMergeSize int64) (DataArea, bool) {
	mergeStart := min(current.Off, existing.Off)
	mergeEnd := max(current.Off+current.Data.Size(), existing.Off+existing.Data.Size())

	if mergeEnd-mergeStart > maxMergeSize {
		return DataArea{}, false // Too large to merge
	}

	// Merge areas
	mergedData := make(Bytes, mergeEnd-mergeStart)

	// Copy each area's data to its correct position
	existingStart := existing.Off - mergeStart
	existingEnd := existingStart + existing.Data.Size()
	copy(mergedData[existingStart:existingEnd], existing.Data)

	currentStart := current.Off - mergeStart
	currentEnd := currentStart + current.Data.Size()
	copy(mergedData[currentStart:currentEnd], current.Data)

	return DataArea{Off: mergeStart, Data: mergedData}, true
}

// trimOverlappingAreas trims overlapping areas to avoid conflicts.
// Returns the two trimmed areas in order.
func trimOverlappingAreas(current, existing DataArea) (DataArea, DataArea) {
	if current.Off < existing.Off {
		// Trim current to end before existing starts
		current.Data = current.Data[:existing.Off-current.Off]
		return current, existing
	} else {
		// Trim existing to end before current starts
		existing.Data = existing.Data[:current.Off-existing.Off]
		return existing, current
	}
}
