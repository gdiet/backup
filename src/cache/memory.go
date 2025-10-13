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
	if len(data) == 0 {
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
func (memory *Memory) Truncate(newSize int64) {
	var filteredAreas DataAreas
	for _, area := range memory.areas {
		if area.Off >= newSize {
			continue // Area starts beyond new size, remove it
		}
		if area.Off+area.Data.Size() > newSize {
			area.Data = area.Data[:newSize-area.Off] // Truncate area data
		}
		filteredAreas = append(filteredAreas, area)
	}

	memory.areas = filteredAreas
}

// Write caches the data in the memory at the specified position, overwriting and merging where needed.
// Required invariants: Sorted by Offset, Non-overlapping.
func (memory *Memory) Write(position int64, data Bytes, maxMergeSize int64) {
	if len(data) == 0 {
		return // Nothing to write
	}
	// Add the new area at the correct position
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
			current = merged
			continue
		} else {
			// Trim areas to avoid overlap
			first, second := trimOverlappingAreas(current, area)
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
}

// Close clears the memory entry.
func (memory *Memory) Close() {
	memory.areas = nil
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
