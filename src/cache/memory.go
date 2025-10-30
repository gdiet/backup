package cache

// memory is a file cache layer that stores parts of a cached file in memory.
type memory struct {
	// areas holds cached data areas in memory.
	//
	// Invariants: The areas are non-empty, sorted by offset, non-overlapping.
	// Use validateDataAreasInvariants to check invariants.
	//
	// Additional implementation goals:
	//   - Areas should be mostly merged to reduce fragmentation.
	//   - Areas should be compact, i.e. be backed by arrays with no unused capacity.
	areas dataAreas
}

// close clears the memory entry.
// Returns the change in memory usage (bytes allocated) caused by this operation (always negative or zero).
func (memory *memory) close() (memoryDelta int) {
	for _, area := range memory.areas {
		memoryDelta -= area.len()
	}
	memory.areas = nil
	return
}

// read reads data from the memory entry into the provided buffer.
// Returns the areas that were not read because they are not cached.
func (memory *memory) read(off int, data bytes) (unreadAreas areas) {
	end := off + len(data)
	if off == end {
		return nil // Nothing to read
	}

	lastUnread := area{off: off, len: len(data)}

	// For each memory area, try to satisfy part of the read request
	for _, memArea := range memory.areas {
		if memArea.off >= end {
			break // No further areas can satisfy the read
		}
		if memArea.end() <= off {
			continue // This area is before the requested read
		}

		// Determine overlapping range
		readStart := max(off, memArea.off)
		readEnd := min(end, memArea.end())

		// Copy data from memory area to output buffer
		copy(data[readStart-off:], memArea.data[readStart-memArea.off:readEnd-memArea.off])

		// Adjust unread areas
		if readStart > lastUnread.off {
			// There is an unread area before the readStart
			unreadAreas = append(unreadAreas, area{off: lastUnread.off, len: readStart - lastUnread.off})
		}
		lastUnread.off = readEnd
		lastUnread.len = end - readEnd
	}

	if lastUnread.len > 0 {
		return append(unreadAreas, lastUnread)
	}

	return unreadAreas
}

// remove removes memory areas overlapping with the specified area.
//
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (memory *memory) remove(area area) (memoryDelta int) { // TODO align signature with sparse.remove
	defer func() {
		validateDataAreasInvariants(memory.areas)
	}()

	newAreas := dataAreas{}
	for index, data := range memory.areas {
		if data.end() <= area.off {
			// dataArea ends before the cleared area starts, keep dataArea
			newAreas = append(newAreas, data)
			continue
		}
		if data.off >= area.end() {
			// dataArea starts after the cleared area ends, keep remaining dataAreas
			newAreas = append(newAreas, memory.areas[index:]...)
			break
		}
		if data.off >= area.off && data.end() <= area.end() {
			// dataArea fully within cleared area, remove it
			memoryDelta -= len(data.data)
			continue
		}
		deltaApplied := false
		if data.off < area.off {
			// dataArea partially left of cleared area, trim it
			trimmedLen := area.off - data.off
			trimmed := data.data[:trimmedLen]
			newAreas = append(newAreas, dataArea{off: data.off, data: trimmed.copy()})
			memoryDelta -= len(data.data) - trimmedLen
			deltaApplied = true
		}
		if data.end() > area.end() {
			// dataArea partially right of cleared area, trim it
			trimmedLen := data.end() - area.end()
			trimmed := data.data[len(data.data)-trimmedLen:]
			newAreas = append(newAreas, dataArea{off: area.end(), data: trimmed.copy()})
			if !deltaApplied {
				memoryDelta -= len(data.data) - trimmedLen
			}
		}
	}
	memory.areas = newAreas
	return
}

// truncate changes the size of the memory entry, adjusting cached areas as needed.
//
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (memory *memory) truncate(newSize int) (memoryDelta int) {
	defer func() {
		validateDataAreasInvariants(memory.areas)
	}()

	for index := len(memory.areas) - 1; index >= -1; index-- {
		if index == -1 {
			// All areas processed
			memory.areas = memory.areas[:0]
			break
		}
		area := &memory.areas[index]
		if area.off >= newSize {
			// Area starts beyond new size, will be removed entirely
			memoryDelta -= len(area.data)
			continue
		}
		truncate := area.end() - newSize
		if truncate > 0 {
			// Area extends beyond new size, truncate it
			memoryDelta -= truncate
			truncated := area.data[:area.len()-truncate]
			area.data = truncated.copy()
		}
		// Area is fully within new size, finish processing
		memory.areas = memory.areas[:index+1]
		break
	}
	return memoryDelta
}

// write caches a copy of the data in the memory at the specified offset, overwriting and merging
// where needed. It merges areas unless they exceed mergeSizeHint.
//
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (memory *memory) write(off int, data bytes, mergeSizeHint int) (memoryDelta int) {
	dataLen := len(data)
	if dataLen == 0 {
		return 0 // Nothing to write, no memory change
	}

	defer func() {
		validateDataAreasInvariants(memory.areas)
	}()

	before, mergeLeft, mergeRight, after, memoryDelta := splitAreasForProcessing(memory.areas, off, dataLen)
	leftLen := mergeLeft.len()
	rightLen := mergeRight.len()

	memory.areas = before

	if dataLen+leftLen+rightLen <= mergeSizeHint {
		// Merge all three areas
		merged := make(bytes, 0, leftLen+dataLen+rightLen)
		merged = append(merged, mergeLeft.data...)
		merged = append(merged, data...)
		merged = append(merged, mergeRight.data...)
		if leftLen > 0 {
			memory.areas = append(memory.areas, dataArea{off: mergeLeft.off, data: merged})
		} else {
			memory.areas = append(memory.areas, dataArea{off: off, data: merged})
		}

	} else if dataLen+leftLen <= mergeSizeHint {
		// Merge left and current, keep right separate
		merged := make(bytes, 0, leftLen+dataLen)
		merged = append(merged, mergeLeft.data...)
		merged = append(merged, data...)
		if leftLen > 0 {
			memory.areas = append(memory.areas, dataArea{off: mergeLeft.off, data: merged})
		} else {
			memory.areas = append(memory.areas, dataArea{off: off, data: merged})
		}
		if rightLen > 0 { // not necessary here, just for clarity
			memory.areas = append(memory.areas, mergeRight.copy())
		}

	} else if dataLen+rightLen <= mergeSizeHint {
		// Keep left separate, merge current and right
		if leftLen > 0 { // not necessary here, just for clarity
			memory.areas = append(memory.areas, mergeLeft.copy())
		}
		merged := make(bytes, 0, dataLen+rightLen)
		merged = append(merged, data...)
		merged = append(merged, mergeRight.data...)
		memory.areas = append(memory.areas, dataArea{off: off, data: merged})

	} else {
		// No merges possible, keep all separate
		if leftLen > 0 {
			memory.areas = append(memory.areas, mergeLeft.copy())
		}
		memory.areas = append(memory.areas, (&dataArea{off: off, data: data}).copy())
		if rightLen > 0 {
			memory.areas = append(memory.areas, mergeRight.copy())
		}
	}

	memory.areas = append(memory.areas, after...)
	return
}

// splitAreasForProcessing determines how a new data region (defined by offset and length)
// would interact with the existing areas and calculates the net change in memory usage if the new
// region is written. The function assumes that areas are non-overlapping and sorted by offset.
//
// Returns:
//
//	before: areas completely before the new region (not adjacent, no overlap)
//	mergeLeft: the left part of an area that partially overlaps (if any)
//	mergeRight: the right part of an area that partially overlaps (if any)
//	after: areas completely after the new region (not adjacent, no overlap)
//	memoryDelta: net change in memory usage (bytes)
func splitAreasForProcessing(areas dataAreas, off int, length int) (
	before dataAreas, mergeLeft, mergeRight dataArea, after dataAreas, memoryDelta int) {

	mergeLeft = dataArea{}
	mergeRight = dataArea{}
	memoryDelta = length

	end := off + length
	// for very large numbers of areas, a binary search could be more efficient here
	for index, area := range areas {
		areaEnd := area.end()
		if off > areaEnd {
			// Current starts after end of area with space in between, keep area
			before = append(before, area)
			continue
		}
		if end < area.off {
			// Current ends before start of area with space in between, keep all remaining areas
			after = areas[index:]
			break
		}
		if off <= area.off && end >= areaEnd {
			// Current fully overwrites area, update memory delta and discard area
			memoryDelta -= len(area.data)
			continue
		}
		deltaApplied := false
		if off > area.off {
			// Area is partially left of current, mark for merge left and update memory delta
			memoryDelta -= min(end, areaEnd) - off
			deltaApplied = true
			mergeLeft = dataArea{off: area.off, data: area.data[:off-area.off]}
		}
		if end < areaEnd {
			// Area is partially right of current, mark for merge right and update memory delta
			if !deltaApplied {
				memoryDelta -= end - max(off, area.off)
			}
			mergeRight = dataArea{off: end, data: area.data[end-area.off:]}
		}
	}
	return
}
