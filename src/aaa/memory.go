package aaa

// memory is a file cache layer that stores parts of a cached file in memory.
type memory struct {
	// areas holds cached data areas in memory.
	//
	// Invariants: The areas are non-empty, sorted by position, non-overlapping.
	// Use validateDataAreasInvariants to check invariants.
	//
	// Additional implementation goals:
	//   - Areas should be mostly merged to reduce fragmentation.
	//   - Areas should be compact, i.e. be backed by arrays with no unused capacity.
	areas dataAreas
}

// truncate changes the size of the memory entry, adjusting cached areas as needed.
//
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (memory *memory) truncate(newSize int) int {
	memoryFreed := 0 // Track only the memory that gets freed
	for index := len(memory.areas) - 1; index >= -1; index-- {
		if index == -1 {
			// All areas processed
			memory.areas = memory.areas[:0]
			break
		}
		area := &memory.areas[index]
		if area.position >= newSize {
			// Area starts beyond new size, will be removed entirely
			memoryFreed += len(area.data)
			continue
		}
		truncate := area.end() - newSize
		if truncate > 0 {
			// Area extends beyond new size, truncate it
			memoryFreed += truncate
			truncated := area.data[:area.len()-truncate]
			area.data = truncated.copy()
		}
		// Area is fully within new size, finish processing
		memory.areas = memory.areas[:index+1]
		break
	}
	return -memoryFreed
}

// write caches a copy of the data in the memory at the specified position, overwriting and merging
// where needed. It merges areas unless they exceed mergeSizeHint.
//
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (memory *memory) write(position int, data bytes, mergeSizeHint int) (memoryDelta int) {
	defer func() {
		validateDataAreasInvariants(memory.areas)
	}()

	dataLen := len(data)
	if dataLen == 0 {
		return 0 // Nothing to write, no memory change
	}

	before, mergeLeft, mergeRight, after, memoryDelta := splitAreasForProcessing(memory.areas, position, dataLen)
	leftLen := mergeLeft.len()
	rightLen := mergeRight.len()

	memory.areas = before

	if dataLen+leftLen+rightLen <= mergeSizeHint {
		// Merge all three areas
		merged := make(bytes, 0, leftLen+dataLen+rightLen)
		merged = append(merged, mergeLeft.data...)
		merged = append(merged, data...)
		merged = append(merged, mergeRight.data...)
		if mergeLeft.len() > 0 {
			memory.areas = append(memory.areas, dataArea{position: mergeLeft.position, data: merged})
		} else {
			memory.areas = append(memory.areas, dataArea{position: position, data: merged})
		}

	} else if dataLen+leftLen <= mergeSizeHint {
		// Merge left and current, keep right separate
		merged := make(bytes, 0, leftLen+dataLen)
		merged = append(merged, mergeLeft.data...)
		merged = append(merged, data...)
		if mergeLeft.len() > 0 {
			memory.areas = append(memory.areas, dataArea{position: mergeLeft.position, data: merged})
		} else {
			memory.areas = append(memory.areas, dataArea{position: position, data: merged})
		}
		memory.areas = append(memory.areas, mergeRight.copy())

	} else if dataLen+rightLen <= mergeSizeHint {
		// Keep left separate, merge current and right
		memory.areas = append(memory.areas, mergeLeft.copy())
		merged := make(bytes, 0, dataLen+rightLen)
		merged = append(merged, data...)
		merged = append(merged, mergeRight.data...)
		memory.areas = append(memory.areas, dataArea{position: position, data: merged})

	} else {
		// No merges possible, keep all separate
		memory.areas = append(memory.areas, mergeLeft.copy())
		memory.areas = append(memory.areas, (&dataArea{position: position, data: data}).copy())
		memory.areas = append(memory.areas, mergeRight.copy())
	}

	memory.areas = append(memory.areas, after...)
	return
}

// splitAreasForProcessing determines how a new data region (defined by position and length)
// would interact with the existing areas and calculates the net change in memory usage if the new
// region is written. The function assumes that areas are non-overlapping and sorted by position.
//
// Returns:
//
//	before: areas completely before the new region (not adjacent, no overlap)
//	mergeLeft: the left part of an area that partially overlaps (if any)
//	mergeRight: the right part of an area that partially overlaps (if any)
//	after: areas completely after the new region (not adjacent, no overlap)
//	memoryDelta: net change in memory usage (bytes)
func splitAreasForProcessing(areas dataAreas, position int, length int) (
	before dataAreas, mergeLeft, mergeRight dataArea, after dataAreas, memoryDelta int) {

	mergeLeft = dataArea{}
	mergeRight = dataArea{}
	memoryDelta = length

	end := position + length
	// for very large numbers of areas, a binary search could be more efficient here
	for index, area := range areas {
		areaEnd := area.end()
		if position > areaEnd {
			// Current starts after end of area with space in between, keep area
			before = append(before, area)
			continue
		}
		if end < area.position {
			// Current ends before start of area with space in between, keep all remaining areas
			after = areas[index:]
			break
		}
		if position <= area.position && end >= areaEnd {
			// Current fully overwrites area, update memory delta and discard area
			memoryDelta -= len(area.data)
			continue
		}
		deltaApplied := false
		if position > area.position {
			// Area is partially left of current, mark for merge left and update memory delta
			memoryDelta -= min(end, areaEnd) - position
			deltaApplied = true
			mergeLeft = dataArea{position: area.position, data: area.data[:position-area.position]}
		}
		if end < areaEnd {
			// Area is partially right of current, mark for merge right and update memory delta
			if !deltaApplied {
				memoryDelta -= end - max(position, area.position)
			}
			mergeRight = dataArea{position: end, data: area.data[end-area.position:]}
		}
	}
	return
}
