package cache

// memory is a file cache layer that stores parts of a cached file in memory.
type memory struct {
	// areas holds data areas cached in memory.
	areas dataAreas
}

// read reads data from the memory entry into the provided buffer.
//
// Returns the areas that were not read.
func (m *memory) read(off int64, data bytes) (unreadAreas areas) {
	end := off + int64(len(data))
	if off == end {
		return nil // Nothing to read
	}

	lastUnread := area{off: off, len: int64(len(data))}

	// For each memory area, try to satisfy part of the read request
	for _, a := range m.areas {
		if a.off >= end {
			break // No further areas can satisfy the read
		}
		if a.end() <= off {
			continue // This area is before the requested read
		}

		// Determine overlapping range
		readStart := max(off, a.off)
		readEnd := min(end, a.end())

		// Copy data from memory to output buffer
		copy(data[readStart-off:], a.data[readStart-a.off:readEnd-a.off])

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

// shrink cuts off cached data beyond the new size.
//
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (m *memory) shrink(newSize int64) (memoryDelta int) {
	defer func() {
		validateDataAreasInvariants(m.areas)
	}()

	// Remove or truncate areas beyond new size.
	// Reverse iteration facilitates memoryDelta calculation.
	for i := len(m.areas) - 1; i >= -1; i-- {
		if i == -1 {
			// All areas are beyond new size, remove all
			m.areas = nil
			break
		}
		area := &m.areas[i]
		if area.off >= newSize {
			// Area starts beyond new size, will be removed entirely
			memoryDelta -= len(area.data)
			continue
		}
		truncate := area.end() - newSize
		if truncate > 0 {
			// Area extends beyond new size, truncate it
			memoryDelta -= int(truncate)
			truncatedData := area.data[:area.len()-truncate]
			area.data = truncatedData.copy()
		}
		// Area has been truncated or is fully within new size, remove later areas
		m.areas = m.areas[:i+1]
		break
	}
	return memoryDelta
}

// remove removes memory areas overlapping with the specified area.
//
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (m *memory) remove(off int64, length int64) (memoryDelta int) {
	if length <= 0 {
		return // Nothing to write or invalid length
	}
	defer func() {
		validateDataAreasInvariants(m.areas)
	}()

	end := off + length
	newAreas := dataAreas{}
	for index, currentArea := range m.areas {
		if currentArea.end() <= off {
			// Area is completely before the removed area, keep it
			newAreas = append(newAreas, currentArea)
		} else if currentArea.off >= end {
			// Area is completely after the removed area, keep it and remaining areas
			newAreas = append(newAreas, m.areas[index:]...)
			break
		} else {
			// Area overlaps with the removed area
			memoryDelta -= len(currentArea.data)
			if currentArea.off < off {
				// There is a part left of the removed area
				trimmed := currentArea.data[:off-currentArea.off]
				newAreas = append(newAreas, dataArea{off: currentArea.off, data: trimmed.copy()})
				memoryDelta += int(len(trimmed))
			}
			if currentArea.end() > end {
				// There is a part right of the removed area
				excessLen := currentArea.end() - end
				trimmed := currentArea.data[len(currentArea.data)-int(excessLen):]
				newAreas = append(newAreas, dataArea{off: end, data: trimmed.copy()})
				memoryDelta += int(excessLen)
			}
		}
	}
	m.areas = newAreas
	return memoryDelta
}

// write caches a copy of the data in the memory at the specified offset, overwriting and merging
// where needed. It merges areas unless they exceed mergeSizeHint.
//
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (m *memory) write(off int64, data bytes, mergeSizeHint int64) (memoryDelta int) {
	dataLen := len(data)
	if dataLen == 0 {
		return 0 // Nothing to write, no memory change
	}

	defer func() {
		validateDataAreasInvariants(m.areas)
	}()

	before, mergeLeft, mergeRight, after, memoryDelta := splitAreasForProcessing(m.areas, off, int64(dataLen))
	m.areas = before

	// Apply merging strategy and add resulting areas
	mergedAreas := determineMergeStrategy(off, data, mergeLeft, mergeRight, mergeSizeHint)
	m.areas = append(m.areas, mergedAreas...)
	m.areas = append(m.areas, after...)
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
func splitAreasForProcessing(areas dataAreas, off int64, length int64) (
	before dataAreas, mergeLeft, mergeRight dataArea, after dataAreas, memoryDelta int) {

	mergeLeft = dataArea{}
	mergeRight = dataArea{}
	memoryDelta = int(length)

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
			memoryDelta -= int(min(end, areaEnd) - off)
			deltaApplied = true
			mergeLeft = dataArea{off: area.off, data: area.data[:off-area.off]}
		}
		if end < areaEnd {
			// Area is partially right of current, mark for merge right and update memory delta
			if !deltaApplied {
				memoryDelta -= int(end - max(off, area.off))
			}
			mergeRight = dataArea{off: end, data: area.data[end-area.off:]}
		}
	}
	return
}

// determineMergeStrategy determines which areas to merge based on size constraints
func determineMergeStrategy(off int64, data bytes, mergeLeft, mergeRight dataArea, mergeSizeHint int64) dataAreas {
	dataLen := int64(len(data))
	leftLen := mergeLeft.len()
	rightLen := mergeRight.len()

	if dataLen+leftLen+rightLen <= mergeSizeHint {
		return mergeAllThreeAreas(off, data, mergeLeft, mergeRight)
	}

	if dataLen+leftLen <= mergeSizeHint {
		return mergeLeftAndCurrent(off, data, mergeLeft, mergeRight)
	}

	if dataLen+rightLen <= mergeSizeHint {
		return mergeCurrentAndRight(off, data, mergeLeft, mergeRight)
	}

	return keepAllSeparate(off, data, mergeLeft, mergeRight)
}

// mergeAllThreeAreas merges left, current, and right areas into one
func mergeAllThreeAreas(off int64, data bytes, mergeLeft, mergeRight dataArea) dataAreas {
	leftLen := mergeLeft.len()
	merged := make(bytes, 0, int(leftLen)+len(data)+int(mergeRight.len()))
	merged = append(merged, mergeLeft.data...)
	merged = append(merged, data...)
	merged = append(merged, mergeRight.data...)

	areaOff := off
	if leftLen > 0 {
		areaOff = mergeLeft.off
	}
	return dataAreas{{off: areaOff, data: merged}}
}

// mergeLeftAndCurrent merges left and current areas, keeps right separate
func mergeLeftAndCurrent(off int64, data bytes, mergeLeft, mergeRight dataArea) dataAreas {
	leftLen := mergeLeft.len()
	merged := make(bytes, 0, int(leftLen)+len(data))
	merged = append(merged, mergeLeft.data...)
	merged = append(merged, data...)

	result := make(dataAreas, 0, 2)
	areaOff := off
	if leftLen > 0 {
		areaOff = mergeLeft.off
	}
	result = append(result, dataArea{off: areaOff, data: merged})

	if mergeRight.len() > 0 {
		result = append(result, mergeRight.copy())
	}
	return result
}

// mergeCurrentAndRight keeps left separate, merges current and right areas
func mergeCurrentAndRight(off int64, data bytes, mergeLeft, mergeRight dataArea) dataAreas {
	merged := make(bytes, 0, len(data)+int(mergeRight.len()))
	merged = append(merged, data...)
	merged = append(merged, mergeRight.data...)

	result := make(dataAreas, 0, 2)
	if mergeLeft.len() > 0 {
		result = append(result, mergeLeft.copy())
	}
	result = append(result, dataArea{off: off, data: merged})
	return result
}

// keepAllSeparate keeps left, current, and right areas separate
func keepAllSeparate(off int64, data bytes, mergeLeft, mergeRight dataArea) dataAreas {
	result := make(dataAreas, 0, 3)
	if mergeLeft.len() > 0 {
		result = append(result, mergeLeft.copy())
	}
	result = append(result, (&dataArea{off: off, data: data}).copy())
	if mergeRight.len() > 0 {
		result = append(result, mergeRight.copy())
	}
	return result
}

// close clears the memory entry.
// Returns the change in memory usage (bytes allocated) caused by this operation (always negative or zero).
func (m *memory) close() (memoryDelta int) {
	for _, area := range m.areas {
		memoryDelta -= int(area.len())
	}
	m.areas = nil
	return
}
