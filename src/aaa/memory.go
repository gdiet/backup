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

// Write caches a copy of the data in the memory at the specified position, overwriting and merging where needed.
// Returns the change in memory usage (bytes allocated) caused by this operation.
func (memory *memory) Write(position int, data bytes, maxMergeSize int) int {
	defer func() {
		validateDataAreasInvariants(memory.areas)
	}()
	// before, mergeLeft, mergeRight, after, memoryDelta := splitAreasForProcessing(memory.areas, position, len(data))
	panic("not implemented yet")
}

func splitAreasForProcessing(areas dataAreas, position int, length int) (
	before dataAreas, mergeLeft, mergeRight *dataArea, after dataAreas, memoryDelta int) {
	end := position + length
	memoryDelta = length
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
		if position > area.position {
			// Area is partially left of current, mark for merge left and update memory delta
			memoryDelta -= min(end, areaEnd) - position
			mergeLeft = &dataArea{position: area.position, data: area.data[:position-area.position]}
		}
		if end < areaEnd {
			// Area is partially right of current, mark for merge right and update memory delta
			memoryDelta -= end - max(position, area.position)
			mergeRight = &dataArea{position: end, data: area.data[end-area.position:]}
		}
	}
	return
}

// func splitAreasForProcessing(areas dataAreas, position int, length int) (
// 	before dataAreas, mergeLeft, mergeRight *dataArea, after dataAreas, memoryDelta int) {
// 	end := position + length
// 	memoryDelta = length
// 	for i, area := range areas {
// 		areaEnd := area.end()
// 		// No overlap, area is completely before new data
// 		if areaEnd <= position {
// 			before = append(before, area)
// 			continue
// 		}
// 		// No overlap, area is completely after new data
// 		if area.position >= end {
// 			after = append(after, areas[i:]...)
// 			break
// 		}
// 		// Full overwrite: new data fully covers area
// 		if position <= area.position && end >= areaEnd {
// 			memoryDelta -= len(area.data)
// 			continue
// 		}
// 		// Partial left overlap: area overlaps left side of new data
// 		if area.position < position && areaEnd > position && areaEnd <= end {
// 			mergeLeft = &dataArea{position: area.position, data: area.data[:position-area.position]}
// 			memoryDelta -= areaEnd - position
// 			continue
// 		}
// 		// Partial right overlap: area overlaps right side of new data
// 		if area.position < end && area.position >= position && areaEnd > end {
// 			mergeRight = &dataArea{position: end, data: area.data[end-area.position:]}
// 			memoryDelta -= areaEnd - end
// 			continue
// 		}
// 		// New data is inside an existing area (should not happen for non-overlapping areas, but handle gracefully)
// 		if area.position < position && areaEnd > end {
// 			mergeLeft = &dataArea{position: area.position, data: area.data[:position-area.position]}
// 			mergeRight = &dataArea{position: end, data: area.data[end-area.position:]}
// 			memoryDelta -= len(area.data)
// 			continue
// 		}
// 	}
// 	return
// }
