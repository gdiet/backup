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
		deltaApplied := false
		if position > area.position {
			// Area is partially left of current, mark for merge left and update memory delta
			memoryDelta -= min(end, areaEnd) - position
			deltaApplied = true
			mergeLeft = &dataArea{position: area.position, data: area.data[:position-area.position]}
		}
		if end < areaEnd {
			// Area is partially right of current, mark for merge right and update memory delta
			if !deltaApplied {
				memoryDelta -= end - max(position, area.position)
			}
			mergeRight = &dataArea{position: end, data: area.data[end-area.position:]}
		}
	}
	return
}
