package aaa

// sparse is a file cache layer that manages the sparse parts of a cached file and the actual file size.
type sparse struct {
	sparseAreas areas
}

// read reads data from the sparse entry, filling sparse data areas with zeros.
// Returns the Areas that were not read.
func (sparse *sparse) read(position int, data bytes) (unreadAreas areas) {
	end := position + len(data)
	if position == end {
		return areas{} // Nothing to read
	}

	lastUnread := area{off: position, len: len(data)}

	// For each sparse area, try to satisfy part of the read request
	for _, sparseArea := range sparse.sparseAreas {
		if sparseArea.off >= end {
			break // No further areas can satisfy the read
		}
		if sparseArea.end() <= position {
			continue // This area is before the requested read
		}

		// Determine overlapping range
		readStart := max(position, sparseArea.off)
		readEnd := min(end, sparseArea.end())

		// Fill sparse data area with zeros
		for i := readStart; i < readEnd; i++ {
			data[i-position] = 0
		}

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

// truncate adjusts sparse areas as needed.
func (sparse *sparse) truncate(newSize int) {
	defer func() {
		validateAreasInvariants(sparse.sparseAreas)
	}()

	// Remove or truncate sparse areas beyond new size
	for index, area := range sparse.sparseAreas {
		if area.off >= newSize {
			// Area is beyond new size, skip remaining areas
			sparse.sparseAreas = sparse.sparseAreas[:index]
			break
		}
		if area.off+area.len > newSize {
			// Area extends beyond new size, truncate it and skip remaining areas
			sparse.sparseAreas[index].len = newSize - area.off
			sparse.sparseAreas = sparse.sparseAreas[:index+1]
			break
		}
	}
}

// add adds a new sparse area. Assumes the area's position is beyond the current sparse areas.
func (sparse *sparse) add(area area) {
	defer func() {
		validateAreasInvariants(sparse.sparseAreas)
	}()

	sparse.sparseAreas = append(sparse.sparseAreas, area)
}

// remove removes sparse areas overlapping with the specified area.
func (sparse *sparse) remove(position int, len int) {
	if len == 0 {
		return // Nothing to do
	}
	defer func() {
		validateAreasInvariants(sparse.sparseAreas)
	}()

	end := position + len
	newAreas := areas{}
	for index, currentArea := range sparse.sparseAreas {
		if currentArea.end() <= position {
			// Area is completely before the removed area
			newAreas = append(newAreas, currentArea)
		} else if currentArea.off >= end {
			// Area is completely after the removed area
			newAreas = append(newAreas, sparse.sparseAreas[index:]...)
			break
		} else {
			// Area overlaps with the removed area
			if currentArea.off < position {
				// There is a part left of the removed area
				newAreas = append(newAreas, area{off: currentArea.off, len: position - currentArea.off})
			}
			if currentArea.end() > end {
				// There is a part right of the removed area
				newAreas = append(newAreas, area{off: end, len: currentArea.end() - end})
			}
		}
	}
	sparse.sparseAreas = newAreas
}

// close clears the sparse entry.
func (sparse *sparse) close() {
	sparse.sparseAreas = nil
}
