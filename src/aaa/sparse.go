package aaa

// sparse is a file cache layer that manages the sparse parts of a cached file and the actual file size.
type sparse struct {
	sparseAreas areas
}

// Read reads data from the sparse entry, filling sparse data areas with zeros.
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

// Truncate adjusts sparse areas as needed.
func (sparse *sparse) truncate(newSize int) {
	// TODO add invariants check
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

// Add adds a new sparse area. Assumes the area's position is beyond the current sparse areas.
func (sparse *sparse) add(area area) {
	// TODO add invariants check
	sparse.sparseAreas = append(sparse.sparseAreas, area)
}

// Write updates the sparse entry.
func (sparse *sparse) write(position int, data bytes) {
	// if data.Size() == 0 {
	// 	return // Nothing to write
	// }
	// endPosition := position + data.Size()
	// // Update size if writing beyond current size
	// if endPosition > sparse.size {
	// 	sparse.size = endPosition
	// }
	// // Remove overlapping sparse areas
	// sparse.sparseAreas = sparse.sparseAreas.RemoveOverlappingAreas(Area{Off: position, Len: data.Size()})
	panic("not implemented")
}

// close clears the sparse entry.
func (sparse *sparse) close() {
	sparse.sparseAreas = nil
}
