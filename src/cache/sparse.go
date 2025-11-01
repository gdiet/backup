package cache

// sparse is a file cache layer that stores sparse parts of a cached file.
type sparse struct {
	// areas holds sparse (zero filled) areas in the file.
	areas areas
}

// read reads sparse (zero filled) data from the sparse entry into the provided buffer.
//
// Returns the areas that were not read.
func (s *sparse) read(off int64, data bytes) (unreadAreas areas) {
	end := off + int64(len(data))
	if off == end {
		return nil // Nothing to read
	}

	lastUnread := area{off: off, len: int64(len(data))}

	// For each sparse area, try to satisfy part of the read request
	for _, a := range s.areas {
		if a.off >= end {
			break // No further areas can satisfy the read
		}
		if a.end() <= off {
			continue // This area is before the requested read
		}

		// Determine overlapping range
		readStart := max(off, a.off)
		readEnd := min(end, a.end())

		// Write zeros to output buffer
		for i := readStart; i < readEnd; i++ {
			data[i-off] = 0
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

// shrink cuts off sparse areas beyond the new size.
func (s *sparse) shrink(newSize int64) {
	defer func() {
		validateAreasInvariants(s.areas)
	}()

	// Remove or truncate areas beyond new size
	for i, a := range s.areas {
		if a.off >= newSize {
			// Area starts beyond new size, skip it and remaining areas
			s.areas = s.areas[:i]
			break
		}
		if a.end() > newSize {
			// Area extends beyond new size, truncate it and skip remaining areas
			s.areas[i].len = newSize - a.off
			s.areas = s.areas[:i+1]
			break
		}
		// Otherwise, area is fully within new size, keep it
	}
}

// add adds a new sparse area. Assumes the area's offset is beyond the current sparse areas.
func (s *sparse) add(area area) {
	defer func() {
		validateAreasInvariants(s.areas)
	}()

	s.areas = append(s.areas, area)
}

// remove removes sparse areas overlapping with the specified area.
func (s *sparse) remove(off int64, length int64) {
	if length == 0 {
		return // Nothing to do
	}
	defer func() {
		validateAreasInvariants(s.areas)
	}()

	end := off + length
	newAreas := areas{}
	for index, currentArea := range s.areas {
		if currentArea.end() <= off {
			// Area is completely before the removed area
			newAreas = append(newAreas, currentArea)
		} else if currentArea.off >= end {
			// Area is completely after the removed area
			newAreas = append(newAreas, s.areas[index:]...)
			break
		} else {
			// Area overlaps with the removed area
			if currentArea.off < off {
				// There is a part left of the removed area
				newAreas = append(newAreas, area{off: currentArea.off, len: off - currentArea.off})
			}
			if currentArea.end() > end {
				// There is a part right of the removed area
				newAreas = append(newAreas, area{off: end, len: currentArea.end() - end})
			}
		}
	}
	s.areas = newAreas
}

// close clears the sparse entry.
func (s *sparse) close() {
	s.areas = nil
}
