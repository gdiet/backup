package aaa

// sparse is a file cache layer that manages the sparse parts of a cached file and the actual file size.
type sparse struct {
	size        int
	sparseAreas areas
}

// Size returns the current size of the file.
func (sparse *sparse) Size() int {
	return sparse.size
}

// Read reads data from the sparse entry, filling sparse data areas with zeros.
// Returns the Areas that were not read and the number of bytes total
// for the read operation, which may be less than len(data) if EOF is reached.
func (sparse *sparse) read(position int, data bytes) (areas, int) {
	// // Calculate total read size, considering EOF
	// totalReadSize := max(0, min(len(data), sparse.size-position))
	// if totalReadSize == 0 { // TODO check whether this condition is needed or helpful
	// 	return areas{}, 0 // Nothing to read
	// }
	// // Adjust data to available size
	// data = data[:totalReadSize]

	// // Initialize non-sparse areas with the full requested area
	// nonSparseAreas := areas{{off: position, len: totalReadSize}}

	// // Process sparse areas
	// for _, sparseArea := range sparse.sparseAreas {
	// 	// Calculate overlap
	// 	overlapStart := max(position, sparseArea.off)
	// 	overlapEnd := min(position+totalReadSize, sparseArea.off+sparseArea.len)
	// 	overlapSize := overlapEnd - overlapStart
	// 	if overlapStart >= overlapEnd {
	// 		continue // No overlap
	// 	}
	// 	// Fill overlap data area with zeros
	// 	for i := overlapStart; i < overlapEnd; i++ {
	// 		data[i-position] = 0
	// 	}
	// 	// Adjust non-sparse areas
	// 	// nonSparseAreas = nonSparseAreas.RemoveOverlappingAreas(area{off: overlapStart, len: overlapSize})
	// }
	// return nonSparseAreas, int(totalReadSize)
	panic("not implemented")
}

// Truncate changes the size of the sparse file, adjusting sparse areas as needed.
// Returns true if following cache stages might need truncation as well.
func (sparse *sparse) truncate(newSize int) bool {
	// if newSize == sparse.size {
	// 	return false // No change
	// }
	// // File grows, add new sparse area at the end
	// if newSize > sparse.size {
	// 	sparse.sparseAreas = append(sparse.sparseAreas, Area{Off: sparse.size, Len: newSize - sparse.size})
	// 	sparse.size = newSize
	// 	return false // No truncation needed
	// }
	// // File shrinks, remove or truncate sparse areas beyond new size
	// var filteredAreas Areas
	// for _, area := range sparse.sparseAreas {
	// 	if area.Off >= newSize {
	// 		continue // Area starts beyond new size, remove it
	// 	}
	// 	if area.Off+area.Len > newSize {
	// 		area.Len = newSize - area.Off // Area extends beyond new size, truncate it
	// 	}
	// 	filteredAreas = append(filteredAreas, area)
	// }
	// sparse.size = newSize
	// sparse.sparseAreas = filteredAreas
	// return true // Truncation occurred
	panic("not implemented")
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
	sparse.size = 0
	sparse.sparseAreas = nil
}
