package cache

// Sparse is a file cache layer that manages the sparse parts of a cached file and the actual file size.
type Sparse struct {
	size        int64
	sparseAreas DataAreas
}

// Size returns the current size of the file.
func (sparse *Sparse) Size() int64 {
	return sparse.size
}

// Read reads data from the sparse entry, filling sparse data areas with zeros.
// Returns the DataAreas that were not read and the number of bytes total
// for the read operation, which may be less than len(data) if EOF is reached.
func (sparse *Sparse) Read(position int64, data Bytes) (DataAreas, int) {
	// Calculate total read size, considering EOF
	totalReadSize := max(0, min(data.Size(), sparse.size-position))
	if totalReadSize == 0 {
		return DataAreas{}, 0 // Nothing to read
	}
	// Adjust data to available size
	data = data[:totalReadSize]

	// Initialize non-sparse areas with the full requested area
	var nonSparseAreas DataAreas
	nonSparseAreas = append(nonSparseAreas, DataArea{Off: position, Len: totalReadSize})

	// Process sparse areas
	for _, sparseArea := range sparse.sparseAreas {
		// Calculate overlap
		overlapStart := max(position, sparseArea.Off)
		overlapEnd := min(position+totalReadSize, sparseArea.Off+sparseArea.Len)
		overlapSize := overlapEnd - overlapStart
		if overlapStart >= overlapEnd {
			continue // No overlap
		}
		// Fill overlap data area with zeros
		for i := overlapStart; i < overlapEnd; i++ {
			data[i-position] = 0
		}
		// Adjust non-sparse areas
		nonSparseAreas = nonSparseAreas.RemoveOverlappingAreas(DataArea{Off: overlapStart, Len: overlapSize})
	}

	return nonSparseAreas, int(totalReadSize)
}

// Truncate changes the size of the sparse file, adjusting sparse areas as needed.
// Returns true if following cache stages might need truncation as well.
func (sparse *Sparse) Truncate(newSize int64) bool {
	if newSize == sparse.size {
		return false // No change
	}
	// File grows, add new sparse area at the end
	if newSize > sparse.size {
		sparse.sparseAreas = append(sparse.sparseAreas, DataArea{Off: sparse.size, Len: newSize - sparse.size})
		sparse.size = newSize
		return false // No truncation needed
	}
	// File shrinks, remove or truncate sparse areas beyond new size
	var filteredAreas DataAreas
	for _, area := range sparse.sparseAreas {
		if area.Off >= newSize {
			continue // Area starts beyond new size, remove it
		}
		if area.Off+area.Len > newSize {
			area.Len = newSize - area.Off // Area extends beyond new size, truncate it
		}
		filteredAreas = append(filteredAreas, area)
	}
	sparse.size = newSize
	sparse.sparseAreas = filteredAreas
	return true // Truncation occurred
}

// Write updates the sparse entry.
func (sparse *Sparse) Write(position int64, data Bytes) {
	if data.Size() == 0 {
		return // Nothing to write
	}
	endPosition := position + data.Size()
	// Update size if writing beyond current size
	if endPosition > sparse.size {
		sparse.size = endPosition
	}
	// Remove overlapping sparse areas
	sparse.sparseAreas = sparse.sparseAreas.RemoveOverlappingAreas(DataArea{Off: position, Len: data.Size()})
}
