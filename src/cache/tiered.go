package cache

// Tiered combines the sparse, memory, and disk cache layers.
type Tiered struct {
	sparse Sparse
	memory Memory
	disk   Disk
}

// Read reads data from the cache entry.
// Returns the total number of bytes read, which may be less than len(data) if EOF is reached.
func (tiered Tiered) Read(position int64, data Bytes) int {
	// Step 1: Read from sparse layer
	nonSparseAreas, totalRead := tiered.sparse.Read(position, data)
	if totalRead == 0 {
		return totalRead // Nothing to read (EOF)
	}

	// Step 2: Read from memory layer for non-sparse areas
	for _, nonSparseArea := range nonSparseAreas {
		nonSparseAreaData := data[nonSparseArea.Off-position : nonSparseArea.Off-position+nonSparseArea.Len]
		remainingAreas := tiered.memory.Read(nonSparseArea.Off, nonSparseAreaData)

		// Step 3: Read from disk layer for remaining unread areas
		for _, remainingArea := range remainingAreas {
			remainingAreaData := nonSparseAreaData[remainingArea.Off-nonSparseArea.Off : remainingArea.Off-nonSparseArea.Off+remainingArea.Len]
			tiered.disk.Read(remainingArea.Off, remainingAreaData)
		}
	}

	return totalRead
}
