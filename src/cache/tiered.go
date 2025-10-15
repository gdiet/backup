package cache

import "io"

// Tiered combines the sparse, memory, and disk cache layers.
type Tiered struct {
	sparse Sparse
	memory Memory
	disk   Disk
}

// Read reads data from the cache entry.
// Returns the total number of bytes read, which may be less than len(data) if EOF is reached.
func (tiered Tiered) Read(position int64, data Bytes) (int, error) {
	if len(data) == 0 {
		return 0, nil // Nothing to read
	}

	// Step 1: Read from sparse layer
	nonSparseAreas, totalRead := tiered.sparse.Read(position, data)

	// Step 2: Read from memory layer for non-sparse areas
	for _, nonSparseArea := range nonSparseAreas {
		nonSparseAreaData := data[nonSparseArea.Off-position : nonSparseArea.Off-position+nonSparseArea.Len]
		remainingAreas := tiered.memory.Read(nonSparseArea.Off, nonSparseAreaData)

		// Step 3: Read from disk layer for remaining unread areas
		for _, remainingArea := range remainingAreas {
			remainingAreaData := nonSparseAreaData[remainingArea.Off-nonSparseArea.Off : remainingArea.Off-nonSparseArea.Off+remainingArea.Len]
			if err := tiered.disk.Read(remainingArea.Off, remainingAreaData); err != nil {
				return totalRead, err
			}
		}
	}

	if totalRead < len(data) {
		return totalRead, io.EOF
	}
	return totalRead, nil
}

// Truncate changes the size of the cached file, adjusting all layers as needed.
func (tiered *Tiered) Truncate(newSize int64) error {
	if tiered.sparse.Truncate(newSize) {
		tiered.memory.Truncate(newSize)
		return tiered.disk.Truncate(newSize)
	}
	return nil
}

// Write writes data to the cache entry at the specified position.
func (tiered *Tiered) Write(position int64, data Bytes, maxMergeSize int64) error {
	if len(data) == 0 {
		return nil // Nothing to write
	}

	// Step 1: Write to sparse layer - this updates size and removes sparse areas
	tiered.sparse.Write(position, data)

	// Step 2: Check if enough memory is available (TODO: implement proper memory check)
	// For now, simulate 50% memory availability
	hasMemory := (position+data.Size())%2 == 0 // Simple deterministic 50/50 split

	if hasMemory {
		// Write to memory cache
		tiered.memory.Write(position, data, maxMergeSize)
	} else {
		// TODO: Maybe there is a data block at the position in memory - remove it
		// Write directly to disk (memory full)
		if err := tiered.disk.Write(position, data); err != nil {
			return err
		}
	}

	return nil
}
