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
		nonSparseDataStart := nonSparseArea.Off - position
		nonSparseAreaData := data[nonSparseDataStart : nonSparseDataStart+nonSparseArea.Len]
		remainingAreas := tiered.memory.Read(nonSparseArea.Off, nonSparseAreaData)

		// Step 3: Read from disk layer for remaining unread areas
		for _, remainingArea := range remainingAreas {
			remainingDataStart := remainingArea.Off - nonSparseArea.Off
			remainingAreaData := nonSparseAreaData[remainingDataStart : remainingDataStart+remainingArea.Len]
			// Invariant: disk.file is always open here
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
// Returns the memory usage change (negative if memory was freed, positive if more memory was used).
func (tiered *Tiered) Truncate(newSize int64) (int, error) {
	if tiered.sparse.Truncate(newSize) {
		memoryDelta := tiered.memory.Truncate(newSize)
		err := tiered.disk.Truncate(newSize)
		return memoryDelta, err
	}
	return 0, nil
}

// Write writes data to the cache entry at the specified position.
// Returns the memory usage change (negative if memory was freed, positive if more memory was used).
func (tiered *Tiered) Write(position int64, data Bytes, storeInMemory bool, maxMergeSize int64) (int, error) {
	if len(data) == 0 {
		return 0, nil // Nothing to write, no memory change
	}

	// Update sparse layer (updates size and removes sparse areas)
	tiered.sparse.Write(position, data)

	if storeInMemory {
		// Either write to memory cache
		memoryDelta := tiered.memory.Write(position, data, maxMergeSize)
		return memoryDelta, nil
	} else {
		// Or write to disk, clearing any overlapping memory areas
		memoryDelta := tiered.memory.Clear(Area{Off: position, Len: data.Size()})
		err := tiered.disk.Write(position, data)
		return memoryDelta, err
	}
}

// Close clears all cached data from memory and closes disk file.
// Returns the amount of memory freed (always negative or zero).
func (tiered *Tiered) Close() (int, error) {
	memoryFreed := tiered.memory.Close()
	err := tiered.disk.Close()
	return memoryFreed, err
}
