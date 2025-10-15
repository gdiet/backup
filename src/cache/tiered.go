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
// Returns the memory usage change (negative if memory was freed, positive if more memory was used).
func (tiered *Tiered) Truncate(newSize int64) (int, error) {
	memoryDelta := 0
	if tiered.sparse.Truncate(newSize) {
		memoryDelta = tiered.memory.Truncate(newSize)
		// Only truncate disk if file is available
		/*
			TODO Ich halte die nil-Abfrage für falsch: Wenn ich mir zum Beispiel nur TestTieredReadMemoryLayer
			ansehe, dann sehe ich dort, dass die Tiered Struct in einem Zustand ist, der nicht durch die
			geplanten Operationen Write und Truncate erreicht werden kann. Wir sollten nur Zustände testen,
			die durch die geplanten Operationen erreichbar sind.
		*/
		if tiered.disk.file != nil {
			err := tiered.disk.Truncate(newSize)
			return memoryDelta, err
		}
	}
	return memoryDelta, nil
}

// Write writes data to the cache entry at the specified position.
func (tiered *Tiered) Write(position int64, data Bytes, maxMergeSize int64) error {
	if len(data) == 0 {
		return nil // Nothing to write
	}

	// Step 1: Write to sparse layer - this updates size and removes sparse areas
	tiered.sparse.Write(position, data)

	// Step 2: Check if enough memory is available
	// For now, simulate 50% memory availability based on position and data size
	hasMemory := (position+data.Size())%2 == 0 // Simple deterministic 50/50 split

	if hasMemory {
		// Write to memory cache and track memory usage change
		memoryDelta := tiered.memory.Write(position, data, maxMergeSize)
		_ = memoryDelta // Memory usage tracked but not yet exposed to caller
	} else {
		// Memory full - check if there's existing data at position and remove it
		existingAreas := tiered.memory.Read(position, make(Bytes, data.Size()))
		if len(existingAreas) == 0 {
			// Data exists at this position in memory, remove it by truncating and rewriting
			memoryDelta := tiered.memory.Truncate(position)
			_ = memoryDelta // Memory freed from removal
		}

		// Write directly to disk
		if err := tiered.disk.Write(position, data); err != nil {
			return err
		}
	}

	return nil
}

// Close clears all cached data from memory and closes disk file.
// Returns the amount of memory freed (always negative or zero).
func (tiered *Tiered) Close() (int, error) {
	memoryFreed := tiered.memory.Close()
	err := tiered.disk.Close()
	return memoryFreed, err
}
