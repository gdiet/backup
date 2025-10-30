package cache

import "io"

// Cache is a file contents cache that combines the memory and disk cache layers.
type Cache struct {
	size   int
	sparse sparse
	memory memory
	disk   disk
	base   baseFile
}

func NewCache(cacheFile string, baseFile baseFile) Cache {
	return Cache{
		size:   baseFile.length(),
		sparse: sparse{},
		memory: memory{},
		disk:   disk{filePath: cacheFile},
		base:   baseFile,
	}
}

// Read reads data from the cache entry.
// Returns the total number of bytes read, which may be less than len(data) if EOF is reached.
func (cache Cache) Read(position int, data bytes) (int, error) {
	if cache.size < position+len(data) {
		return 0, io.EOF // Nothing to read due to EOF
	}
	if len(data) <= 0 {
		return 0, nil // Nothing to read
	}
	// Calculate total read size to prevent reading past EOF
	totalReadSize := min(len(data), cache.size-position)

	// Adjust data to available size
	data = data[:totalReadSize]

	// Step 1: Read from sparse layer
	nonSparseAreas := cache.sparse.read(position, data)

	// Step 2: Read from memory layer all non-sparse areas
	for _, nonSparseArea := range nonSparseAreas {
		nonSparseDataStart := nonSparseArea.off - position
		nonSparseAreaData := data[nonSparseDataStart : nonSparseDataStart+nonSparseArea.len]
		remainingAreas := cache.memory.read(nonSparseArea.off, nonSparseAreaData)

		// Step 3: Read from disk layer any remaining unread areas
		for _, remainingArea := range remainingAreas {
			remainingDataStart := remainingArea.off - nonSparseArea.off
			remainingAreaData := nonSparseAreaData[remainingDataStart : remainingDataStart+remainingArea.len]
			// Invariant: disk.file is always open here
			toBeReadFromBase, err := cache.disk.read(remainingArea.off, remainingAreaData)
			if err != nil {
				return totalReadSize, err
			}

			// Step 4: Read from base layer any remaining areas not present in disk cache
			for _, baseArea := range toBeReadFromBase {
				baseDataStart := baseArea.off - remainingArea.off
				baseAreaData := remainingAreaData[baseDataStart : baseDataStart+baseArea.len]
				err := cache.base.read(baseArea.off, baseAreaData)
				assert(err != io.EOF, "base read returned EOF unexpectedly")
				if err != nil && err != io.EOF {
					return totalReadSize, err
				}
			}
		}
	}

	if totalReadSize < len(data) {
		return totalReadSize, io.EOF
	}
	return totalReadSize, nil
}

// Truncate changes the size of cache entry, adjusting all layers as needed.
// Returns the memory usage change (negative if memory was freed, positive if more memory was used).
func (cache *Cache) Truncate(newSize int) (memoryDelta int, err error) {
	if newSize == cache.size {
		return 0, nil // No size change
	}
	if newSize > cache.size {
		// New sparse area from old size to new size
		cache.sparse.add(area{off: cache.size, len: newSize - cache.size})
		cache.size = newSize
		return 0, nil // No memory change when growing
	}
	// Shrink the cache
	cache.size = newSize
	cache.sparse.truncate(newSize)
	memoryDelta = cache.memory.truncate(newSize)
	err = cache.disk.truncate(newSize)
	return memoryDelta, err
}

// Write writes data to the cache entry at the specified position.
// Returns the memory usage change (negative if memory was freed, positive if more memory was used).
func (cache *Cache) Write(position int, data bytes, storeInMemory bool, maxMergeSize int) (int, error) {
	if len(data) == 0 {
		return 0, nil // Nothing to write, no memory change
	}

	// Update sparse layer (removes sparse areas)
	cache.sparse.remove(position, len(data))

	if storeInMemory {
		// Either write to memory cache ...
		memoryDelta := cache.memory.write(position, data, maxMergeSize)
		return memoryDelta, nil
	} else {
		// ... or write to disk, clearing any overlapping memory areas
		memoryDelta := cache.memory.clear(area{off: position, len: len(data)})
		err := cache.disk.write(position, data)
		return memoryDelta, err
	}
}

// Close clears all cached data from memory and closes and deletes the cache file.
// Returns the amount of memory freed (always negative or zero).
func (cache *Cache) Close() (int, error) {
	memoryFreed := cache.memory.close()
	err := cache.disk.close()
	return memoryFreed, err
}
