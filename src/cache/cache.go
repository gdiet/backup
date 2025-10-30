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

func NewCache(cacheFilePath string, baseFile baseFile) Cache {
	return Cache{
		size:   baseFile.length(),
		sparse: sparse{},
		memory: memory{},
		disk:   disk{filePath: cacheFilePath},
		base:   baseFile,
	}
}

// Read reads data from the cache entry.
// Returns the total number of bytes read, which may be less than len(data) if EOF is reached.
func (cache Cache) Read(off int, data bytes) (bytesRead int, err error) {
	if cache.size < off+len(data) {
		return 0, io.EOF // Nothing to read due to EOF
	}
	if len(data) <= 0 {
		return 0, nil // Nothing to read
	}
	// Calculate total read size to prevent reading past EOF
	bytesRead = min(len(data), cache.size-off)

	// Adjust data to available size
	data = data[:bytesRead]

	// Step 1: Read from sparse layer
	nonSparseAreas := cache.sparse.read(off, data)

	// Step 2: Read from memory layer all non-sparse areas
	for _, nonSparseArea := range nonSparseAreas {
		nonSparseDataStart := nonSparseArea.off - off
		nonSparseAreaData := data[nonSparseDataStart : nonSparseDataStart+nonSparseArea.len]
		remainingAreas := cache.memory.read(nonSparseArea.off, nonSparseAreaData)

		// Step 3: Read from disk layer any remaining unread areas
		for _, remainingArea := range remainingAreas {
			remainingDataStart := remainingArea.off - nonSparseArea.off
			remainingAreaData := nonSparseAreaData[remainingDataStart : remainingDataStart+remainingArea.len]
			// Invariant: disk.file is always open here
			toBeReadFromBase, err := cache.disk.read(remainingArea.off, remainingAreaData)
			if err != nil {
				return bytesRead, err
			}

			// Step 4: Read from base layer any remaining areas not present in disk cache
			for _, baseArea := range toBeReadFromBase {
				baseDataStart := baseArea.off - remainingArea.off
				baseAreaData := remainingAreaData[baseDataStart : baseDataStart+baseArea.len]
				err := cache.base.read(baseArea.off, baseAreaData)
				assert(err != io.EOF, "base read returned EOF unexpectedly")
				if err != nil && err != io.EOF {
					return bytesRead, err
				}
			}
		}
	}

	if bytesRead < len(data) {
		return bytesRead, io.EOF
	}
	return bytesRead, nil
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

// Write writes data to the cache entry at the specified offset.
// Returns the memory usage change (negative if memory was freed, positive if more memory was used).
func (cache *Cache) Write(off int, data bytes, storeInMemory bool, maxMergeSize int) (memoryDelta int, err error) {
	if len(data) == 0 {
		return 0, nil // Nothing to write, no memory change
	}

	// Update sparse layer (removes sparse areas)
	cache.sparse.remove(off, len(data))

	if storeInMemory {
		// Either write to memory cache ...
		memoryDelta = cache.memory.write(off, data, maxMergeSize)
		cache.disk.remove(off, len(data))
		return memoryDelta, nil
	} else {
		// ... or write to disk, clearing any overlapping memory areas
		memoryDelta = cache.memory.remove(area{off: off, len: len(data)})
		err := cache.disk.write(off, data)
		return memoryDelta, err
	}
}

// Close clears all cached data from memory and closes and deletes the cache file.
// Returns the amount of memory freed (always negative or zero).
func (cache *Cache) Close() (memoryDelta int, err error) {
	memoryDelta = cache.memory.close()
	err = cache.disk.close()
	return memoryDelta, err
}
