package cache

import "io"

// Cache is a file contents cache that combines the memory and disk cache layers.
type Cache struct {
	size   int64
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
func (c *Cache) Read(off int64, data bytes) (bytesRead int, err error) {
	len := len(data)
	if len <= 0 {
		return 0, nil // Nothing to read
	}
	if c.size <= off {
		return 0, io.EOF // Nothing to read due to EOF
	}
	// Calculate total read size to prevent reading internally past EOF
	bytesRead = int(min(int64(len), c.size-off))

	// Adjust target slice to available size
	data = data[:bytesRead]

	// Step 1: Read from sparse layer
	nonSparseAreas := c.sparse.read(off, data)

	// Step 2: Read from memory layer all non-sparse areas
	for _, nonSparseArea := range nonSparseAreas {
		nonSparseDataStart := nonSparseArea.off - off
		nonSparseAreaData := data[nonSparseDataStart : nonSparseDataStart+nonSparseArea.len]
		remainingAreas := c.memory.read(nonSparseArea.off, nonSparseAreaData)

		// Step 3: Read from disk layer any remaining unread areas
		for _, remainingArea := range remainingAreas {
			remainingDataStart := remainingArea.off - off
			remainingAreaData := data[remainingDataStart : remainingDataStart+remainingArea.len]
			// Invariant: disk.file is always open here
			toBeReadFromBase, err := c.disk.read(remainingArea.off, remainingAreaData)
			if err != nil {
				return bytesRead, err
			}

			// Step 4: Read from base layer any remaining areas not present in disk cache
			for _, baseArea := range toBeReadFromBase {
				baseDataStart := baseArea.off - off
				baseAreaData := data[baseDataStart : baseDataStart+baseArea.len]
				err := c.base.read(baseArea.off, baseAreaData)
				assert(err != io.EOF, "base read returned EOF unexpectedly")
				if err != nil && err != io.EOF {
					return bytesRead, err
				}
			}
		}
	}

	if bytesRead < len {
		return bytesRead, io.EOF
	}
	return bytesRead, nil
}

// Truncate changes the size of cache entry, adjusting all layers as needed.
// Returns the memory usage change (negative if memory was freed, positive if more memory was used).
func (c *Cache) Truncate(newSize int64) (memoryDelta int, err error) {
	if newSize == c.size {
		return 0, nil // No size change
	}
	if newSize > c.size {
		// New sparse area from old size to new size
		c.sparse.write(c.size, newSize-c.size)
		c.size = newSize
		return 0, nil // No memory change when growing
	}
	// Shrink the cache
	c.size = newSize
	c.sparse.shrink(newSize)
	memoryDelta = c.memory.shrink(newSize)
	err = c.disk.shrink(newSize)
	return memoryDelta, err
}

// Write writes data to the cache entry at the specified offset.
// Returns the memory usage change (negative if memory was freed, positive if more memory was used).
func (c *Cache) Write(off int64, data bytes, storeInMemory bool, maxMergeSize int) (memoryDelta int, err error) {
	if len(data) == 0 {
		return 0, nil // Nothing to write, no memory change
	}

	// Update sparse layer (removes sparse areas)
	c.sparse.remove(off, int64(len(data)))

	if storeInMemory {
		// Either write to memory cache ...
		memoryDelta = c.memory.write(off, data, maxMergeSize)
		c.disk.remove(off, int64(len(data)))
		return memoryDelta, nil
	} else {
		// ... or write to disk, clearing any overlapping memory areas
		memoryDelta = c.memory.remove(off, int64(len(data)))
		err := c.disk.write(off, data)
		return memoryDelta, err
	}
}

// Close clears all cached data from memory and closes and deletes the cache file.
// Returns the amount of memory freed (always negative or zero).
func (c *Cache) Close() (memoryDelta int, err error) {
	memoryDelta = c.memory.close()
	err = c.disk.close()
	return memoryDelta, err
}
