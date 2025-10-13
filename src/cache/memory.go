package cache

// import (
// 	"fmt"
// 	"sync"
// 	"time"
// )

// // MemoryConfig configures the adaptive memory cache behavior
// type MemoryConfig struct {
// 	MinSize         uint64        // Minimum cache size in bytes (can be 0)
// 	MaxSize         int64         // Maximum cache size: <0=use MaxPercent, 0=no cache, >0=hard limit
// 	MaxPercent      float64       // Maximum percentage of available memory (used if MaxSize < 0)
// 	UpdateInterval  time.Duration // How often to check memory limits
// 	ShrinkThreshold float64       // When to start evicting (as fraction of current limit)
// 	GrowThreshold   float64       // When cache can grow again (as fraction of current limit)
// }

// // DefaultMemoryConfig returns a sensible default configuration
// func DefaultMemoryConfig() MemoryConfig {
// 	return MemoryConfig{
// 		MinSize:         50 * 1024 * 1024, // 50MB minimum
// 		MaxSize:         -1,               // Use percentage-based limit
// 		MaxPercent:      0.8,              // 80% of available memory
// 		UpdateInterval:  time.Second,      // Check every second
// 		ShrinkThreshold: 0.9,              // Start evicting at 90% of limit
// 		GrowThreshold:   0.7,              // Can grow again at 70% of limit
// 	}
// }

// // memoryFile represents a file stored in memory
// type memoryFile struct {
// 	data     []byte
// 	lastUsed time.Time
// }

// // Memory is a memory-based file cache layer, to be used in between Sparse and FileCache.
// //
// // Thread Safety:
// // Uses similar locking strategy as FileCache:
// // 1. globalLock (RWMutex) protects files map and configuration
// // 2. per-file locks (RWMutex) allow concurrent reads, exclusive writes
// type Memory struct {
// 	files      map[int]*memoryFile
// 	fileLocks  map[int]*sync.RWMutex
// 	globalLock sync.RWMutex

// 	// Configuration and adaptive behavior
// 	config       MemoryConfig
// 	currentLimit uint64
// 	currentSize  uint64
// 	lastUpdate   time.Time

// 	// Background monitoring
// 	stopMonitor chan bool
// 	monitoring  bool
// }

// // NewMemory creates a new memory-based cache with the given configuration
// func NewMemory(config MemoryConfig) (*Memory, error) {
// 	mc := &Memory{
// 		files:       make(map[int]*memoryFile),
// 		fileLocks:   make(map[int]*sync.RWMutex),
// 		config:      config,
// 		stopMonitor: make(chan bool, 1),
// 	}

// 	// Check for disabled cache
// 	if config.MaxSize == 0 {
// 		// No cache - set everything to 0
// 		mc.config.MinSize = 0
// 		mc.currentLimit = 0
// 		return mc, nil
// 	}

// 	// Set initial limit
// 	mc.updateMemoryLimit()

// 	// Start background monitoring if using adaptive limits
// 	if config.MaxSize < 0 {
// 		mc.startMonitoring()
// 	}

// 	return mc, nil
// }

// // NewMemCacheDefault creates a new memory cache with default configuration
// func NewMemCacheDefault() (*MemCache, error) {
// 	return NewMemCache(DefaultMemCacheConfig())
// }

// // getFile returns a memory file and its lock for the given fileId if it exists.
// // Returns error if file doesn't exist.
// func (mc *MemCache) getFile(fileId int) (*memFile, *sync.RWMutex, error) {
// 	mc.globalLock.RLock()
// 	defer mc.globalLock.RUnlock()

// 	if file, exists := mc.files[fileId]; exists {
// 		fileLock := mc.fileLocks[fileId] // Invariant: lock always exists when file exists
// 		file.lastUsed = time.Now()
// 		return file, fileLock, nil
// 	}

// 	return nil, nil, fmt.Errorf("file %d does not exist", fileId)
// }

// // getOrCreateFile returns a memory file and its lock for the given fileId.
// // Creates the file if it doesn't exist. Uses double-checked locking pattern.
// func (mc *MemCache) getOrCreateFile(fileId int) (*memFile, *sync.RWMutex, error) {
// 	// First check: read lock only (fast path for existing files)
// 	mc.globalLock.RLock()
// 	if file, exists := mc.files[fileId]; exists {
// 		fileLock := mc.fileLocks[fileId] // Invariant: lock always exists when file exists
// 		file.lastUsed = time.Now()
// 		mc.globalLock.RUnlock()
// 		return file, fileLock, nil
// 	}
// 	mc.globalLock.RUnlock()

// 	// File doesn't exist, we need to create it
// 	// Create WITHOUT holding the global lock
// 	newFile := &memFile{
// 		data:     make([]byte, 0),
// 		lastUsed: time.Now(),
// 	}

// 	// Second check: acquire write lock and check again
// 	mc.globalLock.Lock()
// 	defer mc.globalLock.Unlock()

// 	// Double-check: another goroutine might have created the same file
// 	if existingFile, exists := mc.files[fileId]; exists {
// 		fileLock := mc.fileLocks[fileId] // Invariant: lock always exists when file exists
// 		existingFile.lastUsed = time.Now()
// 		return existingFile, fileLock, nil
// 	}

// 	// We're the first to create this file, register it
// 	fileLock := &sync.RWMutex{}
// 	mc.fileLocks[fileId] = fileLock
// 	mc.files[fileId] = newFile
// 	return newFile, fileLock, nil
// }

// // getLockedFile returns a memory file with appropriate lock already acquired.
// // The returned unlock function MUST be called to release the lock.
// func (mc *MemCache) getLockedFile(fileId int, forWriting bool) (*memFile, func(), error) {
// 	// Get or create the file and its lock atomically
// 	file, fileLock, err := mc.getOrCreateFile(fileId)
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	// Acquire the appropriate lock and return unlock function
// 	if forWriting {
// 		fileLock.Lock()
// 		return file, func() { fileLock.Unlock() }, nil
// 	} else {
// 		fileLock.RLock()
// 		return file, func() { fileLock.RUnlock() }, nil
// 	}
// }

// // Truncate sets the file to the specified length
// func (mc *MemCache) Truncate(fileId int, length int64) error {
// 	if length < 0 {
// 		return fmt.Errorf("length cannot be negative: %d", length)
// 	}

// 	if mc.config.MaxSize == 0 {
// 		return fmt.Errorf("cache is disabled (MaxSize = 0)")
// 	}

// 	// Get locked file handle (exclusive lock for truncating)
// 	file, unlock, err := mc.getLockedFile(fileId, true)
// 	if err != nil {
// 		return err
// 	}
// 	defer unlock()

// 	// Check if we need to make room for expansion
// 	oldSize := uint64(len(file.data))
// 	newSize := uint64(length)

// 	if newSize > oldSize {
// 		if !mc.canAllocate(newSize - oldSize) {
// 			return fmt.Errorf("insufficient memory to truncate file %d to length %d", fileId, length)
// 		}
// 	}

// 	// Truncate or extend the data
// 	if length == 0 {
// 		file.data = make([]byte, 0)
// 	} else if int64(len(file.data)) < length {
// 		// Extend with zeros
// 		newData := make([]byte, length)
// 		copy(newData, file.data)
// 		file.data = newData
// 	} else {
// 		// Truncate
// 		file.data = file.data[:length]
// 	}

// 	// Update size tracking
// 	mc.globalLock.Lock()
// 	mc.currentSize = mc.currentSize - oldSize + uint64(len(file.data))
// 	mc.globalLock.Unlock()

// 	file.lastUsed = time.Now()
// 	return nil
// }

// // Read reads data from the cached file at the specified position
// func (mc *MemCache) Read(fileId int, position int64, length int) ([]byte, error) {
// 	if position < 0 {
// 		return nil, fmt.Errorf("position cannot be negative: %d", position)
// 	}
// 	if length < 0 {
// 		return nil, fmt.Errorf("length cannot be negative: %d", length)
// 	}
// 	if length == 0 {
// 		return []byte{}, nil
// 	}

// 	// Get locked file handle (shared lock for reading)
// 	file, unlock, err := mc.getLockedFile(fileId, false)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer unlock()

// 	// Check bounds
// 	fileSize := int64(len(file.data))
// 	if position >= fileSize {
// 		return []byte{}, nil // Reading beyond end of file returns empty
// 	}

// 	// Calculate actual read length
// 	available := fileSize - position
// 	if int64(length) > available {
// 		length = int(available)
// 	}

// 	// Read data
// 	data := make([]byte, length)
// 	copy(data, file.data[position:position+int64(length)])

// 	file.lastUsed = time.Now()
// 	return data, nil
// }

// // Write writes data to the cached file at the specified position
// func (mc *MemCache) Write(fileId int, position int64, data []byte) error {
// 	if position < 0 {
// 		return fmt.Errorf("position cannot be negative: %d", position)
// 	}
// 	if len(data) == 0 {
// 		return nil // Nothing to write
// 	}

// 	if mc.config.MaxSize == 0 {
// 		return fmt.Errorf("cache is disabled (MaxSize = 0)")
// 	}

// 	// Get locked file handle (exclusive lock for writing)
// 	file, unlock, err := mc.getLockedFile(fileId, true)
// 	if err != nil {
// 		return err
// 	}
// 	defer unlock()

// 	// Calculate required size
// 	endPos := position + int64(len(data))
// 	oldSize := uint64(len(file.data))
// 	newSize := uint64(endPos)

// 	if newSize > oldSize {
// 		if !mc.canAllocate(newSize - oldSize) {
// 			return fmt.Errorf("insufficient memory to write to file %d at position %d", fileId, position)
// 		}
// 	}

// 	// Extend file if necessary
// 	if endPos > int64(len(file.data)) {
// 		newData := make([]byte, endPos)
// 		copy(newData, file.data)
// 		file.data = newData
// 	}

// 	// Write data
// 	copy(file.data[position:], data)

// 	// Update size tracking
// 	mc.globalLock.Lock()
// 	mc.currentSize = mc.currentSize - oldSize + uint64(len(file.data))
// 	mc.globalLock.Unlock()

// 	file.lastUsed = time.Now()
// 	return nil
// }

// // Length returns the current length of the cached file
// func (mc *MemCache) Length(fileId int) (int64, error) {
// 	// Get file only if it exists (don't create)
// 	file, fileLock, err := mc.getFile(fileId)
// 	if err != nil {
// 		return 0, err
// 	}

// 	// Acquire shared lock for reading
// 	fileLock.RLock()
// 	defer fileLock.RUnlock()

// 	file.lastUsed = time.Now()
// 	return int64(len(file.data)), nil
// }

// // Flush is a no-op for memory cache (data is always "flushed" to memory)
// func (mc *MemCache) Flush(fileId int) error {
// 	// For memory cache, flush is essentially a no-op since data is already in memory
// 	// We just check if the file exists
// 	file, unlock, err := mc.getLockedFile(fileId, false)
// 	if err != nil {
// 		return fmt.Errorf("file %d does not exist: %v", fileId, err)
// 	}
// 	defer unlock()

// 	file.lastUsed = time.Now()
// 	return nil
// }

// // Dispose removes the cached file and cleans up resources
// func (mc *MemCache) Dispose(fileId int) error {
// 	// Acquire global lock first to atomically check and mark for disposal
// 	mc.globalLock.Lock()

// 	// Check if file exists
// 	file, exists := mc.files[fileId]
// 	fileLock, lockExists := mc.fileLocks[fileId]

// 	if !exists || !lockExists {
// 		// File already disposed or never existed
// 		mc.globalLock.Unlock()
// 		return nil
// 	}

// 	// File exists - remove it from maps first to prevent other dispose attempts
// 	delete(mc.files, fileId)
// 	delete(mc.fileLocks, fileId)

// 	// Update size tracking
// 	mc.currentSize -= uint64(len(file.data))

// 	// Now we can release global lock - we're the only one disposing this file
// 	mc.globalLock.Unlock()

// 	// Wait for all ongoing operations to complete
// 	fileLock.Lock()
// 	defer fileLock.Unlock()

// 	// Clear the data (help GC)
// 	file.data = nil

// 	return nil
// }

// // Close closes the cache and cleans up all resources
// func (mc *MemCache) Close() error {
// 	// Stop monitoring first
// 	if mc.monitoring {
// 		select {
// 		case mc.stopMonitor <- true:
// 		default:
// 		}
// 		mc.monitoring = false
// 	}

// 	mc.globalLock.Lock()
// 	defer mc.globalLock.Unlock()

// 	// Close all files with proper locking
// 	for fileId, file := range mc.files {
// 		fileLock := mc.fileLocks[fileId]

// 		// Acquire exclusive lock to ensure no ongoing operations
// 		fileLock.Lock()
// 		file.data = nil // Help GC
// 		fileLock.Unlock()
// 	}

// 	// Clear all maps
// 	mc.files = make(map[int]*memFile)
// 	mc.fileLocks = make(map[int]*sync.RWMutex)
// 	mc.currentSize = 0

// 	return nil
// }

// // GetStats returns statistics about the cache
// func (mc *MemCache) GetStats() map[string]interface{} {
// 	mc.globalLock.RLock()
// 	defer mc.globalLock.RUnlock()

// 	utilizationPercent := float64(0)
// 	if mc.currentLimit > 0 {
// 		utilizationPercent = float64(mc.currentSize) / float64(mc.currentLimit) * 100
// 	}

// 	return map[string]interface{}{
// 		"numberOfFiles":    len(mc.files),
// 		"currentSize":      mc.currentSize,
// 		"currentLimit":     mc.currentLimit,
// 		"utilizationPct":   utilizationPercent,
// 		"openFilesList":    mc.getOpenFilesList(),
// 		"configMinSize":    mc.config.MinSize,
// 		"configMaxSize":    mc.config.MaxSize,
// 		"configMaxPercent": mc.config.MaxPercent,
// 	}
// }

// // getOpenFilesList returns a list of currently cached file IDs
// func (mc *MemCache) getOpenFilesList() []int {
// 	var fileIds []int
// 	for fileId := range mc.files {
// 		fileIds = append(fileIds, fileId)
// 	}
// 	return fileIds
// }

// // canAllocate checks if we can allocate the requested amount of memory
// func (mc *MemCache) canAllocate(bytes uint64) bool {
// 	mc.globalLock.RLock()
// 	defer mc.globalLock.RUnlock()

// 	newSize := mc.currentSize + bytes

// 	// Check if we would exceed current limit
// 	if newSize > mc.currentLimit {
// 		// Try to free up space by evicting old files
// 		return mc.tryEvictMemory(bytes)
// 	}

// 	return true
// }

// // tryEvictMemory tries to evict enough memory to satisfy the allocation request
// func (mc *MemCache) tryEvictMemory(bytesNeeded uint64) bool {
// 	// This is called with globalLock.RLock() held, so we can't modify the maps directly
// 	// For now, just return false - in a real implementation, you'd implement LRU eviction
// 	// This would require a more sophisticated data structure to track access order
// 	return false
// }

// // updateMemoryLimit updates the current memory limit based on configuration
// func (mc *MemCache) updateMemoryLimit() {
// 	if mc.config.MaxSize > 0 {
// 		// Hard limit takes precedence
// 		mc.currentLimit = uint64(mc.config.MaxSize)
// 	} else if mc.config.MaxSize < 0 {
// 		// Use percentage-based limit (simplified - in reality you'd use system memory info)
// 		// For now, use a default of 1GB as placeholder
// 		defaultLimit := uint64(1024 * 1024 * 1024) // 1GB
// 		mc.currentLimit = uint64(float64(defaultLimit) * mc.config.MaxPercent)
// 	} else {
// 		// MaxSize == 0: No cache
// 		mc.currentLimit = 0
// 		return
// 	}

// 	// Respect minimum size (but only if cache is enabled)
// 	if mc.config.MaxSize != 0 && mc.currentLimit < mc.config.MinSize {
// 		mc.currentLimit = mc.config.MinSize
// 	}
// }

// // startMonitoring starts the background memory monitoring goroutine
// func (mc *MemCache) startMonitoring() {
// 	if mc.monitoring {
// 		return
// 	}

// 	mc.monitoring = true
// 	go func() {
// 		ticker := time.NewTicker(mc.config.UpdateInterval)
// 		defer ticker.Stop()

// 		for {
// 			select {
// 			case <-ticker.C:
// 				mc.updateMemoryLimit()
// 				// In a real implementation, you'd also check for memory pressure
// 				// and trigger eviction if needed
// 			case <-mc.stopMonitor:
// 				return
// 			}
// 		}
// 	}()
// }
