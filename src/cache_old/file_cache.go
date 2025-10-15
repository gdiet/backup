package cache_old

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// Cache defines the interface for file caching operations
type Cache interface {
	// Truncate sets the file to the specified length, extending with zeros if necessary
	Truncate(fileId int, length int64) error

	// Read reads data from the cached file at the specified position
	Read(fileId int, position int64, length int) ([]byte, error)

	// Write writes data to the cached file at the specified position
	Write(fileId int, position int64, data []byte) error

	// Length returns the current length of the cached file
	Length(fileId int) (int64, error)

	// Dispose removes the cached file and cleans up resources
	Dispose(fileId int) error

	// Flush ensures all pending writes for the specified file are written to disk
	Flush(fileId int) error

	// Close closes the cache and cleans up all resources
	Close() error

	// GetStats returns statistics about the cache
	GetStats() map[string]interface{}
}

// Locking Strategy: Thread-safe with optimized two-level locking:
// 1. globalLock (RWMutex) protects openFiles/fileLocks maps with double-checked locking
// 2. per-file locks (RWMutex) allow concurrent reads, exclusive writes
// Double-checked locking minimizes time holding globalLock during slow file I/O operations

// FileCache implements a file-based cache that stores cached data as files in a directory.
//
// Sparse File Support:
// This implementation automatically supports sparse files on filesystems that support them:
//   - Linux: ext4, XFS, Btrfs (full support), ext3 (limited)
//   - Windows: NTFS (Vista+, holes >64KB), FAT32 (no support)
//   - macOS: APFS, HFS+ (full support)
//
// When using WriteAt() with gaps or Truncate() to extend files, the filesystem will create
// sparse holes automatically, saving significant disk space. ReadAt() operations in sparse
// regions return zeros as expected.
type FileCache struct {
	baseDir    string
	openFiles  map[int]*os.File
	fileLocks  map[int]*sync.RWMutex
	globalLock sync.RWMutex
}

// NewFileCache creates a new file-based cache in the specified directory
func NewFileCache(baseDir string) (*FileCache, error) {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory %q: %v", baseDir, err)
	}

	return &FileCache{
		baseDir:   baseDir,
		openFiles: make(map[int]*os.File),
		fileLocks: make(map[int]*sync.RWMutex),
	}, nil
}

// getFilePath returns the full file path for a given fileId
func (fc *FileCache) getFilePath(fileId int) string {
	return filepath.Join(fc.baseDir, strconv.Itoa(fileId))
}

// getOrCreateFile returns an open file handle and its lock for the given fileId.
// Uses double-checked locking to minimize time holding the globalLock during file I/O operations.
func (fc *FileCache) getOrCreateFile(fileId int) (*os.File, *sync.RWMutex, error) {
	// First check: read lock only (fast path for existing files)
	fc.globalLock.RLock()
	if file, exists := fc.openFiles[fileId]; exists {
		fileLock := fc.fileLocks[fileId] // Invariant: lock always exists when file is open
		fc.globalLock.RUnlock()
		return file, fileLock, nil
	}
	fc.globalLock.RUnlock()

	// File doesn't exist, we need to create it
	// Open the file WITHOUT holding the global lock (possibly slow I/O operation)
	filePath := fc.getFilePath(fileId)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		// Normally, no error should occur if the file has already been opened by another thread
		return nil, nil, fmt.Errorf("failed to open/create file %q: %v", filePath, err)
	}

	// Second check: acquire write lock and check again (another goroutine might have created it)
	fc.globalLock.Lock()
	defer fc.globalLock.Unlock()

	// Double-check: another goroutine might have opened the same file while we were doing I/O
	if existingFile, exists := fc.openFiles[fileId]; exists {
		// Close our file since another goroutine already opened it
		file.Close()
		fileLock := fc.fileLocks[fileId] // Invariant: lock always exists when file is open
		return existingFile, fileLock, nil
	}

	// We're the first to open this file, register it
	fileLock := &sync.RWMutex{}
	fc.fileLocks[fileId] = fileLock
	fc.openFiles[fileId] = file
	return file, fileLock, nil
}

// getLockedFile returns a file handle with appropriate lock already acquired.
// The returned unlock function MUST be called (preferably with defer) to release the lock.
// forWriting=true acquires exclusive lock, forWriting=false acquires shared lock.
func (fc *FileCache) getLockedFile(fileId int, forWriting bool) (*os.File, func(), error) {
	// Get or create the file and its lock atomically
	file, fileLock, err := fc.getOrCreateFile(fileId)
	if err != nil {
		return nil, nil, err
	}

	// Acquire the appropriate lock and return unlock function
	if forWriting {
		fileLock.Lock()
		return file, func() { fileLock.Unlock() }, nil
	} else {
		fileLock.RLock()
		return file, func() { fileLock.RUnlock() }, nil
	}
}

// Truncate sets the file to the specified length
func (fc *FileCache) Truncate(fileId int, length int64) error {
	if length < 0 {
		return fmt.Errorf("length cannot be negative: %d", length)
	}

	// Get locked file handle (exclusive lock for truncating)
	file, unlock, err := fc.getLockedFile(fileId, true)
	if err != nil {
		return err
	}
	defer unlock()

	// Truncate the file
	if err := file.Truncate(length); err != nil {
		return fmt.Errorf("failed to truncate file %d to length %d: %v", fileId, length, err)
	}

	return nil
}

// Read reads data from the cached file at the specified position
func (fc *FileCache) Read(fileId int, position int64, length int) ([]byte, error) {
	if position < 0 {
		return nil, fmt.Errorf("position cannot be negative: %d", position)
	}
	if length < 0 {
		return nil, fmt.Errorf("length cannot be negative: %d", length)
	}
	if length == 0 {
		return []byte{}, nil
	}

	// Get locked file handle (shared lock for reading)
	file, unlock, err := fc.getLockedFile(fileId, false)
	if err != nil {
		return nil, err
	}
	defer unlock()

	// Read data from the specified position
	data := make([]byte, length)
	n, err := file.ReadAt(data, position)

	if err != nil && err.Error() != "EOF" {
		return nil, fmt.Errorf("failed to read from file %d at position %d: %v", fileId, position, err)
	}

	// Return only the bytes that were actually read
	return data[:n], nil
}

// Write writes data to the cached file at the specified position
func (fc *FileCache) Write(fileId int, position int64, data []byte) error {
	if position < 0 {
		return fmt.Errorf("position cannot be negative: %d", position)
	}
	if len(data) == 0 {
		return nil // Nothing to write
	}

	// Get locked file handle (exclusive lock for writing)
	file, unlock, err := fc.getLockedFile(fileId, true)
	if err != nil {
		return err
	}
	defer unlock()

	// Write data at the specified position
	n, err := file.WriteAt(data, position)
	if err != nil {
		return fmt.Errorf("failed to write to file %d at position %d: %v", fileId, position, err)
	}

	if n != len(data) {
		return fmt.Errorf("incomplete write to file %d: wrote %d bytes, expected %d", fileId, n, len(data))
	}

	return nil
}

// Length returns the current length of the cached file
func (fc *FileCache) Length(fileId int) (int64, error) {
	// Get locked file handle (shared lock for reading file info)
	file, unlock, err := fc.getLockedFile(fileId, false)
	if err != nil {
		return 0, err
	}
	defer unlock()

	// Get file info to determine size
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info for %d: %v", fileId, err)
	}

	return fileInfo.Size(), nil
}

// Flush ensures all pending writes for the specified file are written to disk
func (fc *FileCache) Flush(fileId int) error {
	// Get locked file handle (shared lock sufficient for sync)
	file, unlock, err := fc.getLockedFile(fileId, false)
	if err != nil {
		return err
	}
	defer unlock()

	// Sync the file to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file %d: %v", fileId, err)
	}

	return nil
}

// Dispose removes the cached file and cleans up resources
func (fc *FileCache) Dispose(fileId int) error {
	// Acquire global lock first to atomically check and mark for disposal
	fc.globalLock.Lock()

	// Check if file exists and get its lock
	fileLock, exists := fc.fileLocks[fileId]
	if !exists {
		// File already disposed or never existed
		fc.globalLock.Unlock()
		return nil
	}

	// File exists - remove it from maps first to prevent other dispose attempts
	file := fc.openFiles[fileId]
	delete(fc.openFiles, fileId)
	delete(fc.fileLocks, fileId)

	// Now we can release global lock - we're the only one disposing this file
	fc.globalLock.Unlock()

	// Wait for all ongoing operations to complete
	fileLock.Lock()
	defer fileLock.Unlock()

	// Close the file (guaranteed to be non-nil due to our invariants)
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close file %d: %v", fileId, err)
	}

	// Remove the actual file
	filePath := fc.getFilePath(fileId)
	if err := os.Remove(filePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to remove file %q: %v", filePath, err)
	}

	return nil
}

// Close closes all open files and cleans up resources
func (fc *FileCache) Close() error {
	fc.globalLock.Lock()
	defer fc.globalLock.Unlock()

	var errors []error

	// Close all open files with proper locking
	for fileId, file := range fc.openFiles {
		fileLock := fc.fileLocks[fileId]

		// Acquire exclusive lock to ensure no ongoing operations
		fileLock.Lock()
		if err := file.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close file %d: %v", fileId, err))
		}
		fileLock.Unlock()
	}

	// Clear all maps
	fc.openFiles = make(map[int]*os.File)
	fc.fileLocks = make(map[int]*sync.RWMutex)

	// Return combined errors if any
	if len(errors) > 0 {
		errorMsg := "errors during close:"
		for _, err := range errors {
			errorMsg += "\n  " + err.Error()
		}
		return fmt.Errorf("%s", errorMsg)
	}

	return nil
}

// GetStats returns statistics about the cache
func (fc *FileCache) GetStats() map[string]interface{} {
	fc.globalLock.RLock()
	defer fc.globalLock.RUnlock()

	return map[string]interface{}{
		"baseDir":       fc.baseDir,
		"numberOfFiles": len(fc.openFiles),
		"openFilesList": fc.getOpenFilesList(),
	}
}

// getOpenFilesList returns a list of currently open file IDs
func (fc *FileCache) getOpenFilesList() []int {
	var fileIds []int
	for fileId := range fc.openFiles {
		fileIds = append(fileIds, fileId)
	}
	return fileIds
}
