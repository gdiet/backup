package cache

import (
	"fmt"
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

	// Close closes the cache and cleans up all resources
	Close() error
}

// FIXME review locking strategy - it's not yet good

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

// getOrCreateFile returns an open file handle for the given fileId
// getOrCreateFile opens or creates a file for the given fileId
func (fc *FileCache) getOrCreateFile(fileId int) (*os.File, error) {
	fc.globalLock.Lock()
	defer fc.globalLock.Unlock()

	// Check if file is already open
	if file, exists := fc.openFiles[fileId]; exists {
		return file, nil
	}

	// Open or create the file first
	filePath := fc.getFilePath(fileId)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create file %q: %v", filePath, err)
	}

	// Create file lock only after successful file opening
	fc.fileLocks[fileId] = &sync.RWMutex{}
	fc.openFiles[fileId] = file
	return file, nil
}

// getFileLock returns the mutex for a specific fileId
func (fc *FileCache) getFileLock(fileId int) *sync.RWMutex {
	fc.globalLock.RLock()
	defer fc.globalLock.RUnlock()

	if lock, exists := fc.fileLocks[fileId]; exists {
		return lock
	}
	return nil
}

// Truncate sets the file to the specified length
func (fc *FileCache) Truncate(fileId int, length int64) error {
	if length < 0 {
		return fmt.Errorf("length cannot be negative: %d", length)
	}

	// Get file-specific lock FIRST to prevent race with Dispose
	fileLock := fc.getFileLock(fileId)
	if fileLock == nil {
		// File doesn't exist yet, try to create it
		_, err := fc.getOrCreateFile(fileId)
		if err != nil {
			return err
		}
		// Get the lock that was just created
		fileLock = fc.getFileLock(fileId)
		if fileLock == nil {
			return fmt.Errorf("failed to create file lock for fileId %d", fileId)
		}
	}

	fileLock.Lock()
	defer fileLock.Unlock()

	// Now safely get the file handle (protected by file-specific lock)
	file, err := fc.getOrCreateFile(fileId)
	if err != nil {
		return err
	}

	// Truncate the file
	if err := file.Truncate(length); err != nil {
		return fmt.Errorf("failed to truncate file %d to length %d: %v", fileId, length, err)
	}

	// Ensure the change is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file %d after truncate: %v", fileId, err)
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

	// Get file-specific lock FIRST to prevent race with Dispose
	fileLock := fc.getFileLock(fileId)
	if fileLock == nil {
		// File doesn't exist yet, try to create it
		_, err := fc.getOrCreateFile(fileId)
		if err != nil {
			return nil, err
		}
		// Get the lock that was just created
		fileLock = fc.getFileLock(fileId)
		if fileLock == nil {
			return nil, fmt.Errorf("failed to create file lock for fileId %d", fileId)
		}
	}

	fileLock.RLock()
	defer fileLock.RUnlock()

	// Now safely get the file handle (protected by file-specific lock)
	file, err := fc.getOrCreateFile(fileId)
	if err != nil {
		return nil, err
	}

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

	// Get file-specific lock FIRST to prevent race with Dispose
	fileLock := fc.getFileLock(fileId)
	if fileLock == nil {
		// File doesn't exist yet, try to create it
		_, err := fc.getOrCreateFile(fileId)
		if err != nil {
			return err
		}
		// Get the lock that was just created
		fileLock = fc.getFileLock(fileId)
		if fileLock == nil {
			return fmt.Errorf("failed to create file lock for fileId %d", fileId)
		}
	}

	fileLock.Lock()
	defer fileLock.Unlock()

	// Now safely get the file handle (protected by file-specific lock)
	file, err := fc.getOrCreateFile(fileId)
	if err != nil {
		return err
	}

	// Write data at the specified position
	n, err := file.WriteAt(data, position)
	if err != nil {
		return fmt.Errorf("failed to write to file %d at position %d: %v", fileId, position, err)
	}

	if n != len(data) {
		return fmt.Errorf("incomplete write to file %d: wrote %d bytes, expected %d", fileId, n, len(data))
	}

	// Ensure the change is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file %d after write: %v", fileId, err)
	}

	return nil
}

// Length returns the current length of the cached file
func (fc *FileCache) Length(fileId int) (int64, error) {
	// Get file-specific lock FIRST to prevent race with Dispose
	fileLock := fc.getFileLock(fileId)
	if fileLock == nil {
		// File doesn't exist yet, try to create it
		_, err := fc.getOrCreateFile(fileId)
		if err != nil {
			return 0, err
		}
		// Get the lock that was just created
		fileLock = fc.getFileLock(fileId)
		if fileLock == nil {
			return 0, fmt.Errorf("failed to create file lock for fileId %d", fileId)
		}
	}

	fileLock.RLock()
	defer fileLock.RUnlock()

	// Now safely get the file handle (protected by file-specific lock)
	file, err := fc.getOrCreateFile(fileId)
	if err != nil {
		return 0, err
	}

	// Get file info to determine size
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info for %d: %v", fileId, err)
	}

	return fileInfo.Size(), nil
}

// Dispose removes the cached file and cleans up resources
func (fc *FileCache) Dispose(fileId int) error {
	// First get the file-specific lock to wait for all operations to complete
	fc.globalLock.RLock()
	fileLock, exists := fc.fileLocks[fileId]
	fc.globalLock.RUnlock()

	if exists {
		// Wait for all ongoing operations to complete
		fileLock.Lock()
		defer fileLock.Unlock()
	}

	// Now safely dispose of the file
	fc.globalLock.Lock()
	defer fc.globalLock.Unlock()

	// Close the file if it's open
	if file, exists := fc.openFiles[fileId]; exists {
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close file %d: %v", fileId, err)
		}
		delete(fc.openFiles, fileId)
	}

	// Remove the file lock
	delete(fc.fileLocks, fileId)

	// Remove the actual file
	filePath := fc.getFilePath(fileId)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove file %q: %v", filePath, err)
	}

	return nil
}

// Close closes all open files and cleans up resources
func (fc *FileCache) Close() error {
	fc.globalLock.Lock()
	defer fc.globalLock.Unlock()

	var errors []error

	// Close all open files
	for fileId, file := range fc.openFiles {
		if err := file.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close file %d: %v", fileId, err))
		}
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
		"openFiles":     len(fc.openFiles),
		"trackedFiles":  len(fc.fileLocks),
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
