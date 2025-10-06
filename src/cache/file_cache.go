package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Cache defines the interface for file caching operations
type Cache interface {
	// Truncate sets the file to the specified length, extending with zeros if necessary
	Truncate(fileId string, length int64) error

	// Read reads data from the cached file at the specified position
	Read(fileId string, position int64, length int64) ([]byte, error)

	// Write writes data to the cached file at the specified position
	Write(fileId string, position int64, data []byte) error

	// Length returns the current length of the cached file
	Length(fileId string) (int64, error)

	// Dispose removes the cached file and cleans up resources
	Dispose(fileId string) error

	// Close closes the cache and cleans up all resources
	Close() error
}

// FIXME review locking strategy

// FileCache implements a file-based cache that stores cached data as files in a directory
type FileCache struct {
	baseDir    string
	openFiles  map[string]*os.File
	fileLocks  map[string]*sync.RWMutex
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
		openFiles: make(map[string]*os.File),
		fileLocks: make(map[string]*sync.RWMutex),
	}, nil
}

// getFilePath returns the full file path for a given fileId
func (fc *FileCache) getFilePath(fileId string) string {
	return filepath.Join(fc.baseDir, fileId)
}

// getOrCreateFile returns an open file handle for the given fileId
func (fc *FileCache) getOrCreateFile(fileId string) (*os.File, error) {
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
func (fc *FileCache) getFileLock(fileId string) *sync.RWMutex {
	fc.globalLock.RLock()
	defer fc.globalLock.RUnlock()

	if lock, exists := fc.fileLocks[fileId]; exists {
		return lock
	}
	return nil
}

// Truncate sets the file to the specified length
func (fc *FileCache) Truncate(fileId string, length int64) error {
	if length < 0 {
		return fmt.Errorf("length cannot be negative: %d", length)
	}

	file, err := fc.getOrCreateFile(fileId)
	if err != nil {
		return err
	}

	// Get file-specific lock
	fileLock := fc.getFileLock(fileId)
	if fileLock == nil {
		return fmt.Errorf("file lock not found for fileId %q", fileId)
	}

	fileLock.Lock()
	defer fileLock.Unlock()

	// Truncate the file
	if err := file.Truncate(length); err != nil {
		return fmt.Errorf("failed to truncate file %q to length %d: %v", fileId, length, err)
	}

	// Ensure the change is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file %q after truncate: %v", fileId, err)
	}

	return nil
}

// Read reads data from the cached file at the specified position
func (fc *FileCache) Read(fileId string, position int64, length int64) ([]byte, error) {
	if position < 0 {
		return nil, fmt.Errorf("position cannot be negative: %d", position)
	}
	if length < 0 {
		return nil, fmt.Errorf("length cannot be negative: %d", length)
	}
	if length == 0 {
		return []byte{}, nil
	}

	file, err := fc.getOrCreateFile(fileId)
	if err != nil {
		return nil, err
	}

	// Get file-specific lock for reading
	fileLock := fc.getFileLock(fileId)
	if fileLock == nil {
		return nil, fmt.Errorf("file lock not found for fileId %q", fileId)
	}

	fileLock.RLock()
	defer fileLock.RUnlock()

	// Read data from the specified position
	data := make([]byte, length)
	n, err := file.ReadAt(data, position)

	if err != nil && err.Error() != "EOF" {
		return nil, fmt.Errorf("failed to read from file %q at position %d: %v", fileId, position, err)
	}

	// Return only the bytes that were actually read
	return data[:n], nil
}

// Write writes data to the cached file at the specified position
func (fc *FileCache) Write(fileId string, position int64, data []byte) error {
	if position < 0 {
		return fmt.Errorf("position cannot be negative: %d", position)
	}
	if len(data) == 0 {
		return nil // Nothing to write
	}

	file, err := fc.getOrCreateFile(fileId)
	if err != nil {
		return err
	}

	// Get file-specific lock for writing
	fileLock := fc.getFileLock(fileId)
	if fileLock == nil {
		return fmt.Errorf("file lock not found for fileId %q", fileId)
	}

	fileLock.Lock()
	defer fileLock.Unlock()

	// Write data at the specified position
	n, err := file.WriteAt(data, position)
	if err != nil {
		return fmt.Errorf("failed to write to file %q at position %d: %v", fileId, position, err)
	}

	if n != len(data) {
		return fmt.Errorf("incomplete write to file %q: wrote %d bytes, expected %d", fileId, n, len(data))
	}

	// Ensure the change is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file %q after write: %v", fileId, err)
	}

	return nil
}

// Length returns the current length of the cached file
func (fc *FileCache) Length(fileId string) (int64, error) {
	file, err := fc.getOrCreateFile(fileId)
	if err != nil {
		return 0, err
	}

	// Get file-specific lock for reading
	fileLock := fc.getFileLock(fileId)
	if fileLock == nil {
		return 0, fmt.Errorf("file lock not found for fileId %q", fileId)
	}

	fileLock.RLock()
	defer fileLock.RUnlock()

	// Get file info to determine size
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info for %q: %v", fileId, err)
	}

	return fileInfo.Size(), nil
}

// Dispose removes the cached file and cleans up resources
func (fc *FileCache) Dispose(fileId string) error {
	fc.globalLock.Lock()
	defer fc.globalLock.Unlock()

	// Close the file if it's open
	if file, exists := fc.openFiles[fileId]; exists {
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close file %q: %v", fileId, err)
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
			errors = append(errors, fmt.Errorf("failed to close file %q: %v", fileId, err))
		}
	}

	// Clear all maps
	fc.openFiles = make(map[string]*os.File)
	fc.fileLocks = make(map[string]*sync.RWMutex)

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
func (fc *FileCache) getOpenFilesList() []string {
	var fileIds []string
	for fileId := range fc.openFiles {
		fileIds = append(fileIds, fileId)
	}
	return fileIds
}
