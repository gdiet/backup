package storage

import (
	"backup/src/util"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DataStore is a byte store supporting random reads and writes at arbitrary offsets.
type DataStore interface {
	// Write writes data to the datastore at the specified offset.
	Write(offset int64, data []byte) error
	// Read reads data from the datastore, padding with zeros if data is missing.
	// Returns the data and a warnings slice.
	Read(offset int64, length int64) ([]byte, []string)
	// Close closes all open files and releases resources.
	Close()
}

// FileBackedDataStore returns a file backed sequential byte store limited mostly by disc capacity.
// The data store is thread safe. Backing data files are filled up to fileSize bytes. Negative offsets are
// not supported.
//
// The recommended production fileSize is 100.000.000 bytes. That way, data files can be copied fast
// (not too many files which slows down copy) while having a manageable in size for all kinds of file systems.
// The recommended production storage size limit is fileSize * 10.000.000 bytes, i.e. 1.000 TB for fileSize = 100 MB.
//
// openFilesSoftLimit is an important performance factor for parallel access. Set it at least to the expected
// number of concurrent accesses and to not less than 5.
//
// A locking strategy is implemented to optimize for concurrent access. It has been closely reviewed:
//
// func (ds *dataStore) Read(offset int64, length int64) ([]byte, []string)
// => lease(fileID int, forWriting bool)
// => handle.lock.Lock() ... handle.lock.Unlock()
// => release(fileID int)
//
// func (ds *dataStore) Write(offset int64, data []byte) error
// => lease(fileID int, forWriting bool)
// => handle.lock.Lock() ... handle.lock.Unlock()
// => release(fileID int)
//
// func (ds *dataStore) lease(fileID int, forWriting bool) (*fileHandle, error)
// => ds.lock.Lock() ... ds.lock.Unlock() --- or ---
// => ds.lock.Lock() ... handle.lock.Lock() ... handle.lock.Unlock() ... ds.lock.Unlock()
//
// func (ds *dataStore) gc()
// => ds.lock.Lock() ... ds.lock.Unlock()
//
// func (ds *dataStore) release(fileID int)
// => ds.lock.Lock() ... ds.lock.Unlock()
//
// func (ds *dataStore) Close()
// => ds.lock.Lock() ... ds.lock.Unlock()
func FileBackedDataStore(baseDir string, fileSize int64, openFilesSoftLimit int) (DataStore, error) {
	if fileSize <= 0 {
		return nil, fmt.Errorf("fileSize must be greater than 0, got: %d", fileSize)
	}
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory %q: %v", baseDir, err)
	}

	store := &dataStore{
		baseDir:            baseDir,
		fileSize:           fileSize,
		openFilesSoftLimit: openFilesSoftLimit,
		lock:               sync.Mutex{},
		free:               make(map[int64]*fileHandle),
		leased:             make(map[int64]*fileHandle),
		evicting:           make(map[int64]*fileHandle),
		openFiles:          0,
		gcQueued:           false,
	}

	return store, nil
}

type dataStore struct {
	baseDir            string
	fileSize           int64
	openFilesSoftLimit int
	lock               sync.Mutex
	free               map[int64]*fileHandle
	leased             map[int64]*fileHandle
	evicting           map[int64]*fileHandle
	openFiles          int
	gcQueued           bool
}

type fileHandle struct {
	lock sync.RWMutex // allows concurrent reads
	// Synchronized by fileHandle.lock.
	file *os.File
	// Synchronized by dataStore.lock.
	queued int
}

// leaseFile returns a file handle (os.File) with appropriate lock already acquired.
// The returned release function MUST be called to release both lock and handle.
// forWriting=true acquires exclusive lock, forWriting=false acquires shared lock.
func (ds *dataStore) leaseFile(fileID int64, forWriting bool) (*os.File, func(), error) {
	handle, err := ds.leaseFileHandle(fileID, forWriting)
	if err != nil {
		return nil, nil, err
	}

	// Acquire the appropriate lock and return release function
	if forWriting {
		handle.lock.Lock()
		return handle.file, func() { handle.lock.Unlock(); ds.releaseFileHandle(fileID) }, nil
	} else {
		handle.lock.RLock()
		return handle.file, func() { handle.lock.RUnlock(); ds.releaseFileHandle(fileID) }, nil
	}
}

// leaseFileHandle obtains a file handle for the specified fileID with reference counting.
// The handle must be released using releaseFileHandle() when no longer needed.
//
// Parameters:
//   - fileID: The ID of the file to lease (determines file path and name)
//   - forWriting: If true, creates directories and file if they don't exist
func (ds *dataStore) leaseFileHandle(fileID int64, forWriting bool) (*fileHandle, error) {
	ds.lock.Lock()
	// Can't defer unlock because in one case we need to unlock earlier.

	// Start async garbage collection if needed.
	if !ds.gcQueued && ds.openFiles > ds.openFilesSoftLimit {
		ds.gcQueued = true
		go ds.gc()
	}

	// If the handle is free, lease it.
	if handle, exists := ds.free[fileID]; exists {
		handle.queued++
		delete(ds.free, fileID)
		ds.leased[fileID] = handle
		ds.lock.Unlock()
		return handle, nil
	}

	// If the handle is already leased, increase the queue count and lease it.
	if handle, exists := ds.leased[fileID]; exists {
		handle.queued++
		ds.lock.Unlock()
		return handle, nil
	}

	// If the handle is being evicted, wait for the eviction to finish, then retry.
	if handle, exists := ds.evicting[fileID]; exists {
		ds.lock.Unlock()
		handle.lock.Lock()
		{ // When the lock is acquired, the eviction is finished.
		}
		handle.lock.Unlock()
		return ds.leaseFileHandle(fileID, forWriting)
	}

	// Create the handle and lease it.
	fileName := fmt.Sprintf("%010d", fileID*ds.fileSize)
	dirNames := fmt.Sprintf("%06d", fileID)
	dirPath := filepath.Join(ds.baseDir, dirNames[:len(dirNames)-4], dirNames[len(dirNames)-4:len(dirNames)-2])
	filePath := filepath.Join(dirPath, fileName)
	// Open for read/write even if only for reading, because the file handle might be reused for writing later.
	fileOpenFlags := os.O_RDWR
	if forWriting {
		os.MkdirAll(dirPath, 0755)
		fileOpenFlags = os.O_RDWR | os.O_CREATE
	}
	file, err := os.OpenFile(filePath, fileOpenFlags, 0644)
	if err != nil {
		ds.lock.Unlock()
		return nil, err
	}
	handle := &fileHandle{
		file:   file,
		lock:   sync.RWMutex{},
		queued: 1,
	}

	ds.leased[fileID] = handle
	ds.openFiles++
	ds.lock.Unlock()
	return handle, nil
}

func (ds *dataStore) releaseFileHandle(fileID int64) {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	handle, exists := ds.leased[fileID]
	if !exists {
		util.AssertionFailed(fmt.Sprintf("release called for non-leased fileID %d", fileID))
		return // Graceful fallback in production when assertion only logs
	}

	handle.queued--
	if handle.queued == 0 {
		delete(ds.leased, fileID)
		ds.free[fileID] = handle
	}
}

func (ds *dataStore) gc() {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	ds.gcQueued = false

	filesToClose := ds.openFiles - ds.openFilesSoftLimit
	for id := range ds.free {
		if filesToClose <= 0 {
			break
		}
		filesToClose--
		handle := ds.free[id]
		delete(ds.free, id)
		ds.evicting[id] = handle

		handle.lock.Lock()
		go func() {
			handle.file.Sync()
			handle.file.Close()
			handle.lock.Unlock()

			ds.lock.Lock()
			delete(ds.evicting, id)
			ds.openFiles--
			ds.lock.Unlock()
		}()
	}
}

func (ds *dataStore) Write(offset int64, data []byte) error {

	for len(data) > 0 {
		fileID := offset / ds.fileSize
		offsetInFile := offset % ds.fileSize
		bytesToWrite := min(int64(len(data)), ds.fileSize-offsetInFile)

		file, release, err := ds.leaseFile(fileID, true)
		if err != nil {
			return fmt.Errorf("ERROR: Unable to lease data file %d: %v", fileID, err)
		}

		_, err = file.WriteAt(data[:bytesToWrite], offsetInFile)
		release()
		if err != nil {
			return fmt.Errorf("ERROR: Write error for data file %d: %v", fileID, err)
		}

		data = data[bytesToWrite:]
		offset += bytesToWrite
	}

	return nil
}

func (ds *dataStore) Read(offset int64, length int64) ([]byte, []string) {
	result := make([]byte, length)
	position := int64(0)
	warnings := []string{}

	for length > 0 {
		fileID := offset / ds.fileSize
		offsetInFile := offset % ds.fileSize
		bytesToRead := min(length, ds.fileSize-offsetInFile)

		file, release, err := ds.leaseFile(fileID, false)
		if err != nil {
			// If the file does not exist, we just return zeros.
			// Other errors are logged and we return zeros.
			warnings = append(warnings, fmt.Sprintf("WARNING: Unable to lease data file %d: %v. Padding with zeros.", fileID, err))
			position += bytesToRead
			offset += bytesToRead
			length -= bytesToRead
			continue
		}

		_, err = file.ReadAt(result[position:position+bytesToRead], offsetInFile)
		release()

		if err == io.EOF {
			warnings = append(warnings, fmt.Sprintf("WARNING: Reached EOF for data file %d: %v. Padding with zeros.", fileID, err))
		} else if err != nil {
			warnings = append(warnings, fmt.Sprintf("WARNING: Read error for data file %d: %v. Padding with zeros.", fileID, err))
		}

		position += bytesToRead
		offset += bytesToRead
		length -= bytesToRead
	}

	return result, warnings
}

func (ds *dataStore) Close() {
	for {
		ds.lock.Lock()
		if len(ds.leased) == 0 && len(ds.evicting) == 0 {
			for _, handle := range ds.free {
				handle.file.Sync()
				handle.file.Close()
			}
			ds.free = make(map[int64]*fileHandle)
			ds.openFiles = 0
			ds.lock.Unlock()
			break
		}
		ds.lock.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
