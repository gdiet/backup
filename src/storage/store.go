package storage

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
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
// Storage size is limited to fileSize * 10.000.000 bytes, i.e. 1.000 TB for fileSize = 100 MB.
//
// openFilesSoftLimit is an important performance factor for parallel access. Set it at least to the expected
// number of concurrent accesses and to not less than 5.
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
		free:               make(map[int]*fileHandle),
		leased:             make(map[int]*fileHandle),
		evicting:           make(map[int]*fileHandle),
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
	free               map[int]*fileHandle
	leased             map[int]*fileHandle
	evicting           map[int]*fileHandle
	openFiles          int
	gcQueued           bool
}

type fileHandle struct {
	lock sync.Mutex
	// Syncronized by fileHandle.lock.
	file *os.File
	// Syncronized by dataStore.lock.
	queued int
}

func (ds *dataStore) lease(fileID int, forWriting bool) (*fileHandle, error) {
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
		return ds.lease(fileID, forWriting)
	}

	// Create the handle and lease it.
	// FIXME try out
	fileName14 := fmt.Sprintf("%014d", fileID)
	dirPath := filepath.Join(ds.baseDir, fileName14[12:14], fileName14[10:12])
	filePath := filepath.Join(dirPath, strconv.Itoa(fileID))
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
		lock:   sync.Mutex{},
		queued: 1,
	}

	ds.leased[fileID] = handle
	ds.openFiles++
	ds.lock.Unlock()
	return handle, nil
}

func (ds *dataStore) release(fileID int) {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	handle, exists := ds.leased[fileID]
	if !exists {
		// This is kind of an assertion, it should never happen, but let's handle it gracefully.
		log.Printf("WARNING: release called for non-leased fileID %d", fileID)
		return
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

	for id := range ds.free {
		if ds.openFiles <= ds.openFilesSoftLimit {
			break
		}
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
		fileID := int(offset / ds.fileSize)
		offsetInFile := offset % ds.fileSize
		bytesToWrite := min(int64(len(data)), ds.fileSize-offsetInFile)

		handle, err := ds.lease(fileID, true)
		if err != nil {
			return fmt.Errorf("ERROR: Unable to lease data file %d: %v", fileID, err)
		}
		handle.lock.Lock()
		_, err = handle.file.WriteAt(data[:bytesToWrite], offsetInFile)
		handle.lock.Unlock()
		ds.release(fileID)
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
		fileID := int(offset / ds.fileSize)
		offsetInFile := offset % ds.fileSize
		bytesToRead := min(length, ds.fileSize-offsetInFile)

		handle, err := ds.lease(fileID, false)
		if err != nil {
			// If the file does not exist, we just return zeros.
			// Other errors are logged and we return zeros.
			warnings = append(warnings, fmt.Sprintf("WARNING: Unable to lease data file %d: %v. Padding with zeros.", fileID, err))
			position += bytesToRead
			offset += bytesToRead
			length -= bytesToRead
			continue
		}

		handle.lock.Lock()
		_, err = handle.file.ReadAt(result[position:position+bytesToRead], offsetInFile)
		handle.lock.Unlock()
		ds.release(fileID)

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
			ds.free = make(map[int]*fileHandle)
			ds.openFiles = 0
			ds.lock.Unlock()
			break
		}
		ds.lock.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
