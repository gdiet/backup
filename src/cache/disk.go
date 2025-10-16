package cache

import (
	"io"
	"os"
)

// Disk is a file cache layer that stores parts of a cached file on disk.
type Disk struct {
	file     *os.File
	filePath string
}

// Read reads data from the cache file.
// Invariant: The file must already be open (i.e., Write was called before).
// This method should only be called after previous Write operations.
func (disk *Disk) Read(off int64, data Bytes) error {
	bytesRead, err := disk.file.ReadAt(data, off)

	// EOF is expected when reading beyond file end - not an error for us
	if err != nil && err != io.EOF {
		return err
	}

	// Fill remaining bytes with zeros if we read less than requested
	if bytesRead < len(data) {
		for i := bytesRead; i < len(data); i++ {
			data[i] = 0
		}
	}

	return nil
}

// Truncate changes the size cache file.
// Invariant: This method should only be called when the file is already open
// (i.e., after previous Write operations). For the cache architecture,
// we don't need to truncate files that were never written to.
func (disk *Disk) Truncate(newSize int64) error {
	if disk.file == nil {
		return nil // File not open - nothing to truncate, which is fine
	}

	return disk.file.Truncate(newSize)
}

// Write writes data to the cache file at the specified position.
// Opens the file automatically if it's not already open (for both reading and writing).
func (disk *Disk) Write(off int64, data Bytes) error {
	// Open file if not already open
	if disk.file == nil {
		if disk.filePath == "" {
			return io.ErrClosedPipe // No file path configured
		}

		file, err := os.OpenFile(disk.filePath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		disk.file = file
	}

	totalWritten := 0
	for totalWritten < len(data) {
		bytesWritten, err := disk.file.WriteAt(data[totalWritten:], off+int64(totalWritten))
		if err != nil {
			return err
		}

		if bytesWritten == 0 {
			// No progress made - avoid infinite loop
			return io.ErrShortWrite
		}

		totalWritten += bytesWritten
	}

	return nil
}

// Close closes the underlying file.
func (disk *Disk) Close() error {
	file := disk.file
	if file == nil {
		return nil
	}
	disk.file = nil

	// Try to sync first - preserve sync error if close succeeds
	syncErr := file.Sync()
	closeErr := file.Close()

	// Return the more critical error (close failure is usually more severe)
	if closeErr != nil {
		return closeErr
	}
	return syncErr
}
