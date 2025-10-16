package cache_old2

import (
	"io"
	"os"
)

// Disk is a file cache layer that stores parts of a cached file on disk.
type Disk struct {
	file         *os.File
	filePath     string
	areasWritten Areas
}

// Read reads data from the cache file.
// Invariant: The file must already be open (i.e., Write was called before).
// This method should only be called after previous Write operations.
// Returns the Areas that were not read.
func (disk *Disk) Read(position int64, data Bytes) (Areas, error) {
	if len(data) == 0 { // TODO check whether this condition is needed or helpful
		return nil, nil // Nothing to read
	}

	// Initialize unread areas with the full requested area
	unreadAreas := Areas{Area{Off: position, Len: data.Size()}}

	// For each written area, try to satisfy parts of the read request
	for _, writtenArea := range disk.areasWritten {
		readStart := max(position, writtenArea.Off)
		readEnd := min(position+data.Size(), writtenArea.Off+writtenArea.Len)
		if readStart >= readEnd {
			continue // No overlap
		}

		// Read from cache file
		bytesRead, err := disk.file.ReadAt(data[readStart-position:readEnd-position], readStart)
		// EOF is expected when reading beyond file end - not an error for us
		if err != nil && err != io.EOF {
			return unreadAreas, err
		}
		// Fill remaining bytes with zeros if we read less than requested
		if bytesRead < len(data) { // TODO it should be an invariant that we always read the full requested area here
			// TODO so maybe add a panic or log a warning here?
			for i := bytesRead; i < len(data); i++ {
				data[i] = 0
			}
		}

		// Adjust unread areas
		unreadAreas = unreadAreas.RemoveOverlappingAreas(Area{Off: readStart, Len: readEnd - readStart})
	}

	return unreadAreas, nil
}

// Truncate changes the size cache file.
// Invariant: This method should only be called when the file is already open
// (i.e., after previous Write operations). For the cache architecture,
// we don't need to truncate files that were never written to.
func (disk *Disk) Truncate(newSize int64) error {
	if disk.file == nil {
		return nil // File not open - nothing to truncate, which is fine
	}
	// Remove or truncate written areas beyond new size
	var filteredAreas Areas
	for _, writtenArea := range disk.areasWritten {
		if writtenArea.Off >= newSize {
			continue // Area starts beyond new size, remove it
		}
		if writtenArea.Off+writtenArea.Len > newSize {
			writtenArea.Len = newSize - writtenArea.Off // Area extends beyond new size, truncate it
		}
		filteredAreas = append(filteredAreas, writtenArea)
	}
	// In certain edge cases we would profit from truncating the file only if it shrinks,
	// but for simplicity, we always call Truncate here.
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
