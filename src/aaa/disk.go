package aaa

import (
	"io"
	"os"
)

// Disk is a file cache layer that stores parts of a cached file on disk.
type disk struct {
	// file is the underlying file being cached, or nil if not open.
	file *os.File
	// filePath is the path to the underlying file.
	filePath string
	// areas holds the areas that have been written to the file.
	areas areas
}

// read reads data from the cache file. The file must already be open (i.e., write was called before).
// Returns the areas that were not read.
// TODO tests
func (disk *disk) read(position int, data bytes) (unreadAreas areas, err error) {
	end := position + len(data)
	if position == end {
		return nil, nil // Nothing to read
	}

	lastUnread := area{off: position, len: len(data)}

	// For each disk area, try to satisfy parts of the read request
	for _, diskArea := range disk.areas {
		if diskArea.off >= end {
			break // No further areas can satisfy the read
		}
		if diskArea.end() <= position {
			continue // This area is before the requested read
		}

		// Determine overlapping range
		readStart := max(position, diskArea.off)
		readEnd := min(end, diskArea.end())

		// Read data from disk to output buffer
		bytesRead, err := disk.file.ReadAt(data[readStart-position:readEnd-position], int64(readStart))

		// EOF is expected when reading beyond file end - not an error for us
		if err != nil && err != io.EOF {
			return unreadAreas, err
		}

		// Fill remaining bytes with zeros if we read less than requested
		if bytesRead < len(data) {
			// The system should not request areas that are not fully present on disk
			assert(false, "partial read from disk cache file")
			for i := bytesRead; i < len(data); i++ {
				data[i] = 0
			}
		}

		// Adjust unread areas
		if readStart > lastUnread.off {
			// There is an unread area before the readStart
			unreadAreas = append(unreadAreas, area{off: lastUnread.off, len: readStart - lastUnread.off})
		}
		lastUnread.off = readEnd
		lastUnread.len = end - readEnd
	}

	return unreadAreas, nil
}

// write writes data to the cache file at the specified position.
// Opens the file automatically for reading and writing if it's not already open.
func (disk *disk) write(position int, data bytes) (err error) { // TODO align signature with os.File?
	dataLen := len(data)
	if dataLen == 0 {
		return nil // Nothing to write, no memory change
	}

	// TODO validate invariants

	// Open file if not already open
	if disk.file == nil {
		file, err := os.OpenFile(disk.filePath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		disk.file = file
	}

	// Write data to file
	totalWritten := 0
	for totalWritten < dataLen {
		bytesWritten, err := disk.file.WriteAt(data[totalWritten:], int64(position+totalWritten))
		if err != nil {
			return err
		}
		if bytesWritten == 0 {
			return io.ErrShortWrite // No progress made - avoid infinite loop
		}
		// network file systems and FUSE file systems might return n < len(p) without error
		totalWritten += bytesWritten
	}

	disk.areas = insert(disk.areas, position, dataLen) // Update areas
	return nil
}

func insert(previous areas, insertAt int, insertLen int) areas {
	insertEnd := insertAt + insertLen
	var result areas

	for index, current := range previous {
		if current.end() < insertAt {
			// current before insert, keep current
			result = append(result, current)
			continue
		}
		if current.off > insertEnd {
			// current after insert, add insert and remaining areas and return
			result = append(result, area{off: insertAt, len: insertEnd - insertAt})
			result = append(result, previous[index:]...)
			return result
		}
		// merge
		insertAt = min(insertAt, current.off)
		insertEnd = max(insertEnd, current.end())
	}
	result = append(result, area{off: insertAt, len: insertEnd - insertAt})
	return result
}
