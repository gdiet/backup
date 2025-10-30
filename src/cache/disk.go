package cache

import (
	"fmt"
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

// close closes and deletes the cache file and clears the areas.
// The file must already be open (i.e., write was called before).
func (disk *disk) close() (err error) {
	defer func() {
		validateAreasInvariants(disk.areas)
	}()

	err = disk.file.Close()
	assert(err == nil, fmt.Sprintf("failed to close disk cache file %q: %v", disk.filePath, err))
	err = os.Remove(disk.filePath)
	disk.file = nil
	disk.areas = nil
	return err
}

// read reads data from the cache file.
// The file must already be open (i.e., write was called before).
// Returns the areas that were not read.
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

		// Fill remaining bytes with zeros if we read less than requested, but...
		if bytesRead < readEnd-readStart {
			for i := bytesRead; i < readEnd-readStart; i++ {
				data[readStart-position+i] = 0
			}
			// ... but the system should not request areas that are not fully present on disk
			assert(false, "partial read from disk cache file")
		}

		// Adjust unread areas
		if readStart > lastUnread.off {
			// There is an unread area before the readStart
			unreadAreas = append(unreadAreas, area{off: lastUnread.off, len: readStart - lastUnread.off})
		}
		lastUnread.off = readEnd
		lastUnread.len = end - readEnd
	}

	if lastUnread.len > 0 {
		return append(unreadAreas, lastUnread), nil
	}
	return unreadAreas, nil
}

// truncate changes the size of the cache file, adjusting cached areas as needed.
// The file must already be open (i.e., write was called before).
func (disk *disk) truncate(newSize int) (err error) {
	err = disk.file.Truncate(int64(newSize))
	if err != nil {
		return err
	}

	defer func() {
		validateAreasInvariants(disk.areas)
	}()

	// Adjust areas
	for index, current := range disk.areas {
		maxLen := newSize - current.off
		if maxLen <= 0 {
			// This area is beyond new size, remove it and all following areas
			disk.areas = disk.areas[:index]
			break
		}
		if current.len > maxLen {
			// This area extends beyond new size, trim it and remove all following areas
			disk.areas[index].len = maxLen
			disk.areas = disk.areas[:index+1]
			break
		}
		// Otherwise, area is fully within new size, keep it
	}
	return nil
}

// write writes data to the cache file at the specified position.
// Opens the file automatically for reading and writing if it's not already open.
func (disk *disk) write(position int, data bytes) (err error) { // TODO align signature with os.File?
	dataLen := len(data)
	if dataLen == 0 {
		return nil // Nothing to write, no memory change
	}

	// Open file if not already open
	if disk.file == nil {
		file, err := os.OpenFile(disk.filePath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		disk.file = file
	}

	// Write data to file
	// os.File.WriteAt returns a non-nil error when n != len(b).
	// On network file systems and FUSE file systems the system call might write less
	// than requested without error. The go method loops the underlying system write call
	// until all bytes are written.
	_, err = disk.file.WriteAt(data, int64(position))
	if err != nil {
		return err
	}

	defer func() {
		validateAreasInvariants(disk.areas)
	}()

	// Update areas
	disk.areas = insert(disk.areas, position, dataLen)
	return nil
}

func insert(previous areas, insertAt int, insertLen int) (result areas) {
	insertEnd := insertAt + insertLen

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

// remove removes disk areas overlapping with the specified area.
func (disk *disk) remove(position int, len int) {
	if len == 0 {
		return // Nothing to do
	}
	defer func() {
		validateAreasInvariants(disk.areas)
	}()

	end := position + len
	newAreas := areas{}
	for index, currentArea := range disk.areas {
		if currentArea.end() <= position {
			// Area is completely before the removed area
			newAreas = append(newAreas, currentArea)
		} else if currentArea.off >= end {
			// Area is completely after the removed area
			newAreas = append(newAreas, disk.areas[index:]...)
			break
		} else {
			// Area overlaps with the removed area
			if currentArea.off < position {
				// There is a part left of the removed area
				newAreas = append(newAreas, area{off: currentArea.off, len: position - currentArea.off})
			}
			if currentArea.end() > end {
				// There is a part right of the removed area
				newAreas = append(newAreas, area{off: end, len: currentArea.end() - end})
			}
		}
	}
	disk.areas = newAreas
}
