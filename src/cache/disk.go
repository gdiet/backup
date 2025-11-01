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

// read reads data from the cache file into the provided buffer.
// The file must already be open (i.e., write was called before).
//
// Returns the areas that were not read.
func (d *disk) read(off int64, data bytes) (unreadAreas areas, err error) {
	end := off + int64(len(data))
	if off == end {
		return nil, nil // Nothing to read
	}

	lastUnread := area{off: off, len: int64(len(data))}

	// For each disk area, try to satisfy part of the read request
	for _, a := range d.areas {
		if a.off >= end {
			break // No further areas can satisfy the read
		}
		if a.end() <= off {
			continue // This area is before the requested read
		}

		// Determine overlapping range
		readStart := max(off, a.off)
		readEnd := min(end, a.end())

		// Read data from disk to output buffer
		bytesRead, err := d.file.ReadAt(data[readStart-off:readEnd-off], readStart)

		// EOF is expected when reading beyond file end - not an error for us
		if err != nil && err != io.EOF {
			return unreadAreas, err
		}

		// Fill remaining bytes with zeros if we read less than requested, but...
		readLength := readEnd - readStart
		if int64(bytesRead) < readLength {
			for i := int64(bytesRead); i < readLength; i++ {
				data[readStart-off+i] = 0
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

// shrink cuts off cached data beyond the new size, truncating the cache file.
// The file must already be open (i.e., write was called before).
// If called when the size has increased (don't do that), the cache file is extended.
func (d *disk) shrink(newSize int64) (err error) {
	err = d.file.Truncate(int64(newSize))
	if err != nil {
		return err
	}

	defer func() {
		validateAreasInvariants(d.areas)
	}()

	// Remove or truncate areas beyond new size
	for i, a := range d.areas {
		if a.off >= newSize {
			// Area starts beyond new size, skip it and remaining areas
			d.areas = d.areas[:i]
			break
		}
		if a.end() > newSize {
			// Area extends beyond new size, truncate it and skip remaining areas
			d.areas[i].len = newSize - a.off
			d.areas = d.areas[:i+1]
			break
		}
		// Otherwise, area is fully within new size, keep it
	}
	return nil
}

// remove removes disk areas overlapping with the specified area.
func (d *disk) remove(off int64, length int64) {
	if length == 0 {
		return // Nothing to do
	}
	defer func() {
		validateAreasInvariants(d.areas)
	}()

	end := off + length
	newAreas := areas{}
	for index, currentArea := range d.areas {
		if currentArea.end() <= off {
			// Area is completely before the removed area, keep it
			newAreas = append(newAreas, currentArea)
		} else if currentArea.off >= end {
			// Area is completely after the removed area, keep it and remaining areas
			newAreas = append(newAreas, d.areas[index:]...)
			break
		} else {
			// Area overlaps with the removed area
			if currentArea.off < off {
				// There is a part left of the removed area
				newAreas = append(newAreas, area{off: currentArea.off, len: off - currentArea.off})
			}
			if currentArea.end() > end {
				// There is a part right of the removed area
				newAreas = append(newAreas, area{off: end, len: currentArea.end() - end})
			}
		}
	}
	d.areas = newAreas
}

// write writes data to the cache file at the specified offset.
// Opens the file automatically for reading and writing if it's not already open.
func (d *disk) write(off int64, data bytes) (err error) { // TODO align signature with os.File?
	dataLen := len(data)
	if dataLen == 0 {
		return nil // Nothing to write, no memory change
	}

	// Open file if not already open
	if d.file == nil {
		file, err := os.OpenFile(d.filePath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		d.file = file
	}

	// Write data to file
	// os.File.WriteAt returns a non-nil error when n != len(b).
	// On network file systems and FUSE file systems the system call might write less
	// than requested without error. The go method loops the underlying system write call
	// until all bytes are written.
	_, err = d.file.WriteAt(data, int64(off))
	if err != nil {
		return err
	}

	defer func() {
		validateAreasInvariants(d.areas)
	}()

	// Update areas
	d.areas = insert(d.areas, off, int64(dataLen))
	return nil
}

func insert(previous areas, insertAt int64, insertLen int64) (result areas) {
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

// close closes and deletes the cache file and clears the areas.
// The file must already be open (i.e., write was called before).
func (d *disk) close() (err error) {
	defer func() {
		validateAreasInvariants(d.areas)
	}()

	err = d.file.Close()
	assert(err == nil, fmt.Sprintf("failed to close disk cache file %q: %v", d.filePath, err))
	err = os.Remove(d.filePath)
	d.file = nil
	d.areas = nil
	return err
}
