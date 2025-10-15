package cache

import (
	"io"
	"os"
)

// Disk is a file cache layer that stores parts of a cached file on disk.
type Disk struct {
	file *os.File
}

func (d Disk) Read(off int64, data Bytes) error {
	bytesRead, err := d.file.ReadAt(data, off)

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
