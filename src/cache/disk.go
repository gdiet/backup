package cache

import (
	"io"
	"os"
)

// Disk is a file cache layer that stores parts of a cached file on disk.
type Disk struct {
	file *os.File
}

func (disk *Disk) Read(off int64, data Bytes) error {
	/*
		TODO Ich halte die nil-Abfrage für falsch: Wenn ich mir zum Beispiel nur TestTieredReadMemoryLayer
		ansehe, dann sehe ich dort, dass die Tiered Struct in einem Zustand ist, der nicht durch die
		geplanten Operationen Write und Truncate erreicht werden kann. Wir sollten nur Zustände testen,
		die durch die geplanten Operationen erreichbar sind.
	*/

	// If no file is available, fill with zeros (cache miss)
	if disk.file == nil {
		for i := range data {
			data[i] = 0
		}
		return nil
	}

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
func (disk *Disk) Truncate(newSize int64) error {
	if err := disk.file.Truncate(newSize); err != nil {
		return err
	}
	return nil
}

// Write writes data to the cache file at the specified position.
func (disk *Disk) Write(off int64, data Bytes) error {
	if disk.file == nil {
		panic("TODO: wir müssen die Datei erst öffnen")
	}

	_, err := disk.file.WriteAt(data, off)
	return err
}

// Close closes the underlying file.
func (disk *Disk) Close() error {
	if disk.file != nil {
		err := disk.file.Sync()
		if err != nil {
			err = disk.file.Close()
			if err != nil {
				disk.file = nil
			}
		}
		return err
	}
	return nil
}
