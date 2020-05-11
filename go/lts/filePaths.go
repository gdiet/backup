package lts

import "fmt"

// PathOffsetSize returns relative path, offset in file and size in file
// for a data segment specified by position and size.
func PathOffsetSize(position, size uint64) (fpath string, positionInFile uint64, sizeInFile uint64) {
	positionInFile = position % fileSize
	sizeInFile = min(fileSize-positionInFile, size)
	outer := position / fileSize / 100 / 100
	inner := position / fileSize / 100 % 100
	fname := position - positionInFile
	fpath = fmt.Sprintf("%02d/%02d/%010d", outer, inner, fname)
	return
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
