package cache

import (
	"backup/src/util"
	"io"
)

type BaseFile interface {
	// Read reads data from the base file.
	// Returns the total number of bytes read, which may be less than len(data) only if EOF is reached.
	Read(off int64, data bytes) (bytesRead int, err error)
	Length() int64
}

type EmptyBaseFile struct{}

var _ BaseFile = (*EmptyBaseFile)(nil)

func (b *EmptyBaseFile) Read(off int64, data bytes) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	util.AssertionFailed("unexpected read from empty base file")
	return 0, io.EOF
}

func (b *EmptyBaseFile) Length() int64 {
	return 0
}

// area represents a located contiguous range of bytes in a file.
type area struct {
	off int64
	len int64
}

func (area *area) end() int64 {
	return area.off + area.len
}

// areas is a collection of area objects.
// Invariants: area objects have len > 0, are sorted by offset, non-overlapping and fully merged.
// See validateAreasInvariants.
type areas []area

// bytes are a contiguous range of bytes in a file.
type bytes []byte

// copy creates a compact deep copy of the bytes slice.
func (data *bytes) copy() bytes {
	result := make(bytes, len(*data))
	copy(result, *data)
	return result
}

// dataArea is a located contiguous area of bytes in a file.
type dataArea struct {
	off  int64
	data bytes
}

// copy creates a compact deep copy of the data area.
func (area *dataArea) copy() dataArea {
	return dataArea{off: area.off, data: area.data.copy()}
}

func (area *dataArea) len() int64 {
	return int64(len(area.data))
}

func (area *dataArea) end() int64 {
	return area.off + area.len()
}

// dataAreas is a collection of dataArea objects.
// Invariants: dataArea objects have len > 0, are sorted by offset and non-overlapping.
// See validateDataAreasInvariants.
// Additionally:
//   - dataArea objects are mostly merged to reduce fragmentation (unless they become too large).
//   - dataArea objects are compact, i.e. are backed by arrays with no unused capacity.
type dataAreas []dataArea
