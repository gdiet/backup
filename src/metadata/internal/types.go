package internal

import (
	"errors"
)

// TreeEntry interface for directory and file entries
type TreeEntry interface {
	GetName() string
}

// DirEntry represents a directory entry
type DirEntry struct {
	Name string
}

func (d *DirEntry) GetName() string {
	return d.Name
}

func (d *DirEntry) ToBytes() []byte {
	// 1 byte type + name
	buf := make([]byte, 1+len(d.Name))
	// buf[0] is 0, dirEntry type = 0
	copy(buf[1:], []byte(d.Name))
	return buf
}

// NewDirEntry creates a new directory entry
func NewDirEntry(name string) *DirEntry {
	return &DirEntry{Name: name}
}

// area represents a located contiguous range of bytes.
// Other than in the system file API, uint64 is used here because it is easier to serialize.
type Area struct {
	Off uint64
	Len uint64
}

// FileEntry represents a file entry
type FileEntry struct {
	Time int64    // UnixMilli
	Dref [40]byte // len|hash of dataEntry
	Name string   // at least 1 byte
}

func (f *FileEntry) GetName() string {
	return f.Name
}

func (f *FileEntry) ToBytes() []byte {
	// 1 byte type + 8 bytes time + 40 bytes data reference + name
	buf := make([]byte, 49+len(f.Name))
	buf[0] = 1 // fileEntry type = 1
	I64w(buf[1:], f.Time)
	copy(buf[9:49], f.Dref[:])
	copy(buf[49:], []byte(f.Name))
	return buf
}

// DataEntry represents a data entry with reference count and storage areas
type DataEntry struct {
	Refs  uint64 // Reference count
	Areas []Area // Storage areas
}

func (d *DataEntry) ToBytes() []byte {
	// 8 bytes reference count + 16 bytes per area
	buf := make([]byte, 8+16*len(d.Areas))
	U64w(buf, d.Refs)
	pos := 8
	for _, area := range d.Areas {
		U64w(buf[pos:], area.Off)
		U64w(buf[pos+8:], area.Len)
		pos += 16
	}
	return buf
}

func DataEntryFromBytes(data []byte) (DataEntry, error) {
	// 8 bytes reference count + 16 bytes per area
	dataLen := len(data)
	if dataLen < 8 || dataLen%16 != 8 {
		return DataEntry{}, errors.New("dataEntry length invalid")
	}
	d := DataEntry{}
	d.Refs = B64u(data)
	d.Areas = make([]Area, (dataLen-8)/16)
	pos := 8
	for i := range d.Areas {
		off := B64u(data[pos : pos+8])
		len := B64u(data[pos+8 : pos+16])
		d.Areas[i] = Area{Off: off, Len: len}
		pos += 16
	}
	return d, nil
}

// TreeEntryFromBytes parses a tree entry from bytes
func TreeEntryFromBytes(data []byte) (TreeEntry, error) {
	if len(data) < 2 {
		return nil, errors.New("treeEntry too short")
	}
	// Determine entry type by first byte
	switch data[0] {
	case 0:
		// dirEntry: 1 byte type + name
		return &DirEntry{Name: string(data[1:])}, nil
	case 1:
		// fileEntry: 1 byte type + 8 bytes time + 40 bytes data reference + name
		if len(data) < 50 {
			return nil, errors.New("fileEntry too short")
		}
		dref := [40]byte{}
		copy(dref[:], data[9:49])
		f := &FileEntry{
			Time: B64i(data[1:9]),
			Dref: dref,
			Name: string(data[49:]),
		}
		return f, nil
	default:
		return nil, errors.New("invalid treeEntry type")
	}
}
