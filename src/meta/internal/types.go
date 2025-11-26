package internal

import "backup/src/fserr"

// TreeEntry interface for directory and file entries
type TreeEntry interface {
	Name() string
	SetName(name string)
	ToBytes() []byte
}

// DirEntry represents a directory entry
type DirEntry struct {
	name string
}

func (d *DirEntry) Name() string {
	return d.name
}

func (d *DirEntry) SetName(name string) {
	d.name = name
}

func (d *DirEntry) ToBytes() []byte {
	// 1 byte type + name
	buf := make([]byte, 1+len(d.name))
	// buf[0] is 0, dirEntry type = 0
	copy(buf[1:], d.name)
	return buf
}

// NewDirEntry creates a new directory entry
func NewDirEntry(name string) *DirEntry {
	return &DirEntry{name: name}
}

// Area represents a located contiguous range of bytes.
// Other than in the system file API, uint64 is used here because it is easier to serialize.
type Area struct {
	Off uint64
	Len uint64
}

// FileEntry represents a file entry
type FileEntry struct {
	time int64    // UnixMilli
	dref [40]byte // len|hash of dataEntry
	name string   // at least 1 byte
}

func (f *FileEntry) Name() string {
	return f.name
}

func (f *FileEntry) SetName(name string) {
	f.name = name
}

func (f *FileEntry) Time() int64 {
	return f.time
}

func (f *FileEntry) Size() int64 {
	return B64i(f.dref[:8])
}

// NewFileEntry creates a new file entry
func NewFileEntry(name string, time int64, dref [40]byte) *FileEntry {
	return &FileEntry{
		name: name,
		time: time,
		dref: dref,
	}
}

func (f *FileEntry) ToBytes() []byte {
	// 1 byte type + 8 bytes time + 40 bytes data reference + name
	buf := make([]byte, 49+len(f.name))
	buf[0] = 1 // fileEntry type = 1
	I64w(buf[1:], f.time)
	copy(buf[9:49], f.dref[:])
	copy(buf[49:], f.name)
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

func dataEntryFromBytes(data []byte) (DataEntry, error) {
	// 8 bytes reference count + 16 bytes per area
	dataLen := len(data)
	if dataLen < 8 || dataLen%16 != 8 {
		return DataEntry{}, &fserr.DeserializationError{Msg: "dataEntry length invalid"}
	}
	d := DataEntry{}
	d.Refs = B64u(data)
	d.Areas = make([]Area, (dataLen-8)/16)
	pos := 8
	for i := range d.Areas {
		off := B64u(data[pos : pos+8])
		size := B64u(data[pos+8 : pos+16])
		d.Areas[i] = Area{Off: off, Len: size}
		pos += 16
	}
	return d, nil
}

// treeEntryFromBytes parses a tree entry from bytes
func treeEntryFromBytes(data []byte) (TreeEntry, error) {
	if len(data) < 2 {
		return nil, &fserr.DeserializationError{Msg: "treeEntry too short"}
	}
	// Determine entry type by first byte
	switch data[0] {
	case 0:
		// dirEntry: 1 byte type + name
		return &DirEntry{name: string(data[1:])}, nil
	case 1:
		// fileEntry: 1 byte type + 8 bytes time + 40 bytes data reference + name
		if len(data) < 50 {
			return nil, &fserr.DeserializationError{Msg: "fileEntry too short"}
		}
		dref := [40]byte{}
		copy(dref[:], data[9:49])
		return NewFileEntry(string(data[49:]), B64i(data[1:9]), dref), nil
	default:
		return nil, &fserr.DeserializationError{Msg: "invalid treeEntry type"}
	}
}
