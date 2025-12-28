package meta

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

// treeEntryFromBytes parses a tree entry from bytes
func treeEntryFromBytes(data []byte) (TreeEntry, error) {
	if len(data) < 2 {
		return nil, fserr.IO
	}
	// Determine entry type by first byte
	switch data[0] {
	case 0:
		// dirEntry: 1 byte type + name
		return &DirEntry{name: string(data[1:])}, nil
	case 1:
		// fileEntry: 1 byte type + 8 bytes time + 40 bytes data reference + name
		if len(data) < 50 {
			return nil, fserr.IO
		}
		dref := [40]byte{}
		copy(dref[:], data[9:49])
		return NewFileEntry(string(data[49:]), B64i(data[1:9]), dref), nil
	default:
		return nil, fserr.IO
	}
}
